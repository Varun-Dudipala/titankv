package com.titankv.network;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Node-to-node commands are accepted only from members' addresses, which are looked up once and
 * remembered. The lookups used to be remembered for the life of the process, so a member that
 * came back with a new address (a recreated container) was refused by every peer until the peer
 * restarted.
 */
class MemberAddressesTest {

    private static final long INTERVAL = 10_000_000_000L;

    private final Map<String, String> dns = new ConcurrentHashMap<>();
    private final AtomicInteger lookups = new AtomicInteger();
    private final AtomicLong nanos = new AtomicLong(1_000_000_000_000L);
    private final MemberAddresses addresses = new MemberAddresses(host -> {
        lookups.incrementAndGet();
        return dns.getOrDefault(host, host);
    }, nanos::get, INTERVAL);

    @Test
    void aMemberIsRecognisedByItsResolvedAddress() {
        dns.put("kv-a", "10.0.0.1");
        dns.put("kv-b", "10.0.0.2");

        assertThat(addresses.isMember("10.0.0.2", List.of("kv-a", "kv-b"))).isTrue();
        assertThat(addresses.isMember("10.0.0.9", List.of("kv-a", "kv-b"))).isFalse();
        assertThat(addresses.isMember("10.0.0.1", List.of("10.0.0.1"))).isTrue();
    }

    @Test
    void lookupsAreRemembered() {
        dns.put("kv-a", "10.0.0.1");
        for (int i = 0; i < 100; i++) {
            assertThat(addresses.isMember("10.0.0.1", List.of("kv-a"))).isTrue();
        }
        assertThat(lookups.get()).isEqualTo(1);
    }

    @Test
    void aMemberThatChangedAddressIsAcceptedAfterTheNextRefresh() {
        dns.put("kv-a", "10.0.0.1");
        assertThat(addresses.isMember("10.0.0.1", List.of("kv-a"))).isTrue();

        dns.put("kv-a", "10.0.0.50"); // the container was recreated
        nanos.addAndGet(INTERVAL);
        assertThat(addresses.isMember("10.0.0.50", List.of("kv-a"))).isTrue();
        assertThat(addresses.isMember("10.0.0.1", List.of("kv-a"))).as("its old address").isFalse();
    }

    @Test
    void strangersCannotForceALookupPerConnection() {
        dns.put("kv-a", "10.0.0.1");
        assertThat(addresses.isMember("10.0.0.1", List.of("kv-a"))).isTrue();
        nanos.addAndGet(INTERVAL);
        assertThat(addresses.isMember("10.9.9.9", List.of("kv-a"))).isFalse(); // one refresh
        int afterRefresh = lookups.get();

        for (int i = 0; i < 1_000; i++) {
            nanos.addAndGet(1_000_000); // 1ms apart
            assertThat(addresses.isMember("10.9.9.9", List.of("kv-a"))).isFalse();
        }
        assertThat(lookups.get()).isEqualTo(afterRefresh);
    }

    @Test
    void loopbackNamesAndAddressesAreTheSameHost() {
        assertThat(addresses.isMember("127.0.0.1", List.of("localhost"))).isTrue();
        assertThat(addresses.isMember("0:0:0:0:0:0:0:1", List.of("127.0.0.1"))).isTrue();
        assertThat(addresses.isMember("10.0.0.1", List.of("localhost"))).isFalse();
    }
}
