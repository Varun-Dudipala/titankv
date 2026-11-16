package com.titankv.network;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Answers "does this connection come from a cluster member?" by resolving the members' host names
 * and comparing addresses. Lookups are cached, since the check runs for every node-to-node
 * command and must not wait on DNS.
 *
 * A cache that never forgets locks a member out when its address changes: a container recreated
 * with a new IP keeps its name, but every peer still holds the old address, and would refuse its
 * replica reads and writes until the peer itself restarted. So when a connection matches no
 * member, the cached addresses are looked up again, at most once per refresh interval.
 */
final class MemberAddresses {

    private final Function<String, String> lookup;
    private final LongSupplier nanoClock;
    private final long refreshIntervalNanos;
    private final Map<String, String> addressByHost = new ConcurrentHashMap<>();
    private volatile long lastRefreshNanos;

    MemberAddresses() {
        this(MemberAddresses::resolve, System::nanoTime, 10_000_000_000L);
    }

    /**
     * @param lookup               resolves a host name to an address, or returns the name itself
     * @param nanoClock            a monotonic clock, in nanoseconds
     * @param refreshIntervalNanos the least time between two rounds of repeated lookups
     */
    MemberAddresses(Function<String, String> lookup, LongSupplier nanoClock, long refreshIntervalNanos) {
        this.lookup = lookup;
        this.nanoClock = nanoClock;
        this.refreshIntervalNanos = refreshIntervalNanos;
        this.lastRefreshNanos = nanoClock.getAsLong() - refreshIntervalNanos;
    }

    /**
     * @return whether the address is that of one of the hosts, as resolved now or, if none
     *         matched and the last refresh is old enough, as resolved again
     */
    boolean isMember(String remoteAddress, Collection<String> memberHosts) {
        if (matches(remoteAddress, memberHosts)) {
            return true;
        }
        long now = nanoClock.getAsLong();
        if (now - lastRefreshNanos < refreshIntervalNanos) {
            return false;
        }
        lastRefreshNanos = now;
        addressByHost.clear();
        return matches(remoteAddress, memberHosts);
    }

    private boolean matches(String remoteAddress, Collection<String> memberHosts) {
        for (String host : memberHosts) {
            if (host == null || host.isEmpty()) {
                continue;
            }
            if (remoteAddress.equals(host) || remoteAddress.equals(addressByHost.computeIfAbsent(host, lookup))
                    || (isLoopback(remoteAddress) && isLoopback(host))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isLoopback(String address) {
        return "127.0.0.1".equals(address) || "::1".equals(address) || "0:0:0:0:0:0:0:1".equals(address)
                || "localhost".equalsIgnoreCase(address);
    }

    private static String resolve(String host) {
        try {
            return InetAddress.getByName(host).getHostAddress();
        } catch (Exception e) {
            return host;
        }
    }
}
