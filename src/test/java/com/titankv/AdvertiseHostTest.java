package com.titankv;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The address a node tells its peers to reach it at. It used to come from the node id alone, so a
 * node with a plain label for an id ("node-1") advertised localhost, and its peers on other
 * machines sent its replica traffic to themselves.
 */
class AdvertiseHostTest {

    @Test
    void theAdvertiseHostWinsOverTheNodeId() {
        assertThat(TitanKVServer.advertisedHost("node-1", "10.0.0.7")).isEqualTo("10.0.0.7");
        assertThat(TitanKVServer.advertisedHost("kv-a:9001", " kv-a.internal ")).isEqualTo("kv-a.internal");
    }

    @Test
    void withoutOneTheHostComesFromTheNodeId() {
        assertThat(TitanKVServer.advertisedHost("kv-a:9001", null)).isEqualTo("kv-a");
        assertThat(TitanKVServer.advertisedHost("node-1", null)).isEqualTo("localhost");
        assertThat(TitanKVServer.advertisedHost("node-1", "  ")).isEqualTo("localhost");
    }

    @Test
    void aNodeWithALabelForAnIdGossipsItsAdvertisedAddress() {
        TitanKVServer server = new TitanKVServer(19045, "node-1", null, "127.0.0.1");
        try {
            assertThat(server.getNodeId()).isEqualTo("node-1");
            assertThat(server.getClusterManager().getLocalNode().getHost()).isEqualTo("127.0.0.1");
            assertThat(server.getClusterManager().getLocalNode().getAddress()).isEqualTo("127.0.0.1:19045");
        } finally {
            server.stop();
        }
    }

    @Test
    void theNodeIdDefaultsToTheAdvertisedAddress() {
        TitanKVServer server = new TitanKVServer(19046, null, null, "127.0.0.1");
        try {
            assertThat(server.getNodeId()).isEqualTo("127.0.0.1:19046");
        } finally {
            server.stop();
        }
    }
}
