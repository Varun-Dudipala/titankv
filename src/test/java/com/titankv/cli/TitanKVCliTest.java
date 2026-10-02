package com.titankv.cli;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The CLI's commands against a single running node.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TitanKVCliTest {

    private static final int PORT = 19011;
    private static final String HOST = "localhost:" + PORT;

    private TitanKVServer server;
    private TitanKVClient client;

    @BeforeAll
    void start() throws Exception {
        server = new TitanKVServer(PORT);
        server.start();
        client = new TitanKVClient(HOST);
    }

    @AfterAll
    void stop() {
        client.close();
        server.stop();
    }

    private String run(String... args) {
        PrintStream original = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            boolean ok = TitanKVCli.run(client, HOST, args);
            return (ok ? "" : "FAILED ") + captured.toString(StandardCharsets.UTF_8).trim();
        } finally {
            System.setOut(original);
        }
    }

    @Test
    void dataCommands() {
        assertThat(run("put", "cli:user", "Ada")).isEqualTo("OK");
        assertThat(run("get", "cli:user")).isEqualTo("Ada");
        assertThat(run("exists", "cli:user")).isEqualTo("true");
        assertThat(run("del", "cli:user")).isEqualTo("OK");
        assertThat(run("get", "cli:user")).isEqualTo("(nil)");
        assertThat(run("ping")).isEqualTo("PONG");
    }

    @Test
    void consistencyCanBeShownAndSet() {
        assertThat(run("consistency")).startsWith("default");
        assertThat(run("consistency", "one")).isEmpty();
        assertThat(run("consistency")).isEqualTo("ONE");
        assertThat(run("put", "cli:one", "x")).isEqualTo("OK");
        assertThat(run("consistency", "sometimes")).startsWith("FAILED Unknown consistency level");
        assertThat(run("consistency", "default")).isEmpty();
    }

    @Test
    void mistakesAreReportedNotThrown() {
        assertThat(run("get")).isEqualTo("FAILED Error: usage: get <key>");
        assertThat(run("put", "k", "v", "soon")).startsWith("FAILED Error:");
        assertThat(run("frobnicate")).startsWith("FAILED Unknown command");
        assertThat(run("help")).contains("removenode <node-id>");
    }

    @Test
    void statusAndAdminCommandsOnASingleNode() {
        assertThat(run("status")).contains("(this node)");
        assertThat(run("cleanup")).endsWith("Removed 0 keys this node no longer replicates");
        assertThat(run("removenode", "nobody")).startsWith("FAILED Error:").contains("Unknown node");
    }
}
