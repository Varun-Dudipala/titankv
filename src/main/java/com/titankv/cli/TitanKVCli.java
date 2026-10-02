package com.titankv.cli;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import com.titankv.consistency.ConsistencyLevel;
import com.titankv.util.Env;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

/**
 * Command-line client. With a command it runs it and exits; without one it starts a shell.
 *
 * <pre>
 *   titankv-cli localhost:9001 put user:1 Ada
 *   titankv-cli localhost:9001            # interactive
 * </pre>
 */
public final class TitanKVCli {

    private static final String HELP = String.join("\n",
            "Commands:",
            "  get <key>                     print the value, or (nil)",
            "  put <key> <value> [ttl-ms]    store a value, optionally expiring",
            "  delete <key>                  delete a key",
            "  exists <key>                  true or false",
            "  ping                          check the connection",
            "  status                        cluster members as seen by the connected node",
            "  removenode <node-id>          permanently remove a DEAD node",
            "  cleanup                       drop keys the node no longer replicates (after joins)",
            "  consistency [ONE|QUORUM|ALL|default]  show or set the level for get/put/delete/exists",
            "  help                          this text",
            "  quit                          exit the shell");

    // Level for data commands; null uses the server's default
    private static ConsistencyLevel level;

    private TitanKVCli() {
    }

    public static void main(String[] args) {
        if (args.length < 1 || args[0].equals("--help")) {
            System.out.println("Usage: titankv-cli <host:port> [--consistency ONE|QUORUM|ALL] [command [args...]]");
            System.out.println(HELP);
            return;
        }
        int first = 1;
        if (args.length > 2 && args[1].equals("--consistency")) {
            if (!setLevel(args[2])) {
                System.exit(1);
            }
            first = 3;
        }
        try (TitanKVClient client = new TitanKVClient(args[0])) {
            if (args.length > first) {
                boolean ok = run(client, args[0], Arrays.copyOfRange(args, first, args.length));
                if (!ok) {
                    System.exit(1);
                }
                return;
            }
            shell(client, args[0]);
        }
    }

    /**
     * A client for removenode and cleanup, which need the cluster token (TITANKV_CLUSTER_SECRET or
     * TITANKV_INTERNAL_TOKEN) when the cluster runs with authentication.
     */
    private static TitanKVClient adminClient(String host) {
        ClientConfig config = new ClientConfig();
        String token = Env.internalToken();
        if (token != null) {
            config.setAuthToken(token);
        }
        return new TitanKVClient(config, host);
    }

    private static void shell(TitanKVClient client, String host) {
        System.out.println("Connected to " + host + ". Type 'help' for commands.");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        while (true) {
            System.out.print("titankv> ");
            System.out.flush();
            String line;
            try {
                line = in.readLine();
            } catch (IOException e) {
                return;
            }
            if (line == null || line.trim().equals("quit") || line.trim().equals("exit")) {
                return;
            }
            if (!line.isBlank()) {
                run(client, host, line.trim().split("\\s+"));
            }
        }
    }

    /**
     * @return false if the command failed
     */
    static boolean run(TitanKVClient client, String host, String[] args) {
        String command = args[0].toLowerCase();
        try {
            switch (command) {
                case "get":
                    requireArgs(args, 2, "get <key>");
                    Optional<String> value = client.getString(args[1], level);
                    System.out.println(value.orElse("(nil)"));
                    return true;
                case "put":
                    requireArgs(args, 3, "put <key> <value> [ttl-ms]");
                    long ttl = args.length > 3 ? Long.parseLong(args[3]) : 0;
                    client.put(args[1], args[2].getBytes(StandardCharsets.UTF_8), ttl, level);
                    System.out.println("OK");
                    return true;
                case "delete":
                case "del":
                    requireArgs(args, 2, "delete <key>");
                    client.delete(args[1], level);
                    System.out.println("OK");
                    return true;
                case "exists":
                    requireArgs(args, 2, "exists <key>");
                    System.out.println(client.exists(args[1], level));
                    return true;
                case "ping":
                    boolean up = client.ping();
                    System.out.println(up ? "PONG" : "Connection failed");
                    return up;
                case "status":
                    System.out.print(client.clusterStatus());
                    return true;
                case "removenode":
                    requireArgs(args, 2, "removenode <node-id>");
                    try (TitanKVClient admin = adminClient(host)) {
                        admin.removeClusterNode(args[1]);
                    }
                    System.out.println("Removed " + args[1] + "; its data is being re-replicated");
                    return true;
                case "cleanup":
                    try (TitanKVClient admin = adminClient(host)) {
                        System.out.println("Removed " + admin.cleanup() + " keys this node no longer replicates");
                    }
                    return true;
                case "consistency":
                    if (args.length > 1) {
                        return setLevel(args[1]);
                    }
                    System.out.println(level != null ? level : "default (the server's, QUORUM unless configured)");
                    return true;
                case "help":
                    System.out.println(HELP);
                    return true;
                default:
                    System.out.println("Unknown command '" + args[0] + "'. Type 'help' for commands.");
                    return false;
            }
        } catch (IOException | IllegalArgumentException e) {
            System.out.println("Error: " + e.getMessage());
            return false;
        }
    }

    private static boolean setLevel(String name) {
        if (name.equalsIgnoreCase("default")) {
            level = null;
            return true;
        }
        try {
            level = ConsistencyLevel.valueOf(name.toUpperCase());
            return true;
        } catch (IllegalArgumentException e) {
            System.out.println("Unknown consistency level '" + name + "' (ONE, QUORUM, ALL or default)");
            return false;
        }
    }

    private static void requireArgs(String[] args, int count, String usage) {
        if (args.length < count) {
            throw new IllegalArgumentException("usage: " + usage);
        }
    }
}
