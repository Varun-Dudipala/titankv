package com.titankv.benchmark;

import com.titankv.network.protocol.BinaryProtocol;
import com.titankv.network.protocol.Command;
import com.titankv.network.protocol.Response;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Compares bytes on the wire for a PUT and a GET round trip: TitanKV's binary protocol versus an
 * equivalent minimal JSON-over-HTTP/1.1 API. Binary values are base64-encoded in JSON, since JSON
 * has no byte type. The HTTP messages use only the headers a keep-alive client must send.
 */
public final class ProtocolOverhead {

    private static final String KEY = "user:12345";

    public static void main(String[] args) {
        System.out.println("Bytes on the wire per round trip (request + response), key \"" + KEY + "\"");
        System.out.println();
        System.out.printf("%-8s | %-24s | %-24s | %s%n", "value", "PUT binary / JSON-HTTP", "GET binary / JSON-HTTP",
                "binary saves (PUT, GET)");
        System.out.println("---------+--------------------------+--------------------------+------------------------");
        for (int size : new int[] {10, 100, 1_000, 10_000}) {
            byte[] value = new byte[size];
            int putBinary = BinaryProtocol.encode(Command.put(KEY, value)).remaining()
                    + BinaryProtocol.encode(Response.ok()).remaining();
            int putHttp = httpPutRequest(value).length + httpResponse("").length;
            int getBinary = BinaryProtocol.encode(Command.get(KEY)).remaining()
                    + BinaryProtocol.encode(Response.ok(value, System.currentTimeMillis(), 0)).remaining();
            int getHttp = httpGetRequest().length + httpResponse(jsonValue(value)).length;
            System.out.printf("%6d B | %8d / %-13d | %8d / %-13d | %3.0f%%, %3.0f%%%n", size,
                    putBinary, putHttp, getBinary, getHttp,
                    100.0 * (1 - (double) putBinary / putHttp), 100.0 * (1 - (double) getBinary / getHttp));
        }
    }

    private static String jsonValue(byte[] value) {
        return "{\"value\":\"" + Base64.getEncoder().encodeToString(value) + "\",\"timestamp\":1727200000000}";
    }

    private static byte[] httpPutRequest(byte[] value) {
        String body = jsonValue(value);
        return ("PUT /v1/kv/" + KEY + " HTTP/1.1\r\n"
                + "Host: localhost:9001\r\n"
                + "Content-Type: application/json\r\n"
                + "Content-Length: " + body.length() + "\r\n\r\n" + body).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] httpGetRequest() {
        return ("GET /v1/kv/" + KEY + " HTTP/1.1\r\n"
                + "Host: localhost:9001\r\n\r\n").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] httpResponse(String body) {
        String contentType = body.isEmpty() ? "" : "Content-Type: application/json\r\n";
        return ("HTTP/1.1 200 OK\r\n" + contentType
                + "Content-Length: " + body.length() + "\r\n\r\n" + body).getBytes(StandardCharsets.UTF_8);
    }
}
