package net.osslabz.jsonrpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class JsonRpcTcpClientTest {

    private MockJsonRpcServer server;


    @BeforeEach
    void setUp() throws Exception {

        server = new MockJsonRpcServer();
    }


    @AfterEach
    void tearDown() throws Exception {

        if (server != null) {
            server.close();
        }
    }


    @Test
    void callReturnsResult() throws Exception {

        server.handle("echo", params -> params);

        try (JsonRpcTcpClient client = new JsonRpcTcpClient("localhost", server.getPort())) {
            JsonNode result = client.call("echo", List.of("hello"));
            assertNotNull(result);
            assertTrue(result.isArray());
            assertEquals(1, result.size());
            assertEquals("hello", result.get(0).asText());
        }
    }


    @Test
    void handlesMultipleResponsesInSingleRead() throws Exception {

        server.handle("fast", params -> "ok");

        try (JsonRpcTcpClient client = new JsonRpcTcpClient("localhost", server.getPort())) {
            CompletableFuture<JsonNode> f1 = client.callAsync("fast", List.of());
            CompletableFuture<JsonNode> f2 = client.callAsync("fast", List.of());
            CompletableFuture<JsonNode> f3 = client.callAsync("fast", List.of());

            assertNotNull(f1.get(5, TimeUnit.SECONDS));
            assertNotNull(f2.get(5, TimeUnit.SECONDS));
            assertNotNull(f3.get(5, TimeUnit.SECONDS));
        }
    }


    @Test
    void handlesLargeResponse() throws Exception {

        String largeValue = "x".repeat(8000);
        server.handle("large", params -> largeValue);

        try (JsonRpcTcpClient client = new JsonRpcTcpClient("localhost", server.getPort())) {
            JsonNode result = client.call("large", List.of());
            assertEquals(largeValue, result.asText());
        }
    }
}
