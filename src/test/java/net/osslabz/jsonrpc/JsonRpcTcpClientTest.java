package net.osslabz.jsonrpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.ArrayList;
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


    @Test
    void serverErrorIncludesCodeAndMessage() {

        try (JsonRpcTcpClient client = new JsonRpcTcpClient("localhost", server.getPort())) {
            JsonRpcException ex = assertThrows(JsonRpcException.class,
                () -> client.call("nonexistent", List.of()));

            assertTrue(ex.getMessage().contains("-32601"), "Should contain error code");
            assertTrue(ex.getMessage().contains("Method not found"), "Should contain error message");
        }
    }


    @Test
    void callTimesOutWhenServerDoesNotRespond() {

        server.setResponseDelay(Duration.ofSeconds(10));
        server.handle("slow", params -> "ok");

        try (JsonRpcTcpClient client = new JsonRpcTcpClient("localhost", server.getPort(), Duration.ofMillis(500))) {
            assertThrows(JsonRpcException.class, () -> client.call("slow", List.of()));
        }
    }


    @Test
    void selectorThreadSurvivesMalformedResponse() throws Exception {

        server.sendRawResponse("this is not valid json\n");

        try (JsonRpcTcpClient client = new JsonRpcTcpClient("localhost", server.getPort(), Duration.ofMillis(2000))) {
            // First call gets the raw malformed response - should timeout or fail gracefully,
            // but NOT kill the selector thread
            assertThrows(JsonRpcException.class, () -> client.call("test1", List.of()));

            // Second call: server should handle normally
            server.handle("test2", params -> "survived");
            JsonNode result = client.call("test2", List.of());
            assertEquals("survived", result.asText());
        }
    }


    @Test
    void closeCompletesAllPendingFutures() throws Exception {

        server.setResponseDelay(Duration.ofSeconds(60));
        server.handle("slow", params -> "ignored");

        try (JsonRpcTcpClient client = new JsonRpcTcpClient("localhost", server.getPort(), Duration.ofSeconds(60))) {
            CompletableFuture<JsonNode> f1 = client.callAsync("slow", List.of());
            CompletableFuture<JsonNode> f2 = client.callAsync("slow", List.of());

            Thread.sleep(200);

            client.close();

            // Both futures must complete within 2 seconds -- if close() doesn't
            // complete them, they'd hang for the full 60s call timeout
            assertThrows(Exception.class, () -> f1.get(2, TimeUnit.SECONDS));
            assertThrows(Exception.class, () -> f2.get(2, TimeUnit.SECONDS));
        }
    }


    @Test
    void callAfterCloseFailsFast() throws Exception {

        server.handle("test", params -> "ok");
        JsonRpcTcpClient client = new JsonRpcTcpClient("localhost", server.getPort());

        client.call("test", List.of());

        client.close();

        // Should throw immediately with JsonRpcException, not hang for the 30s default timeout
        long start = System.nanoTime();
        assertThrows(JsonRpcException.class, () -> client.call("anything", List.of()));
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(elapsed < 2000, "call after close should fail fast, took " + elapsed + "ms");
    }


    @Test
    void handlesBurstOfRequests() throws Exception {

        server.handle("ping", params -> "pong");

        try (JsonRpcTcpClient client = new JsonRpcTcpClient("localhost", server.getPort())) {
            int count = 50;
            List<CompletableFuture<JsonNode>> futures = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                futures.add(client.callAsync("ping", List.of()));
            }

            for (CompletableFuture<JsonNode> f : futures) {
                JsonNode response = f.get(10, TimeUnit.SECONDS);
                assertNotNull(response);
            }
        }
    }


    @Test
    void timeoutCleansPendingResponses() throws Exception {

        server.setResponseDelay(Duration.ofMillis(1500));
        server.handle("slow", params -> "ignored");

        try (JsonRpcTcpClient client = new JsonRpcTcpClient("localhost", server.getPort(), Duration.ofMillis(200))) {
            CompletableFuture<JsonNode> future = client.callAsync("slow", List.of());

            assertThrows(Exception.class, () -> future.get(2, TimeUnit.SECONDS));

            // Wait for the server to finish processing the delayed request
            // so it can process the next one on the same connection
            Thread.sleep(2000);

            // Verify subsequent calls still work on the same client (no resource leak)
            server.setResponseDelay(Duration.ZERO);
            server.handle("echo", params -> "ok");
            JsonNode result = client.call("echo", List.of());
            assertNotNull(result);
        }
    }
}
