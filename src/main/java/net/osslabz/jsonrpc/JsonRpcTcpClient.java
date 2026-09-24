package net.osslabz.jsonrpc;

import static net.osslabz.jsonrpc.JsonRpcFieldNames.ERROR;
import static net.osslabz.jsonrpc.JsonRpcFieldNames.ID;
import static net.osslabz.jsonrpc.JsonRpcFieldNames.RESULT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonRpcTcpClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(JsonRpcTcpClient.class);

    private static final Duration DEFAULT_CALL_TIMEOUT = Duration.ofSeconds(30);

    private final String host;

    private final int port;

    private final Duration callTimeout;

    private final ObjectMapper objectMapper;

    private final AtomicLong idGenerator = new AtomicLong(0);

    private final Map<Long, PendingCall> pendingResponses = new ConcurrentHashMap<>();

    private final TcpLineConnection connection;

    public JsonRpcTcpClient(String host, int port) {

        this(host, port, DEFAULT_CALL_TIMEOUT);
    }

    public JsonRpcTcpClient(String host, int port, Duration callTimeout) {

        this.host = host;
        this.port = port;
        this.callTimeout = callTimeout;
        this.objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.registerModule(new JavaTimeModule());

        this.connection = new TcpLineConnection(host, port, new ConnectionEvents());
    }

    public CompletableFuture<JsonNode> callAsync(String method, Object params) {

        if (connection.isClosed()) {
            return CompletableFuture.failedFuture(new JsonRpcException("Client is closed"));
        }
        if (connection.hasFailed()) {
            return CompletableFuture.failedFuture(
                    new JsonRpcException("Client is disconnected from %s:%d".formatted(host, port)));
        }

        long id = idGenerator.incrementAndGet();

        JsonRpcRequest request = new JsonRpcRequest(id, method, params);

        CompletableFuture<JsonNode> future = new CompletableFuture<>();

        try {
            String requestJson = objectMapper.writeValueAsString(request);
            pendingResponses.put(id, new PendingCall(future, requestJson));
            connection.send(requestJson);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        future.orTimeout(callTimeout.toMillis(), TimeUnit.MILLISECONDS);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                pendingResponses.remove(id);
                if (ex instanceof TimeoutException) {
                    log.warn("Request {} ({}) timed out after {}", id, method, callTimeout);
                }
            }
        });

        return future;
    }

    public JsonNode call(String method, Object params) {

        JsonNode rawResponse = awaitResponse(method, callAsync(method, params));

        log.debug("Raw response: {}", rawResponse);

        if (rawResponse == null) {
            throw new JsonRpcException("No response received in time.");
        }
        if (rawResponse.has(RESULT)) {
            return rawResponse.get(RESULT);
        }
        if (rawResponse.has(ERROR)) {
            throw new JsonRpcException(readError(rawResponse.get(ERROR)));
        }
        throw new JsonRpcException("Received Invalid JSON-RPC Response (no result and no error)");
    }

    private JsonNode awaitResponse(String method, CompletableFuture<JsonNode> future) {

        try {
            return future.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                throw new JsonRpcException("RPC call '%s' timed out after %s".formatted(method, callTimeout), e);
            }
            throw callFailed(e);
        }
    }

    private JsonRpcError readError(JsonNode error) {

        try {
            return this.objectMapper.treeToValue(error, JsonRpcError.class);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw callFailed(e);
        }
    }

    private static JsonRpcException callFailed(Exception cause) {

        return new JsonRpcException("Failed to execute RPC call: %s".formatted(cause.getMessage()), cause);
    }

    public <T> List<T> callAndMapList(String method, Object params, Class<T> returnType) {

        JsonNode result = this.call(method, params);
        try {
            return this.objectMapper.readerForListOf(returnType).readValue(result);
        } catch (IOException e) {
            throw new JsonRpcException(e);
        }
    }

    private void processResponse(String rawJson) {

        try {
            JsonNode jsonNode = objectMapper.readValue(rawJson, JsonNode.class);
            if (!jsonNode.has(ID)) {
                log.warn("Received message without ID field, ignoring: {}", rawJson);
                return;
            }
            Long id = jsonNode.get(ID).asLong();
            log.debug("Received response for request {}", id);
            PendingCall pending = pendingResponses.remove(id);
            if (pending == null) {
                log.debug("Received response for unknown request ID {}", id);
                return;
            }
            pending.future().complete(jsonNode);
        } catch (Exception e) {
            log.error("Failed to parse JSON-RPC response: {}", rawJson, e);
        }
    }

    private void failPendingCalls(JsonRpcException cause) {

        pendingResponses.forEach((id, pending) -> pending.future().completeExceptionally(cause));
        pendingResponses.clear();
    }

    @Override
    public void close() {

        if (connection.isClosed()) {
            return;
        }

        log.info("Closing JSON-RPC client for {}:{}", host, port);
        connection.close();
        failPendingCalls(new JsonRpcException("Client closed"));
    }

    private record PendingCall(CompletableFuture<JsonNode> future, String requestJson) {}

    private final class ConnectionEvents implements TcpLineConnection.Listener {

        @Override
        public void lineReceived(String line) {

            processResponse(line);
        }

        @Override
        public List<String> linesToResend() {

            return pendingResponses.values().stream()
                    .map(PendingCall::requestJson)
                    .toList();
        }

        @Override
        public void reconnectFailed(JsonRpcException cause) {

            failPendingCalls(cause);
        }
    }
}
