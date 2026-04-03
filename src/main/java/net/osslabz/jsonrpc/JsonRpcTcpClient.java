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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonRpcTcpClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(JsonRpcTcpClient.class);

    private static final int BUFFER_CAPACITY = 4096;

    private static final Duration DEFAULT_CALL_TIMEOUT = Duration.ofSeconds(30);

    private final String host;

    private final int port;

    private final Duration callTimeout;

    private SocketChannel socketChannel;

    private final ObjectMapper objectMapper;

    private int totalConnectCount = 0;

    private final AtomicLong idGenerator = new AtomicLong(0);

    private final ConcurrentLinkedQueue<String> pendingRequests = new ConcurrentLinkedQueue<>();

    private Map<Long, CompletableFuture<JsonNode>> pendingResponses = new ConcurrentHashMap<>();

    private Selector selector;

    private volatile boolean monitorSocket = true;

    private volatile boolean connected = false;

    private final StringBuilder readBuffer = new StringBuilder();


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

        Thread selectorThread = new Thread(this::processSelectorEvents);
        selectorThread.setDaemon(true);
        selectorThread.start();

        if (!this.reconnectSocket()) {
            throw new JsonRpcException("Initial connection to socket failed.");
        }
    }


    private void processSelectorEvents() {

        while (this.monitorSocket) {
            try {
                if (!this.connected) {
                    Thread.sleep(100);
                    continue;
                }

                int readyChannels = this.selector.select(100);
                if (readyChannels == 0) {
                    continue;
                }

                Iterator<SelectionKey> keyIterator = this.selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    try {
                        if (key.isReadable()) {
                            readData(key);
                            if (!connected) {
                                break;
                            }
                        }
                        if (key.isWritable() && !pendingRequests.isEmpty()) {
                            writeData(key);
                        }
                    } catch (IOException e) {
                        log.error("I/O error in selector loop for {}:{}", host, port, e);
                        connected = false;
                        break;
                    } catch (Exception e) {
                        log.error("Unexpected error in selector loop for {}:{}", host, port, e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug("Selector thread interrupted");
            } catch (Exception e) {
                log.error("Critical error in selector loop for {}:{}", host, port, e);
                connected = false;
            }
        }
        log.debug("Selector thread exiting for {}:{}", host, port);
    }


    private void writeData(SelectionKey key) throws IOException {

        SocketChannel channel = (SocketChannel) key.channel();
        String request = pendingRequests.poll();

        if (request != null) {
            log.debug("Sending request: {}", request);
            ByteBuffer buffer = ByteBuffer.wrap((request + "\n").getBytes(StandardCharsets.UTF_8));
            channel.write(buffer);
        }

    }


    public CompletableFuture<JsonNode> callAsync(String method, Object params) {

        long id = idGenerator.incrementAndGet();

        JsonRpcRequest request = new JsonRpcRequest(id, method, params);

        CompletableFuture<JsonNode> future = new CompletableFuture<>();

        try {
            String requestJson = objectMapper.writeValueAsString(request);
            pendingResponses.put(id, future);
            pendingRequests.add(requestJson);
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

        CompletableFuture<JsonNode> future = callAsync(method, params);
        try {
            JsonNode rawResponse = future.join();

            log.debug("Raw response: {}", rawResponse);

            if (rawResponse == null) {
                throw new JsonRpcException("No response received in time.");
            }

            if (rawResponse.has(RESULT)) {
                return rawResponse.get(RESULT);
            } else if (rawResponse.has(ERROR)) {
                JsonRpcError errorResponse = this.objectMapper.treeToValue(rawResponse.get(ERROR), JsonRpcError.class);
                throw new JsonRpcException(errorResponse);
            } else {
                throw new JsonRpcException("Received Invalid JSON-RPC Response (no result and no error)");
            }
        } catch (Exception e) {
            if (e instanceof JsonRpcException je) {
                throw je;
            }
            if (e instanceof CompletionException ce && ce.getCause() instanceof TimeoutException) {
                throw new JsonRpcException("RPC call '%s' timed out after %s".formatted(method, callTimeout));
            }
            throw new JsonRpcException("Failed to execute RPC call: %s".formatted(e.getMessage()));
        }
    }


    public <T> T callAndMap(String method, Object params, Class<T> returnType) {

        JsonNode result = this.call(method, params);
        try {
            return this.objectMapper.treeToValue(result, returnType);
        } catch (JsonProcessingException e) {
            throw new JsonRpcException(e);
        }
    }


    public <T> List<T> callAndMapList(String method, Object params, Class<T> returnType) {

        JsonNode result = this.call(method, params);
        try {
            return this.objectMapper.readerForListOf(returnType).readValue(result);
        } catch (IOException e) {
            throw new JsonRpcException(e);
        }

    }


    private void readData(SelectionKey key) throws IOException {

        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_CAPACITY);

        int bytesRead;
        while ((bytesRead = channel.read(buffer)) > 0) {
            buffer.flip();
            readBuffer.append(StandardCharsets.UTF_8.decode(buffer));
            buffer.clear();
        }

        if (bytesRead == -1) {
            log.warn("Connection closed by server {}:{}", host, port);
            connected = false;
            return;
        }

        String data = readBuffer.toString();
        int lastNewline = data.lastIndexOf('\n');
        if (lastNewline == -1) {
            return;
        }

        String completeData = data.substring(0, lastNewline);
        readBuffer.delete(0, lastNewline + 1);

        for (String line : completeData.split("\n")) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                processResponse(trimmed);
            }
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
            CompletableFuture<JsonNode> future = pendingResponses.remove(id);
            if (future == null) {
                log.debug("Received response for unknown request ID {}", id);
                return;
            }
            future.complete(jsonNode);
        } catch (Exception e) {
            log.error("Failed to parse JSON-RPC response: {}", rawJson, e);
        }
    }


    private boolean reconnectSocket() {

        totalConnectCount++;

        try {
            log.debug("{} connection attempt to '{}:{}'", this.totalConnectCount, this.host, this.port);
            this.socketChannel = SocketChannel.open(new InetSocketAddress(this.host, this.port));

            log.debug("Successfully connected.");

            if (this.socketChannel.isBlocking()) {
                log.trace("Socket channel is blocking, reconfiguring to unblocking...");
                this.socketChannel.configureBlocking(false);
            }
            this.selector = Selector.open();
            this.socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT);

            connected = true;

            return true;
        } catch (Exception e) {
            log.warn("Exception while connecting to socket.");
            return false;
        }
    }


    public void close() {

        this.connected = false;
        this.monitorSocket = false;

        try {
            // this closes also the inputStream and outputStream
            this.socketChannel.close();
        } catch (IOException e) {
            log.warn("Couldn't close the socket: {}.", e.getMessage());
        }
    }
}