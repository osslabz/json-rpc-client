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
import java.nio.CharBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonRpcTcpClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(JsonRpcTcpClient.class);

    private static final int BUFFER_CAPACITY = 4096;

    private final String host;

    private final int port;

    private SocketChannel socketChannel;

    private final ObjectMapper objectMapper;

    private int totalConnectCount = 0;

    private final AtomicLong idGenerator = new AtomicLong(0);

    private final ConcurrentLinkedQueue<String> pendingRequests = new ConcurrentLinkedQueue<>();

    private Map<Long, CompletableFuture<JsonNode>> pendingResponses = new ConcurrentHashMap<>();

    private Selector selector;

    private volatile boolean monitorSocket = true;

    private volatile boolean connected = false;


    public JsonRpcTcpClient(String host, int port) {

        this.host = host;
        this.port = port;
        this.objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.registerModule(new JavaTimeModule());

        Thread selectorThread = new Thread(this::processSelectorEvents);
        selectorThread.setDaemon(true);
        selectorThread.start();

        if (!this.reconnectSocket()) {
            throw new JsonRcpException("Initial connection to socket failed.");
        }
    }


    private void processSelectorEvents() {

        try {
            while (this.monitorSocket) {

                if (!this.connected) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    continue;
                }

                // Wait for events
                int readyChannels = this.selector.select(100); // Small timeout to avoid blocking forever

                if (readyChannels == 0) {
                    log.trace("No channels selected.");
                    continue;
                }

                Iterator<SelectionKey> keyIterator = this.selector.selectedKeys().iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    try {
                        if (!key.isValid()) {
                            log.trace("Key {} is invalid, skipping key.", key);
                            continue;
                        }

                        if (key.isConnectable()) {
                            log.trace("Key {} is connectable, skipping key.", key);
                        }

                        if (key.isReadable()) {
                            log.trace("Key {} is readable, reading data...", key);
                            readData(key);
                        }

                        if (key.isWritable() && !pendingRequests.isEmpty()) {
                            log.trace("Key is writable, sending pending requests...");
                            writeData(key);
                        }
                    } catch (IOException e) {
                        pendingRequests.clear();
                        pendingResponses.forEach((id, future) ->
                            future.completeExceptionally(e));
                        pendingResponses.clear();
                    }
                }
            }
        } catch (Exception e) {
            throw new JsonRcpException(e);
        }
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

        return future;
    }


    public JsonNode call(String method, Object params) {

        CompletableFuture<JsonNode> future = callAsync(method, params);
        try {
            JsonNode rawResponse = future.join();

            log.debug("Raw response: {}", rawResponse);

            if (rawResponse == null) {
                throw new JsonRcpException("No response received in time.");
            }

            if (rawResponse.has(RESULT)) {
                return rawResponse.get(RESULT);
            } else if (rawResponse.has(ERROR)) {
                JsonRpcError errorResponse = this.objectMapper.treeToValue(rawResponse, JsonRpcError.class);
                throw new JsonRcpException(errorResponse);
            } else {
                throw new JsonRcpException("Received Invalid JSON-RPC Response (no result and no error)");
            }
        } catch (Exception e) {
            if (e instanceof JsonRcpException je) {
                throw je;
            }
            throw new JsonRcpException("Failed to execute RPC call: %s".formatted(e.getMessage()));
        }
    }


    public <T> T callAndMap(String method, Object params, Class<T> returnType) {

        JsonNode result = this.call(method, params);
        try {
            return this.objectMapper.treeToValue(result, returnType);
        } catch (JsonProcessingException e) {
            throw new JsonRcpException(e);
        }
    }


    public <T> List<T> callAndMapList(String method, Object params, Class<T> returnType) {

        JsonNode result = this.call(method, params);
        try {
            return this.objectMapper.readerForListOf(returnType).readValue(result);
        } catch (IOException e) {
            throw new JsonRcpException(e);
        }

    }


    private void readData(SelectionKey key) {

        try {
            log.trace("Socket notified about new data to read, reading in chunks of {}.", BUFFER_CAPACITY);

            StringBuilder sb = new StringBuilder();
            int bytesRead = -1;
            int i = 0;
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_CAPACITY);

            while ((bytesRead = ((SocketChannel) key.channel()).read(buffer)) > 0) {
                i++;
                log.trace("{} bytes read in {} round.", bytesRead, i);
                CharBuffer decode = StandardCharsets.UTF_8.decode(buffer.slice(0, bytesRead));
                sb.append(decode);
            }
            if (sb.isEmpty()) {
                log.trace("received empty message, ignoring.");
                return;
            }
            String rawResponse = sb.toString();
            log.trace("Full message read, total length is {}.", rawResponse.length());

            JsonNode jsonNode = this.objectMapper.readValue(rawResponse, JsonNode.class);
            if (!jsonNode.has(ID)) {
                log.warn("Invalid message received, no ID field found.");
                return;
            }

            Long id = jsonNode.get(ID).asLong();

            CompletableFuture<JsonNode> future = this.pendingResponses.remove(id);
            if (future == null) {
                log.debug("Received a message for an unknown ID {}", id);
                return;
            }

            future.complete(jsonNode);

        } catch (Exception e) {
            throw new JsonRcpException(e);
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