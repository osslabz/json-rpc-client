package net.osslabz.jsonrpc;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A TCP connection that exchanges newline-delimited messages and reconnects when the server drops it.
 *
 * <p>A daemon thread reads and writes the socket and calls the {@link Listener} on that thread.
 */
final class TcpLineConnection implements Closeable {

    /** Callbacks from the connection's thread. */
    interface Listener {

        void lineReceived(String line);

        /** Returns the lines to send again on the new connection. */
        List<String> linesToResend();

        void reconnectFailed(JsonRpcException cause);
    }

    private static final Logger log = LoggerFactory.getLogger(TcpLineConnection.class);

    private static final int BUFFER_CAPACITY = 4096;

    private static final int MAX_RECONNECT_ATTEMPTS = 3;

    private final String host;

    private final int port;

    private final Listener listener;

    private SocketChannel socketChannel;

    private Selector selector;

    private final AtomicInteger totalConnectCount = new AtomicInteger();

    private final Queue<String> outgoingLines = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean monitorSocket = new AtomicBoolean(true);

    private final AtomicBoolean connected = new AtomicBoolean();

    private final AtomicBoolean everConnected = new AtomicBoolean();

    private final AtomicBoolean failed = new AtomicBoolean();

    private final Utf8LineDecoder lineDecoder = new Utf8LineDecoder();

    private final Thread selectorThread;

    /** Connects and starts the connection's thread; throws {@link JsonRpcException} if the server can't be reached. */
    TcpLineConnection(String host, int port, Listener listener) {

        this.host = host;
        this.port = port;
        this.listener = listener;

        if (!this.reconnectSocket()) {
            throw new JsonRpcException("Initial connection to socket failed.");
        }

        this.selectorThread = new Thread(this::processSelectorEvents, "json-rpc-selector-%s:%d".formatted(host, port));
        this.selectorThread.setDaemon(true);
        this.selectorThread.start();
    }

    void send(String line) {

        outgoingLines.add(line);
    }

    boolean isClosed() {

        return !monitorSocket.get();
    }

    /** Whether reconnecting gave up; the connection stays unusable until closed. */
    boolean hasFailed() {

        return failed.get();
    }

    private void processSelectorEvents() {

        while (monitorSocket.get()) {
            try {
                if (connected.get()) {
                    processReadyKeys();
                } else {
                    awaitReconnect();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug("Selector thread interrupted");
            } catch (Exception e) {
                log.error("Critical error in selector loop for {}:{}", host, port, e);
                connected.set(false);
            }
        }
        log.debug("Selector thread exiting for {}:{}", host, port);
    }

    private void awaitReconnect() throws InterruptedException {

        if (everConnected.get() && monitorSocket.get() && !failed.get()) {
            handleConnectionLoss();
        }
        if (!connected.get()) {
            Thread.sleep(100);
        }
    }

    private void processReadyKeys() throws IOException {

        int readyChannels = this.selector.select(100);
        if (readyChannels == 0) {
            return;
        }

        Iterator<SelectionKey> keyIterator = this.selector.selectedKeys().iterator();
        while (keyIterator.hasNext()) {
            SelectionKey key = keyIterator.next();
            keyIterator.remove();

            if (key.isValid() && !processKey(key)) {
                break;
            }
        }
    }

    /** Returns false once the connection is lost. */
    private boolean processKey(SelectionKey key) {

        try {
            if (key.isReadable()) {
                readData((SocketChannel) key.channel());
                if (!connected.get()) {
                    return false;
                }
            }
            if (key.isWritable() && !outgoingLines.isEmpty()) {
                writeData((SocketChannel) key.channel());
            }
        } catch (IOException e) {
            log.error("I/O error in selector loop for {}:{}", host, port, e);
            connected.set(false);
            return false;
        } catch (Exception e) {
            log.error("Unexpected error in selector loop for {}:{}", host, port, e);
        }
        return true;
    }

    private void writeData(SocketChannel channel) throws IOException {

        String line;
        while ((line = outgoingLines.poll()) != null) {
            log.debug("Sending request: {}", line);
            ByteBuffer buffer = ByteBuffer.wrap((line + "\n").getBytes(StandardCharsets.UTF_8));
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }
    }

    private void readData(SocketChannel channel) throws IOException {

        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_CAPACITY);

        List<String> lines = new ArrayList<>();
        int bytesRead;
        while ((bytesRead = channel.read(buffer)) > 0) {
            buffer.flip();
            lines.addAll(lineDecoder.decode(buffer));
            buffer.clear();
        }

        for (String line : lines) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                listener.lineReceived(trimmed);
            }
        }

        if (bytesRead == -1) {
            log.warn("Connection closed by server {}:{}", host, port);
            connected.set(false);
        }
    }

    private boolean reconnectSocket() {

        int connectAttempt = totalConnectCount.incrementAndGet();

        try {
            log.debug("{} connection attempt to '{}:{}'", connectAttempt, this.host, this.port);
            this.socketChannel = SocketChannel.open(new InetSocketAddress(this.host, this.port));

            log.info("Connected to {}:{}", this.host, this.port);

            if (this.socketChannel.isBlocking()) {
                log.trace("Socket channel is blocking, reconfiguring to unblocking...");
                this.socketChannel.configureBlocking(false);
            }
            this.selector = Selector.open();
            this.socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

            connected.set(true);
            everConnected.set(true);

            return true;
        } catch (Exception e) {
            log.warn("Failed to connect to {}:{}: {}", this.host, this.port, e.getMessage());
            closeQuietly(socketChannel);
            return false;
        }
    }

    private void handleConnectionLoss() {

        lineDecoder.reset();
        closeQuietly(socketChannel);
        closeQuietly(selector);

        for (int attempt = 1; attempt <= MAX_RECONNECT_ATTEMPTS; attempt++) {
            if (!monitorSocket.get()) {
                return;
            }

            long backoffMs = 1000L * (1 << (attempt - 1));
            log.info(
                    "Reconnecting to {}:{} (attempt {}/{}) in {}ms",
                    host,
                    port,
                    attempt,
                    MAX_RECONNECT_ATTEMPTS,
                    backoffMs);

            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if (reconnectSocket()) {
                log.info("Successfully reconnected to {}:{}", host, port);
                List<String> linesToResend = listener.linesToResend();
                outgoingLines.addAll(linesToResend);
                log.info("Re-queued {} pending requests", linesToResend.size());
                return;
            }
        }

        log.error("Failed to reconnect to {}:{} after {} attempts", host, port, MAX_RECONNECT_ATTEMPTS);
        failed.set(true);
        listener.reconnectFailed(
                new JsonRpcException("Connection lost to %s:%d and reconnection failed after %d attempts"
                        .formatted(host, port, MAX_RECONNECT_ATTEMPTS)));
        outgoingLines.clear();
    }

    /** Stops the connection's thread and closes the socket. Idempotent. */
    @Override
    public void close() {

        if (!monitorSocket.get()) {
            return;
        }

        monitorSocket.set(false);
        connected.set(false);

        if (selector != null && selector.isOpen()) {
            selector.wakeup();
        }

        try {
            selectorThread.join(2000);
            if (selectorThread.isAlive()) {
                selectorThread.interrupt();
                selectorThread.join(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        outgoingLines.clear();

        closeQuietly(socketChannel);
        closeQuietly(selector);
    }

    private void closeQuietly(Closeable resource) {

        if (resource != null) {
            try {
                resource.close();
            } catch (IOException e) {
                log.debug("Error closing resource: {}", e.getMessage());
            }
        }
    }
}
