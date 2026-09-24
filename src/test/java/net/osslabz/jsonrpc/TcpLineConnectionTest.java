package net.osslabz.jsonrpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class TcpLineConnectionTest {

    private final RecordingListener listener = new RecordingListener();

    @Test
    void deliversEachReceivedLineToTheListener() throws Exception {

        try (ServerSocket serverSocket = new ServerSocket(0);
                TcpLineConnection connection =
                        new TcpLineConnection("localhost", serverSocket.getLocalPort(), listener);
                Socket accepted = serverSocket.accept()) {
            OutputStream out = accepted.getOutputStream();
            out.write("first\n  \nsecond\n".getBytes(StandardCharsets.UTF_8));
            out.flush();

            assertEquals("first", listener.lines.poll(5, TimeUnit.SECONDS));
            assertEquals("second", listener.lines.poll(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void throwsWhenTheServerCannotBeReached() {

        assertThrows(JsonRpcException.class, () -> new TcpLineConnection("localhost", 1, listener));
    }

    @Test
    void closingTwiceLeavesTheConnectionClosed() throws Exception {

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            TcpLineConnection connection = new TcpLineConnection("localhost", serverSocket.getLocalPort(), listener);

            connection.close();
            connection.close();

            assertTrue(connection.isClosed());
        }
    }

    @Test
    void idleConnectionLeavesTheCpuAlone() throws Exception {

        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        try (ServerSocket serverSocket = new ServerSocket(0);
                TcpLineConnection connection =
                        new TcpLineConnection("localhost", serverSocket.getLocalPort(), listener)) {
            String threadName = "json-rpc-selector-localhost:" + serverSocket.getLocalPort();
            Thread selectorThread = Thread.getAllStackTraces().keySet().stream()
                    .filter(thread -> thread.getName().equals(threadName))
                    .findFirst()
                    .orElseThrow();

            long cpuBefore = threads.getThreadCpuTime(selectorThread.threadId());
            long wallBefore = System.nanoTime();
            Thread.sleep(1000);
            long cpu = threads.getThreadCpuTime(selectorThread.threadId()) - cpuBefore;
            long wall = System.nanoTime() - wallBefore;

            // An idle loop blocks in select(); a spinning one burns hundreds of ms per second even on a loaded host.
            assertTrue(
                    cpu < wall * 0.05,
                    "Idle selector thread used %d ms CPU in %d ms".formatted(cpu / 1_000_000, wall / 1_000_000));
        }
    }

    private static final class RecordingListener implements TcpLineConnection.Listener {

        private final BlockingQueue<String> lines = new LinkedBlockingQueue<>();

        @Override
        public void lineReceived(String line) {

            lines.add(line);
        }

        @Override
        public List<String> linesToResend() {

            return List.of();
        }

        @Override
        public void reconnectFailed(JsonRpcException cause) {}
    }
}
