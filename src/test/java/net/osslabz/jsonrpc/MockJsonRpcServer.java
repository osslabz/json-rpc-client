package net.osslabz.jsonrpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MockJsonRpcServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(MockJsonRpcServer.class);

    private final ServerSocket serverSocket;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Map<String, Function<JsonNode, Object>> handlers = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors.newCachedThreadPool();

    private volatile boolean running = true;

    private volatile Duration responseDelay = Duration.ZERO;

    private volatile int disconnectAfterRequests = -1;

    private volatile String rawResponse = null;


    public MockJsonRpcServer() throws IOException {

        this.serverSocket = new ServerSocket(0);
        executor.submit(this::acceptLoop);
    }


    public int getPort() {

        return serverSocket.getLocalPort();
    }


    public void handle(String method, Function<JsonNode, Object> handler) {

        handlers.put(method, handler);
    }


    public void setResponseDelay(Duration delay) {

        this.responseDelay = delay;
    }


    public void disconnectAfterRequests(int n) {

        this.disconnectAfterRequests = n;
    }


    public void sendRawResponse(String raw) {

        this.rawResponse = raw;
    }


    private void acceptLoop() {

        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                executor.submit(() -> handleClient(clientSocket));
            } catch (SocketException e) {
                if (running) {
                    log.warn("SocketException in accept loop: {}", e.getMessage());
                }
            } catch (IOException e) {
                if (running) {
                    log.warn("IOException in accept loop: {}", e.getMessage());
                }
            }
        }
    }


    private void handleClient(Socket clientSocket) {

        try (Socket socket = clientSocket;
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8)) {

            int requestCount = 0;
            String line;

            while (running && (line = reader.readLine()) != null) {
                requestCount++;
                log.debug("Received request: {}", line);

                if (!responseDelay.isZero()) {
                    Thread.sleep(responseDelay.toMillis());
                }

                if (rawResponse != null) {
                    writer.println(rawResponse);
                    writer.flush();
                    continue;
                }

                JsonNode requestNode = objectMapper.readTree(line);
                String method = requestNode.get("method").asText();
                JsonNode params = requestNode.get("params");
                long id = requestNode.get("id").asLong();

                ObjectNode response = objectMapper.createObjectNode();
                response.put("jsonrpc", "2.0");
                response.put("id", id);

                Function<JsonNode, Object> handler = handlers.get(method);
                if (handler != null) {
                    Object result = handler.apply(params);
                    response.set("result", objectMapper.valueToTree(result));
                } else {
                    ObjectNode error = objectMapper.createObjectNode();
                    error.put("code", -32601);
                    error.put("message", "Method not found");
                    response.set("error", error);
                }

                String responseJson = objectMapper.writeValueAsString(response);
                log.debug("Sending response: {}", responseJson);
                writer.println(responseJson);
                writer.flush();

                if (disconnectAfterRequests >= 0 && requestCount >= disconnectAfterRequests) {
                    log.debug("Disconnecting after {} requests", requestCount);
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (running) {
                log.warn("Error handling client: {}", e.getMessage());
            }
        }
    }


    @Override
    public void close() throws IOException {

        running = false;
        serverSocket.close();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
