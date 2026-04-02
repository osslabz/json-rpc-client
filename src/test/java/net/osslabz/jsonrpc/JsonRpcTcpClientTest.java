package net.osslabz.jsonrpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
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
}
