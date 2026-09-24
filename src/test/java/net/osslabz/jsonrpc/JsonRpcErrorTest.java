package net.osslabz.jsonrpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class JsonRpcErrorTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void readsCodeMessageAndDataFromJson() throws Exception {

        JsonRpcError error = objectMapper.readValue(
                "{\"code\":-32602,\"message\":\"Invalid params\",\"data\":{\"param\":\"height\"}}", JsonRpcError.class);

        assertEquals(-32602, error.getCode());
        assertEquals("Invalid params", error.getMessage());
        assertEquals("height", error.getData().get("param").asText());
    }

    @Test
    void acceptsMissingData() {

        JsonRpcError error = new JsonRpcError();
        error.setData(null);

        assertNull(error.getData());
    }
}
