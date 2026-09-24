package net.osslabz.jsonrpc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonRpcRequestTest {

    @Test
    void serializesAsJsonRpcTwoRequest() throws Exception {

        String json = new ObjectMapper().writeValueAsString(new JsonRpcRequest(7L, "getblock", List.of("abc")));

        assertEquals("{\"id\":7,\"method\":\"getblock\",\"params\":[\"abc\"],\"jsonrpc\":\"2.0\"}", json);
    }
}
