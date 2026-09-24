package net.osslabz.jsonrpc;

public class JsonRpcException extends RuntimeException {

    public JsonRpcException(String message) {

        super(message);
    }

    public JsonRpcException(String message, Throwable cause) {

        super(message, cause);
    }

    public JsonRpcException(Throwable e) {

        super(e);
    }

    public JsonRpcException(JsonRpcError errorResponse) {

        super("%s: %s".formatted(errorResponse.getCode(), errorResponse.getMessage()));
    }
}
