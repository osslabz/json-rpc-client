package net.osslabz.jsonrpc;

public class JsonRcpException extends RuntimeException {

    public JsonRcpException(String message) {

        super(message);

    }


    public JsonRcpException(Throwable e) {

        super(e);
    }


    public JsonRcpException(JsonRpcError errorResponse) {

        super("%s: %s".formatted(errorResponse.getCode(), errorResponse.getMessage()));
    }
}