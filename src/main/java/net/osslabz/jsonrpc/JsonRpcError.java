package net.osslabz.jsonrpc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;


public class JsonRpcError {

    private int code;

    private String message;

    private ObjectNode data;


    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonRpcError that = (JsonRpcError) o;
        return code == that.code && Objects.equals(message, that.message) && Objects.equals(data, that.data);
    }


    @Override
    public int hashCode() {

        return Objects.hash(code, message, data);
    }


    public int getCode() {

        return code;
    }


    public void setCode(int code) {

        this.code = code;
    }


    public String getMessage() {

        return message;
    }


    public void setMessage(String message) {

        this.message = message;
    }


    public ObjectNode getData() {

        return data;
    }


    public void setData(ObjectNode data) {

        this.data = data;
    }
}