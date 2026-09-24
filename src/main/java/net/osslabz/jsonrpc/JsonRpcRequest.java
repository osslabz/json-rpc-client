package net.osslabz.jsonrpc;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonRpcRequest {

    private static final String VERSION = "2.0";

    private final Long id;

    private final String method;

    private final Object params;

    public JsonRpcRequest(Long id, String method, Object params) {

        this.id = id;
        this.method = method;
        this.params = params;
    }

    public String getMethod() {

        return method;
    }

    public Object getParams() {

        return params;
    }

    public Long getId() {

        return id;
    }

    @JsonProperty("jsonrpc")
    public String getVersion() {

        return VERSION;
    }
}
