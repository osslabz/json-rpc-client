package net.osslabz.jsonrpc;

import com.fasterxml.jackson.annotation.JsonProperty;


public class JsonRpcRequest {

    @JsonProperty("jsonrpc")
    private final String version = "2.0";

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


    public String getVersion() {

        return version;
    }
}