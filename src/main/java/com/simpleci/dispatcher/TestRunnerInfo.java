package com.simpleci.dispatcher;

public class TestRunnerInfo {
    private final String host;
    private final int port;

    public TestRunnerInfo(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "TestRunnerInfo{host=" + host + ", port=" + port + "}";
    }
}