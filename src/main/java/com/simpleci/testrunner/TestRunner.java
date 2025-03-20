package com.simpleci.testrunner;

import com.simpleci.common.CommunicationConstants;
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

public class TestRunner {
    private static final Logger logger = Logger.getLogger(TestRunner.class.getName());
    private static final String DISPATCHER_HOST = "localhost";
    private static final int DISPATCHER_PORT = 8000;
    private final String identifier;

    public TestRunner(String identifier) {
        this.identifier = identifier;
    }

    public void start() {
        try (Socket socket = new Socket(DISPATCHER_HOST, DISPATCHER_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            // Register with the dispatcher.
            out.println(CommunicationConstants.REGISTER_CMD + identifier);
            logger.info("Test Runner " + identifier + " registered with dispatcher.");
            String command;
            while ((command = in.readLine()) != null) {
                if (command.startsWith(CommunicationConstants.TEST_CMD)) {
                    String commitId = command.substring(CommunicationConstants.TEST_CMD.length());
                    logger.info("Received test command for commit " + commitId);
                    runTest(commitId);
                    out.println(CommunicationConstants.DONE);
                }
            }
            logger.info("Dispatcher disconnected. Shutting down test runner " + identifier);
        } catch (UnknownHostException e) {
            logger.severe("Unknown host: " + DISPATCHER_HOST);
        } catch (IOException e) {
            logger.severe("I/O error in test runner: " + e.getMessage());
        }
    }

    private void runTest(String commitId) {
        logger.info("Running tests for commit " + commitId + "...");
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logger.info("Tests completed for commit " + commitId);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java com.ci.testrunner.TestRunner <identifier>");
            System.exit(1);
        }
        String identifier = args[0];
        TestRunner runner = new TestRunner(identifier);
        runner.start();
    }
}
