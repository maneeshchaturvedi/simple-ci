package com.simpleci.testrunner;

import com.simpleci.common.CommunicationConstants;
import com.simpleci.helpers.Helpers;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class TestRunner {
    private static final Logger logger = Logger.getLogger(TestRunner.class.getName());

    private final String runnerHost;
    private final int runnerPort;
    private final String repoPath;
    private final String dispatcherHost;
    private final int dispatcherPort;

    private volatile boolean busy = false;
    private volatile long lastCommunication = System.currentTimeMillis();
    private volatile boolean dead = false;

    private ServerSocket serverSocket;
    private final ExecutorService connectionPool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public TestRunner(String runnerHost, int runnerPort, String repoPath, String dispatcherHost, int dispatcherPort)
            throws IOException {
        this.runnerHost = runnerHost;
        this.runnerPort = runnerPort;
        this.repoPath = repoPath;
        this.dispatcherHost = dispatcherHost;
        this.dispatcherPort = dispatcherPort;
        this.serverSocket = new ServerSocket(runnerPort, 50, InetAddress.getByName(runnerHost));
    }

    public void start() {
        // Register with dispatcher.
        String registerCommand = CommunicationConstants.REGISTER_CMD + ":" + runnerHost + ":" + runnerPort;
        String response = Helpers.communicate(dispatcherHost, dispatcherPort, registerCommand);
        if (!CommunicationConstants.OK_RESPONSE.equals(response)) {
            logger.severe("Unable to register with dispatcher!");
            System.exit(1);
        }
        logger.info("Registered with dispatcher");

        // Schedule periodic dispatcher connectivity check.
        scheduler.scheduleAtFixedRate(new DispatcherChecker(), 5, 5, TimeUnit.SECONDS);

        // Accept incoming connections.
        connectionPool.submit(() -> {
            while (!dead) {
                try {
                    Socket socket = serverSocket.accept();
                    connectionPool.submit(new TestRunnerHandler(socket));
                } catch (IOException e) {
                    if (!dead) {
                        logger.severe("Error accepting connection: " + e.getMessage());
                    }
                }
            }
        });
    }

    private void shutdown() {
        dead = true;
        scheduler.shutdown();
        connectionPool.shutdown();
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.warning("Error closing server socket: " + e.getMessage());
        }
    }

    // Handler for incoming commands from the dispatcher.
    private class TestRunnerHandler implements Runnable {
        private final Socket socket;

        public TestRunnerHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                String commandLine = in.readLine();
                if (commandLine == null) {
                    return;
                }
                if (commandLine.startsWith(CommunicationConstants.PING_CMD)) {
                    lastCommunication = System.currentTimeMillis();
                    out.println(CommunicationConstants.PONG_RESPONSE);
                } else if (commandLine.startsWith(CommunicationConstants.RUNT_TEST_CMD)) {
                    if (busy) {
                        out.println("BUSY");
                    } else {
                        out.println(CommunicationConstants.OK_RESPONSE);
                        String commit = commandLine.substring(CommunicationConstants.RUNT_TEST_CMD.length() + 1);
                        busy = true;
                        runTests(commit);
                        busy = false;
                    }
                } else {
                    out.println("Invalid command");
                }
            } catch (IOException e) {
                logger.severe("Error in TestRunnerHandler: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (IOException ex) {
                }
            }
        }
    }

    private void runTests(String commit) {
        logger.info("Running tests for commit " + commit);
        // Update repository: checkout the given commit.
        try {
            ProcessBuilder pb = new ProcessBuilder("git", "checkout", commit);
            pb.directory(new File(repoPath));
            Process process = pb.start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                logger.warning("Git checkout failed for commit " + commit);
            }
        } catch (IOException | InterruptedException e) {
            logger.warning("Error during git checkout: " + e.getMessage());
        }
        // Simulate test execution.
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        String testResult = "Tests passed for commit " + commit;
        // Write result to file.
        try {
            Path resultsDir = Paths.get("test_results");
            if (!Files.exists(resultsDir)) {
                Files.createDirectories(resultsDir);
            }
            Path resultFile = resultsDir.resolve(commit + ".txt");
            Files.write(resultFile, testResult.getBytes());
        } catch (IOException e) {
            logger.warning("Error writing test results: " + e.getMessage());
        }
        // Send results to dispatcher.
        String resultsCommand = CommunicationConstants.RESULTS_CMD + ":" + commit + ":" + testResult.length() + ":"
                + testResult;
        String dispatcherResponse = Helpers.communicate(dispatcherHost, dispatcherPort, resultsCommand);
        logger.info("Dispatcher response for results: " + dispatcherResponse);
    }

    // Checks connectivity with the dispatcher.
    private class DispatcherChecker implements Runnable {
        @Override
        public void run() {
            if (System.currentTimeMillis() - lastCommunication > 10000) {
                String statusResponse = Helpers.communicate(dispatcherHost, dispatcherPort,
                        CommunicationConstants.STATUS_CMD);
                if (!CommunicationConstants.OK_RESPONSE.equals(statusResponse)) {
                    logger.severe("Dispatcher is no longer functional. Shutting down test runner.");
                    shutdown();
                }
            }
        }
    }

    public static void main(String[] args) {
        // Expected args: <host> <port> <dispatcherServer(host:port)> <repoPath>
        if (args.length < 4) {
            System.out.println(
                    "Usage: java com.simpleci.testrunner.TestRunner <host> <port> <dispatcherServer(host:port)> <repoPath>");
            System.exit(1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String[] dispatcherParts = args[2].split(":");
        String dispatcherHost = dispatcherParts[0];
        int dispatcherPort = Integer.parseInt(dispatcherParts[1]);
        String repoPath = args[3];
        try {
            TestRunner runner = new TestRunner(host, port, repoPath, dispatcherHost, dispatcherPort);
            runner.start();
        } catch (IOException e) {
            logger.severe("Error starting test runner: " + e.getMessage());
        }
    }
}