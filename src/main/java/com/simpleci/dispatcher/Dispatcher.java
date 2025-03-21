package com.simpleci.dispatcher;

import com.simpleci.common.CommunicationConstants;
import com.simpleci.helpers.Helpers;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;
import java.util.logging.*;

public class Dispatcher {
    private static final Logger logger = Logger.getLogger(Dispatcher.class.getName());

    // Configuration
    private final String host;
    private final int port;

    // Internal state
    private volatile boolean dead = false;
    private final List<TestRunnerInfo> runners = new CopyOnWriteArrayList<>();
    private final Map<String, TestRunnerInfo> dispatchedCommits = new ConcurrentHashMap<>();
    private final List<String> pendingCommits = new CopyOnWriteArrayList<>();

    private ServerSocket serverSocket;

    // Concurrency management
    private final ExecutorService connectionPool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

    public Dispatcher(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        this.serverSocket = new ServerSocket(port, 50, InetAddress.getByName(host));
    }

    public void start() {
        logger.info(String.format("Dispatcher serving on %s:%d", host, port));

        // Schedule periodic tasks.
        scheduler.scheduleAtFixedRate(new RunnerChecker(), 1, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(new Redistributor(), 1, 1, TimeUnit.SECONDS);

        // Start connection acceptance in an asynchronous task.
        connectionPool.submit(() -> {
            while (!dead) {
                try {
                    Socket socket = serverSocket.accept();
                    connectionPool.submit(new DispatcherHandler(socket));
                } catch (IOException e) {
                    if (!dead) {
                        logger.severe("Error accepting connection: " + e.getMessage());
                    }
                }
            }
        });
    }

    public void dispatchTests(String commitId) {
        // Use a CompletableFuture to run the dispatch loop asynchronously.
        CompletableFuture.runAsync(() -> {
            while (true) {
                logger.info("Attempting to dispatch commit " + commitId + " to runners");
                for (TestRunnerInfo runner : runners) {
                    String response = Helpers.communicate(runner.getHost(), runner.getPort(),
                            CommunicationConstants.RUNT_TEST_CMD + ":" + commitId);
                    if (CommunicationConstants.OK_RESPONSE.equals(response)) {
                        logger.info("Dispatched commit " + commitId + " to runner " + runner);
                        dispatchedCommits.put(commitId, runner);
                        pendingCommits.remove(commitId);
                        return;
                    }
                }
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, connectionPool);
    }

    // Called by the DispatcherHandler when an observer sends a commit.
    public void addCommit(String commitId) {
        logger.info("Received commit: " + commitId);
        pendingCommits.add(commitId);
    }

    // Handler for runner registration.
    public void registerTestRunner(TestRunnerInfo runner) {
        logger.info("Registering runner: " + runner);
        runners.add(runner);
    }

    // Called internally to remove a runner and requeue its commit.
    private void removeRunnerAndRequeue(TestRunnerInfo runner) {
        runners.remove(runner);
        for (Map.Entry<String, TestRunnerInfo> entry : dispatchedCommits.entrySet()) {
            if (entry.getValue().equals(runner)) {
                String commit = entry.getKey();
                dispatchedCommits.remove(commit);
                pendingCommits.add(commit);
                break;
            }
        }
    }

    // Graceful shutdown.
    public void shutdown() {
        dead = true;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.warning("Error closing server socket: " + e.getMessage());
        }
        scheduler.shutdown();
        connectionPool.shutdown();
    }

    // --- Internal Classes ---

    private class DispatcherHandler implements Runnable {
        private final Socket socket;
        private final int BUF_SIZE = 1024;
        private final Pattern commandPattern = Pattern.compile("(\\w+)(:.+)?");

        public DispatcherHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                char[] buffer = new char[BUF_SIZE];
                int numRead = in.read(buffer);
                if (numRead == -1) {
                    socket.close();
                    return;
                }
                String data = new String(buffer, 0, numRead).trim();
                Matcher matcher = commandPattern.matcher(data);
                if (!matcher.matches()) {
                    out.println("Invalid command");
                    return;
                }
                String command = matcher.group(1);
                String arg = matcher.group(2);
                if (arg != null) {
                    arg = arg.substring(1);
                }
                if (CommunicationConstants.STATUS_CMD.equalsIgnoreCase(command)) {
                    out.println(CommunicationConstants.OK_RESPONSE);
                } else if (CommunicationConstants.REGISTER_CMD.equalsIgnoreCase(command)) {
                    // Expected format: register:host:port
                    String[] parts = arg.split(":");
                    if (parts.length < 2) {
                        out.println("Invalid register command");
                    } else {
                        String runnerHost = parts[0];
                        int runnerPort = Integer.parseInt(parts[1]);
                        TestRunnerInfo runner = new TestRunnerInfo(runnerHost, runnerPort);
                        registerTestRunner(runner);
                        out.println(CommunicationConstants.OK_RESPONSE);
                    }
                } else if (CommunicationConstants.DISPATCH_CMD.equalsIgnoreCase(command)) {
                    // Expected format: dispatch:<commitId>
                    String commitId = arg;
                    if (runners.isEmpty()) {
                        out.println("No runners are registered");
                    } else {
                        out.println(CommunicationConstants.OK_RESPONSE);
                        dispatchTests(commitId);
                    }
                } else if (CommunicationConstants.RESULTS_CMD.equalsIgnoreCase(command)) {
                    // Expected format: results:<commitId>:<length>:<output>
                    if (arg == null) {
                        out.println("Invalid results command");
                    } else {
                        String[] parts = arg.split(":", 3);
                        if (parts.length < 3) {
                            out.println("Invalid results command");
                        } else {
                            String commitId = parts[0];
                            int lengthMsg = Integer.parseInt(parts[1]);
                            String resultOutput = parts[2];
                            while (resultOutput.length() < lengthMsg) {
                                char[] extra = new char[BUF_SIZE];
                                int extraRead = in.read(extra);
                                if (extraRead == -1) break;
                                resultOutput += new String(extra, 0, extraRead);
                            }
                            dispatchedCommits.remove(commitId);
                            Path resultsDir = Paths.get("test_results");
                            if (!Files.exists(resultsDir)) {
                                Files.createDirectories(resultsDir);
                            }
                            Path resultFile = resultsDir.resolve(commitId + ".txt");
                            Files.write(resultFile, resultOutput.getBytes());
                            out.println(CommunicationConstants.OK_RESPONSE);
                        }
                    }
                } else {
                    out.println("Invalid command");
                }
            } catch (IOException e) {
                logger.severe("Connection error: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (IOException ex) { }
            }
        }
    }

    private class RunnerChecker implements Runnable {
        @Override
        public void run() {
            for (TestRunnerInfo runner : runners) {
                try {
                    String response = Helpers.communicate(runner.getHost(), runner.getPort(), CommunicationConstants.PING_CMD);
                    if (!CommunicationConstants.PONG_RESPONSE.equalsIgnoreCase(response)) {
                        logger.info("Removing runner (bad response): " + runner);
                        removeRunnerAndRequeue(runner);
                    }
                } catch (Exception ex) {
                    logger.info("Removing runner (exception): " + runner);
                    removeRunnerAndRequeue(runner);
                }
            }
        }
    }

    private class Redistributor implements Runnable {
        @Override
        public void run() {
            for (String commit : pendingCommits) {
                logger.info("Redistributing commit: " + commit);
                dispatchTests(commit);
            }
        }
    }

    public static void main(String[] args) {
        String host = "localhost";
        int port = 8888;
        if (args.length >= 1) host = args[0];
        if (args.length >= 2) port = Integer.parseInt(args[1]);
        try {
            Dispatcher dispatcher = new Dispatcher(host, port);
            dispatcher.start();
        } catch (IOException e) {
            logger.severe("Error starting dispatcher: " + e.getMessage());
        }
    }
}