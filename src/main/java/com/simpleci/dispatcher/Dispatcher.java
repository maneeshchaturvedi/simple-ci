package com.simpleci.dispatcher;

import com.simpleci.common.CommunicationConstants;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class Dispatcher {
    private static final Logger logger = Logger.getLogger(Dispatcher.class.getName());
    private static final int DISPATCHER_PORT = 8000;
    private ServerSocket serverSocket;
    // Map of available test runners keyed by identifier.
    private final ConcurrentMap<String, TestRunnerHandler> testRunners = new ConcurrentHashMap<>();
    // Queue of commit tasks.
    private final BlockingQueue<String> commitQueue = new LinkedBlockingQueue<>();

    // Thread pool for handling incoming connections.
    private final ExecutorService connectionPool = Executors.newCachedThreadPool();
    // Single-thread executor for dispatching commits.
    private final ScheduledExecutorService dispatcherExecutor = Executors.newSingleThreadScheduledExecutor();

    public Dispatcher() throws IOException {
        serverSocket = new ServerSocket(DISPATCHER_PORT);
    }

    public void start() {
        logger.info("Dispatcher started on port " + DISPATCHER_PORT);

        // Schedule the commit dispatcher task.
        dispatcherExecutor.scheduleWithFixedDelay(() -> {
            try {
                String commit = commitQueue.take();
                dispatchTest(commit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warning("Dispatcher thread interrupted");
            }
        }, 0, 1, TimeUnit.SECONDS);

        // Accept and handle incoming connections.
        while (!serverSocket.isClosed()) {
            try {
                Socket socket = serverSocket.accept();
                connectionPool.submit(new ConnectionHandler(socket));
            } catch (IOException e) {
                logger.severe("Error accepting connection: " + e.getMessage());
            }
        }
        shutdownExecutors();
    }

    public void addCommit(String commitId) {
        logger.info("Received commit: " + commitId);
        commitQueue.offer(commitId);
    }

    private void dispatchTest(String commitId) {
        logger.info("Dispatching commit " + commitId);
        Optional<TestRunnerHandler> runnerOpt = testRunners.values().stream().findAny();
        if (runnerOpt.isPresent()) {
            TestRunnerHandler runner = runnerOpt.get();
            boolean sent = runner.sendTest(commitId);
            if (!sent) {
                logger.warning("Test runner failed. Removing from pool and requeueing commit " + commitId);
                testRunners.remove(runner.getIdentifier());
                commitQueue.offer(commitId);
            }
        } else {
            logger.warning("No available test runners. Requeueing commit " + commitId);
            commitQueue.offer(commitId);
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void registerTestRunner(String identifier, TestRunnerHandler handler) {
        logger.info("Registering test runner: " + identifier);
        testRunners.put(identifier, handler);
    }

    public void removeTestRunner(String identifier) {
        logger.info("Removing test runner: " + identifier);
        testRunners.remove(identifier);
    }

    private void shutdownExecutors() {
        dispatcherExecutor.shutdown();
        connectionPool.shutdown();
    }

    // Handles incoming connections from both the observer and test runners.
private class ConnectionHandler implements Runnable {
    private Socket socket;
    public ConnectionHandler(Socket socket) {
        this.socket = socket;
    }
    
    public void run() {
        try {
            // Create the reader and writer without try-with-resources so we can control socket closing.
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String line = in.readLine();
            if (line == null) {
                socket.close();
                return;
            }
            if (line.startsWith(CommunicationConstants.REGISTER_CMD)) {
                // For test runner registration, do not close the socket.
                String identifier = line.substring(CommunicationConstants.REGISTER_CMD.length());
                TestRunnerHandler runnerHandler = new TestRunnerHandler(socket, identifier, Dispatcher.this);
                registerTestRunner(identifier, runnerHandler);
                // The TestRunnerHandler now manages the socket.
            } else if (line.startsWith(CommunicationConstants.COMMIT_CMD)) {
                // For observer messages, process and then close the socket.
                String commitId = line.substring(CommunicationConstants.COMMIT_CMD.length());
                addCommit(commitId);
                out.println(CommunicationConstants.ACK);
                socket.close();
            } else {
                out.println("Unknown command");
                socket.close();
            }
        } catch (IOException e) {
            logger.severe("Connection error: " + e.getMessage());
            try {
                socket.close();
            } catch (IOException ex) {
                // Ignore further closing errors.
            }
        }
    }
}
    public static void main(String[] args) {
        try {
            Dispatcher dispatcher = new Dispatcher();
            dispatcher.start();
        } catch (IOException e) {
            logger.severe("Failed to start dispatcher: " + e.getMessage());
        }
    }
}
