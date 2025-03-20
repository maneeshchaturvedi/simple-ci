package com.simpleci.dispatcher;

import com.simpleci.common.CommunicationConstants;
import java.io.*;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.logging.*;

public class TestRunnerHandler {
    private static final Logger logger = Logger.getLogger(TestRunnerHandler.class.getName());
    private Socket socket;
    private String identifier;
    private Dispatcher dispatcher;
    private PrintWriter out;
    private BufferedReader in;
    // A queue to safely transfer messages read from the input stream.
    private final BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
    // Executor to run the reader thread.
    private final ExecutorService readerExecutor = Executors.newSingleThreadExecutor();

    public TestRunnerHandler(Socket socket, String identifier, Dispatcher dispatcher) throws IOException {
        this.socket = socket;
        this.identifier = identifier;
        this.dispatcher = dispatcher;
        this.out = new PrintWriter(socket.getOutputStream(), true);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        startReader();
    }

    public String getIdentifier() {
        return identifier;
    }

    // Starts a dedicated thread to read from the socket and enqueue messages.
    private void startReader() {
        readerExecutor.submit(() -> {
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    logger.fine("Received from test runner " + identifier + ": " + line);
                    messageQueue.offer(line);
                }
            } catch (IOException e) {
                logger.warning("Error reading from test runner " + identifier + ": " + e.getMessage());
            } finally {
                dispatcher.removeTestRunner(identifier);
                try {
                    socket.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        });
    }

    // Sends a test command to the test runner and waits for the "DONE" response.
    public boolean sendTest(String commitId) {
        try {
            out.println(CommunicationConstants.TEST_CMD + commitId);
            // Wait up to 30 seconds for a response.
            String response = messageQueue.poll(30, TimeUnit.SECONDS);
            if (CommunicationConstants.DONE.equals(response)) {
                logger.info("Test runner " + identifier + " completed commit " + commitId);
                return true;
            } else {
                logger.warning("Unexpected response from test runner " + identifier + ": " + response);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warning("Interrupted while waiting for test runner response: " + e.getMessage());
        }
        return false;
    }
}
