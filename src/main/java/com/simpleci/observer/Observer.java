package com.simpleci.observer;

import com.simpleci.common.CommunicationConstants;
import java.io.*;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.logging.*;

public class Observer {
    private static final Logger logger = Logger.getLogger(Observer.class.getName());
    private static final String DISPATCHER_HOST = "localhost";
    private static final int DISPATCHER_PORT = 8000;
    private final String repoPath;
    private String lastCommit = "";
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public Observer(String repoPath) {
        this.repoPath = repoPath;
    }

    public void start() {
        logger.info("Observer started for repository: " + repoPath);
        // Poll the repository for new commits every 5 seconds.
        scheduler.scheduleWithFixedDelay(this::checkForCommitChanges, 0, 5, TimeUnit.SECONDS);
    }

    private void checkForCommitChanges() {
        String currentCommit = getCurrentCommitId();
        if (currentCommit == null || currentCommit.isEmpty()) {
            logger.warning("Failed to retrieve commit id from repository.");
            return;
        }
        if (!currentCommit.equals(lastCommit)) {
            logger.info("New commit detected: " + currentCommit);
            lastCommit = currentCommit;
            sendCommit(currentCommit);
        }
    }

    // Uses the git command to get the current commit id.
    private String getCurrentCommitId() {
        try {
            ProcessBuilder pb = new ProcessBuilder("git", "rev-parse", "HEAD");
            pb.directory(new File(repoPath));
            Process process = pb.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String commitId = reader.readLine();
            process.waitFor();
            return commitId != null ? commitId.trim() : "";
        } catch (IOException | InterruptedException e) {
            logger.severe("Error retrieving current commit id: " + e.getMessage());
            return "";
        }
    }

    private void sendCommit(String commitId) {
        try (Socket socket = new Socket(DISPATCHER_HOST, DISPATCHER_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            out.println(CommunicationConstants.COMMIT_CMD + commitId);
            String response = in.readLine();
            if (!CommunicationConstants.ACK.equals(response)) {
                logger.warning("Dispatcher did not acknowledge commit.");
            } else {
                logger.info("Commit " + commitId + " sent to dispatcher.");
            }
        } catch (IOException e) {
            logger.severe("Error connecting to dispatcher: " + e.getMessage());
            throw new RuntimeException("Dispatcher is down", e);
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java com.ci.observer.Observer <git_repo_path>");
            System.exit(1);
        }
        String repoPath = args[0];
        Observer observer = new Observer(repoPath);
        observer.start();
    }
}
