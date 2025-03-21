package com.simpleci.observer;

import com.simpleci.common.CommunicationConstants;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class Observer {
    private static final Logger logger = Logger.getLogger(Observer.class.getName());

    private final String repoPath;
    private final String dispatcherHost;
    private final int dispatcherPort;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public Observer(String repoPath, String dispatcherHost, int dispatcherPort) {
        this.repoPath = repoPath;
        this.dispatcherHost = dispatcherHost;
        this.dispatcherPort = dispatcherPort;
    }

    private String getCurrentCommit() {
        try {
            ProcessBuilder pb = new ProcessBuilder("git", "rev-parse", "HEAD");
            pb.directory(new File(repoPath));
            Process process = pb.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String commit = reader.readLine();
            process.waitFor();
            return commit != null ? commit.trim() : "";
        } catch (IOException | InterruptedException e) {
            logger.severe("Error getting current commit: " + e.getMessage());
            return "";
        }
    }

    private void sendCommit(String commit) {
        try (Socket socket = new Socket(dispatcherHost, dispatcherPort);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String command = CommunicationConstants.DISPATCH_CMD + ":" + commit;
            out.println(command);
            String response = in.readLine();
            logger.info("Dispatcher response: " + response);
        } catch (IOException e) {
            logger.severe("Error sending commit to dispatcher: " + e.getMessage());
        }
    }

    public void start() {
        final String[] lastCommit = { "" };
        scheduler.scheduleAtFixedRate(() -> {
            String currentCommit = getCurrentCommit();
            if (!currentCommit.isEmpty() && !currentCommit.equals(lastCommit[0])) {
                logger.info("New commit detected: " + currentCommit);
                sendCommit(currentCommit);
                lastCommit[0] = currentCommit;
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        // Expected args: <repoPath> <dispatcherHost> <dispatcherPort>
        if (args.length < 3) {
            System.out
                    .println("Usage: java com.simpleci.observer.Observer <repoPath> <dispatcherHost> <dispatcherPort>");
            System.exit(1);
        }
        String repoPath = args[0];
        String dispatcherHost = args[1];
        int dispatcherPort = Integer.parseInt(args[2]);
        Observer observer = new Observer(repoPath, dispatcherHost, dispatcherPort);
        observer.start();
    }
}