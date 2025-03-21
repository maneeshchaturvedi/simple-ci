package com.simpleci.helpers;

import java.io.*;
import java.net.*;
import java.util.logging.*;

public class Helpers {
    private static final int BUF_SIZE = 1024;
    private static final Logger logger = Logger.getLogger(Helpers.class.getName());

    /**
     * Opens a TCP connection to the given host and port, sends the request, and
     * returns the response.
     */
    public static String communicate(String host, int port, String request) {
        try (Socket socket = new Socket(host, port)) {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out.println(request);
            char[] buffer = new char[BUF_SIZE];
            int numRead = in.read(buffer);
            if (numRead == -1) {
                return null;
            }
            return new String(buffer, 0, numRead).trim();
        } catch (Exception e) {
            logger.warning("Error communicating with " + host + ":" + port + " - " + e.getMessage());
            return null;
        }
    }
}