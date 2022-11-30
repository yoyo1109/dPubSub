package edu.scu.utils;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

public class Connector {
    private Socket socket;
    private BufferedReader reader;
    private PrintWriter writer;

    public Connector(Socket socket) {
        this.socket = socket;
        this.setup();
    }

    private void setup() {
        InputStream inputStream = null;
        OutputStream outputStream = null;

        try {
            inputStream = this.socket.getInputStream();
            outputStream = this.socket.getOutputStream();
        } catch (IOException var5) {
            var5.printStackTrace();
            return;
        }

        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        this.reader = new BufferedReader(inputStreamReader);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
        this.writer = new PrintWriter(outputStreamWriter);
    }

    public String readLine() {
        try {
            return this.reader.readLine();
        } catch (IOException var2) {
            var2.printStackTrace();
            this.close();
            return null;
        }
    }

    public void writeLine(String data) {
        this.writer.write(data + "\n");
        this.writer.flush();
    }

    public void close() {
        try {
            this.socket.close();
        } catch (IOException var2) {
            var2.printStackTrace();
        }

    }

    public int getRemotePort() {
        return socket.getPort();
    }
}

