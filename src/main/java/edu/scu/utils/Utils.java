package edu.scu.utils;

import edu.scu.utils.Connector;
import java.io.IOException;
import java.net.Socket;

public class Utils {

    public static ResponseBase SendTo(String address, int port, String command, String payload) throws IOException {
        Socket socket = new Socket(address, port);
        ResponseBase resp = SendTo(socket, command, payload);
        socket.close();
        return resp;
    }

    public static ResponseBase SendTo(Socket socket, String command, String payload) throws IOException {
        Connector connector = new Connector(socket);
        connector.writeLine(command);
        connector.writeLine(payload);

        String rcvStatus = connector.readLine();
        String rcvData = connector.readLine();
        return new ResponseBase(rcvStatus, rcvData);
    }

    public static class RequestBase {
        public String command;
        public String payload;

        public RequestBase(String command, String payload) {
            this.command = command;
            this.payload = payload;
        }
    }

    public static class ResponseBase {
        public String status;
        public String data;

        public ResponseBase(String status, String data) {
            this.status = status;
            this.data = data;
        }

        public boolean Ok() {
            return status.equals("OK");
        }


    }
}
