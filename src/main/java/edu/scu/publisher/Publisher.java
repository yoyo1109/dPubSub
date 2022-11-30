package edu.scu.publisher;

import edu.scu.utils.Utils;
import edu.scu.utils.Utils.ResponseBase;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Scanner;

import static edu.scu.utils.Utils.SendTo;

public class Publisher {
    // The latest broker address, should be updated periodically.
    private static int brokerPort = 0;

    public static void main(String[] args) throws IOException {
        System.out.println("Started Publisher..");
        // Get leader from FrontEnd Server.
        brokerPort = GetCurrentBroker();
        System.out.println("Received current broker address: localhost:"+brokerPort);

        // Get input;
        Scanner scanner = new Scanner(System.in);
        String publisherMsg = scanner.nextLine();
        Pub(publisherMsg);
        System.out.println("END");
    }

    private static void Pub(String data) throws IOException {
        String[] tokens = data.split(" ");
        if (tokens.length != 3) {
            System.out.println("ERROR: illegal input; Valid Format: 'Pub Topic Message'");
            return;
        }

        String payload = tokens[1]+","+tokens[2];
        ResponseBase resp = SendTo( "127.0.0.1",brokerPort, "Pub", payload);
        if (resp.Ok()) {
            System.out.println(resp.data);
        } else {
            System.out.println("Error publishing message");
        }
    }

    private static int GetCurrentBroker() throws IOException {
        ResponseBase response = SendTo("127.0.0.1", 8800, "GetBroker", "");
        if (response.status.equals("OK")) {
            return Integer.parseInt(response.data);
        } else {
            //TODO
        }
        return 0;
    }

}
