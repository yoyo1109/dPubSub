package edu.scu.subscriber;

import edu.scu.utils.Connector;
import edu.scu.utils.Utils.ResponseBase;

import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

import static edu.scu.utils.Utils.SendTo;
import static java.lang.Thread.sleep;

public class Subscriber {

    // The latest broker address, should be updated periodically.
    private static int brokerPort = 0;
    private static int localPort = 1000;
    private static Socket brokerSocket;




    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Started Publisher..");
        if (args.length > 0) {
            localPort = Integer.parseInt(args[0]);
        }
        System.out.println("Started subscriber at local port " + localPort);


        // Get leader from FrontEnd Server.

        GetCurrentBrokerThread();
        System.out.println("Received current broker address: localhost:" + brokerPort);

        // Subscribe to topics.
        Scanner scanner = new Scanner(System.in);
        String publisherMsg = scanner.nextLine();



        // TODO:Waiting for updates;
        brokerSocket = new Socket("127.0.0.1", brokerPort);
        Sub(publisherMsg);

        Connector reader = new Connector(brokerSocket);
        while (true) {
            //
            String data = reader.readLine();
            if (!data.isEmpty()) {
                // TODO
                System.out.println(data);
            }
            sleep(1000);
        }
    }

    private static void Sub(String payload) throws IOException {
        ResponseBase resp = SendTo( brokerSocket, "Sub", payload);
        if (resp.Ok()) {
            System.out.println(resp.data);
        } else {
            System.out.println("Error publishing message");
        }
    }

    private static void GetCurrentBrokerThread(){
        new Thread( ()-> {
            while (true) {
                try {
                    brokerPort = GetCurrentBroker();
                    Thread.sleep(1000);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
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

    //TODO
    private static void UpdateSubMessage(){

    }
}
