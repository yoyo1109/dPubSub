package edu.scu.subscriber;

import edu.scu.utils.Utils.ResponseBase;

import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

import static edu.scu.utils.Utils.SendTo;
import static java.lang.Thread.sleep;

public class Subscriber {

    // The latest broker address, should be updated periodically.
    private static int brokerPort = 0;
    private static int subscribeID = 1000;
    private static Socket brokerSocket;




    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Started Publisher..");
        if (args.length > 0) {
            subscribeID = Integer.parseInt(args[0]);
        }
        System.out.println("Started subscriber at local port " + subscribeID);

        // Get leader from FrontEnd Server.
        GetCurrentBrokerThread();
//        brokerPort = GetCurrentBroker();
        sleep(3000);
        System.out.println("Received current broker address: localhost:" + brokerPort);

        // Subscribe to topics.
        Scanner scanner = new Scanner(System.in);
        String publisherMsg = scanner.nextLine();

        // TODO:Waiting for updates;
        brokerSocket = new Socket("127.0.0.1", brokerPort);
        Sub(publisherMsg);
        StartUpdateSubMessageThread();
    }

    // data: id + multiple topic
    private static void Sub(String payload) throws IOException {
        payload = subscribeID + "," + payload;
        ResponseBase resp = SendTo( brokerSocket, "Sub", payload);
        if (resp.Ok()) {
            System.out.println(resp.data);
        } else {
            System.out.println("Error subscribe.");
        }
    }

    private static void GetCurrentBrokerThread(){
        new Thread( ()-> {
            while (true) {
                try {
                    sleep(1000);
                    brokerPort = GetCurrentBroker();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private static int GetCurrentBroker()  {
        ResponseBase response = null;
        try {
            response = SendTo("127.0.0.1", 8800, "GetBroker", "");
            if (response.status.equals("OK")) {
                return Integer.parseInt(response.data);
            } else {
                System.out.println("GetCurrentBroker: FrontEnd returned error: " + response.status);
            }
        } catch (IOException e) {
            System.out.println("Failed to get current leader broker from FrontEnd Server");
        }
        return 0;
    }

    private static void StartUpdateSubMessageThread(){
        new Thread( ()-> {
            System.out.println("Started polling messages on all subscribed topics");
            while (true) {
                try {
                    sleep(3000);
                    String payload = subscribeID + "";
                    ResponseBase resp = SendTo("127.0.0.1",brokerPort, "GetUpdate", payload);
                    if (resp.Ok()) {
                        if (resp.data.isEmpty()) continue;
                        System.out.println(resp.data);
                    } else {
                        System.out.println("Error update subscriber message");
                    }
                } catch ( InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    System.out.println("Failed to get updates on subscribed topics, will retry later");
                }
            }
        }).start();
    }
}
