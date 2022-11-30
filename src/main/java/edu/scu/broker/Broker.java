package edu.scu.broker;

import edu.scu.utils.BrokerStates;
import edu.scu.utils.Connector;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static edu.scu.utils.Utils.ResponseBase;
import static edu.scu.utils.Utils.SendTo;

public class Broker {
    private static int localPort = 6000;
    // A well-known address.
    private static int frontEndPort = 8800;
    private static int heartBeatIntervalSec = 1;
    private static int brokerSyncIntervalSec = 2;

    private static BrokerStates brokerStates;

    private static DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    private static int currentLeaderBroker;

    // Key: topic
    // Value: MsgList {timestamp -> message}
    private static ConcurrentHashMap<String, HashMap<String, String>> topicToMsgList;

    // Key: subscriber id (port)
    // Value: The time of message read last time by this subscriber.
    //        {topic -> last read time}
    private static ConcurrentHashMap<String, HashMap<String, String>> subscriberToTopic;

    public static void main(String[] args) throws IOException {
        topicToMsgList = new ConcurrentHashMap<>();
        subscriberToTopic = new ConcurrentHashMap<>();

        if (args.length > 0) {
            localPort = Integer.parseInt(args[0]);
        }
        System.out.println("Started Broker at local port " + localPort);
        // Register this broker to FrontEnd Server.
        Register();

        // Get current leader broker to update the data
        currentLeaderBroker = GetLeaderBroker();
        if (currentLeaderBroker < 0) {
            System.out.println("Broker " + localPort + " couldn't find a leader broker to update");
        } else if (currentLeaderBroker == localPort) {
            System.out.println("Broker " + localPort + " is the leader broker");
        } else {
            // if self is not leader, synchronize data with leader broke
//            StartBrokerSyncThread();
        }


        // Start the HeartBeat thread.
        StartHeartBeatThread();
//        StartMessageUpdateThread();


        // Start broker server.
        System.out.println("Starting FrontEnd Server at port " + localPort);
        ServerSocket serverSocket = new ServerSocket(localPort);

        while (true) {
            Socket socket = serverSocket.accept();
            Connector connector = new Connector(socket);
            System.out.println("Accepted connection from  " + connector.getRemotePort());

            String action = connector.readLine();
            String data = connector.readLine();
            String status = "OK";
            String dResult = "";
            String[] tokens = data.split(",");
            switch (action) {
                case "Pub": {
                    System.out.println("Received Pub request: " + data);
                    String topic = tokens[0];
                    String message = tokens[1];
                    if (!topicToMsgList.containsKey(topic)) {
                        System.out.println("Create new Message list");
                        topicToMsgList.put(topic, new HashMap<>());
                    }
                    topicToMsgList.get(topic).put(LocalDateTime.now().toString(), message);
                    System.out.println("Current Messages: " + topicToMsgList.get(topic).toString());
                    break;
                }
                case "Sub": {
                    System.out.println("Received Sub request: " + data);
                    // Save this socket and use it to send messages back;
                    System.out.println("Topic size: " + tokens.length);
                    String subId = tokens[0];
                    for (int i = 1; i < tokens.length; i++) {
                        if (!subscriberToTopic.containsKey(subId)) {
                            System.out.println("Create new Topic list for new subscriber " + subId);
                            subscriberToTopic.put(subId, new HashMap<>());
                        }
                        subscriberToTopic.get(subId).put(tokens[i], LocalDateTime.now().toString());
                        System.out.printf("subscriber(%s) subscribed topics:  %s%n", subId, subscriberToTopic.get(subId));
                    }

                    break;
                }
                case "GetUpdate":
                    //update sub message
                    System.out.println("Received GetUpdate request: " + data);
                    dResult = SendMessageToSubscriber(/*subId=*/tokens[0], socket);
                    break;

                case "Sync":
                    //TODO: sync status between brokers.
//                    System.out.println("Received Sync request: " + data);
//                    brokerStates = new BrokerStates();




                    break;
                default:
                    status = "Not supported";
                    break;
            }
            String result = dResult + "";
            connector.writeLine(status);
            connector.writeLine(result);

            // Update subscribers.
        }
    }

    //TODO

    private static LocalDateTime ToLocalDateTime(String input) {
        return LocalDateTime.parse(input, timeFormatter);
    }

    private static String SendMessageToSubscriber(String subId, Socket socket) {
        System.out.println("Responding to subscriber " + subId);
        ArrayList<String> rtn = new ArrayList<>();
        HashMap<String, String> topicMap = subscriberToTopic.get(subId);
        for (Map.Entry<String, String> set : topicMap.entrySet()) {
            String topic = set.getKey();
            LocalDateTime tSub = ToLocalDateTime(set.getValue());
            if (topicToMsgList.containsKey(topic)) {
                HashMap<String, String> messageList = topicToMsgList.get(topic);
                if (messageList.isEmpty()) continue;
                // {timestamp -> message}
                for (Map.Entry<String, String> msgEntry : messageList.entrySet()) {
                    String msg = msgEntry.getValue();
                    LocalDateTime tMsg =  ToLocalDateTime(msgEntry.getKey());
                    if (tSub.isBefore(tMsg)) {
                        System.out.println("Find message with in time range " + msg);
                        rtn.add(topic + ": " + msg);
                    }
                }
                // Update subscriber's last read time.
                topicMap.put(topic, LocalDateTime.now().toString());
            }
        }

        if (!rtn.isEmpty()) {
            String payload = String.join("\n", rtn);
            System.out.println("Updating subscriber (" + subId + ") with " + payload);
            return payload;
        } else {
            System.out.println("No update for " + subId + " right now");
            return "";
        }

    }

    private static void StartHeartBeatThread() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000 * heartBeatIntervalSec);
                    SendHeartBeat();
                } catch (IOException | InterruptedException e) {
                    System.out.println("ERROR: sending heart beat to FrontEnd, will try againt later.");
                    e.printStackTrace();
                }
            }
        }).start();
    }

//    public static void SyncWithLeaderBroker(Integer brokerPort) throws IOException {
//        ResponseBase resp = SendTo("127.0.0.1", brokerPort, "Sync", "");
//        if (resp.Ok()) {
//            System.out.println(resp.data);
//            //TODO save the resp.data
//        } else {
//            System.out.println(resp.status);
//        }
//    }
//    private static void StartBrokerSyncThread() {
//        new Thread(() -> {
//            System.out.println("Broker sync thread started.");
//            while (true) {
//                System.out.println("Trying to sync with broker");
//                try {
//                    SyncWithLeaderBroker(currentLeaderBroker);
//                    Thread.sleep(10000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
////                try {
////                    currentLeaderBroker = GetLeaderBroker();
////                    if (currentLeaderBroker > 0 && currentLeaderBroker != localPort) {
////                        SyncWithLeaderBroker(currentLeaderBroker);
////                    }
////                    sleep(1000 * brokerSyncIntervalSec);
////                } catch (IOException | InterruptedException e) {
////                    throw new RuntimeException(e);
////                }
//
//            }
//        }).start();
//    }


    private static void SendHeartBeat() throws IOException {
        String payload = localPort + "," + LocalDateTime.now().toString();
        ResponseBase resp = SendTo("127.0.0.1", frontEndPort, "HeartBeat", payload);
        if (resp.Ok()) {
//            System.out.println("HeartBeat! " + payload);
        } else {
            System.out.println(resp.status);
        }
    }

    private static void Register() throws IOException {
        ResponseBase resp = SendTo("127.0.0.1", frontEndPort, "RegisterBroker", Integer.toString(localPort));
        if (resp.Ok()) {
            System.out.println(resp.data);
        } else {
            System.out.println(resp.status);
        }
    }

    private static Integer GetLeaderBroker() throws IOException {

        ResponseBase resp = SendTo("127.0.0.1", frontEndPort, "GetLeaderBroker", Integer.toString(localPort));
        if (resp.Ok()) {
            return currentLeaderBroker = Integer.parseInt(resp.data);
        } else {
            System.out.println(resp.status);
            return -1;
        }
    }
};


//
//class BrokerState {
//    private String serializedData = null;
//    private ConcurrentHashMap<String, HashMap<String, String>> topicToMsgList = null;
//    private ConcurrentHashMap<String, HashMap<String, LocalDateTime>> subscriberToTopic = null;
//
//
//    public BrokerState(String serializedData) {
//        this.serializedData = serializedData;
//    }
//
//    public BrokerState(ConcurrentHashMap<String, HashMap<String, String>> topicToMsgList, ConcurrentHashMap<String, HashMap<String, LocalDateTime>> subscriberToTopic) {
//        this.subscriberToTopic = subscriberToTopic;
//        this.topicToMsgList = topicToMsgList;
//    }
//
//
//    public String getSerializedData() {
//        if (serializedData == null) {
//            Serialize();
//        }
//        return serializedData;
//    }
//
//    public ConcurrentHashMap<String, HashMap<String, String>> getTopicToMsgList() {
//        if (topicToMsgList == null) {
//            Deserialize();
//        }
//        return topicToMsgList;
//    }
//
//    public ConcurrentHashMap<String, HashMap<String, LocalDateTime>> getSubscriberToTopic() {
//        if (subscriberToTopic == null) {
//            Deserialize();
//        }
//        return subscriberToTopic;
//    }
//
//    // From data structure to string
//    private void Serialize() {
////        Gson gson = new Gson();
//        JsonObject json1 = new JsonObject();
//
//        for (String key : subscriberToTopic.keySet()) {
//            JsonObject jsonObject = new Gson().fromJson(String.valueOf(subscriberToTopic.get(key)), JsonObject.class);
//            json1.add(key.toString(), jsonObject);
////            HashMap val = subscriberToTopic.get(key);
////
////            for(Object subkey: val.keySet()){
////                String topicToLastReadtime = subkey.toString()+val.get(subkey).toString();
////                JsonObject jsonObject = new Gson().fromJson(topicToLastReadtime,JsonObject.class);
////                json1.add(key.toString(),jsonObject);
////            }
//        }
//        System.out.println(json1);
//    }
//
//    // From string to data structure.
//    private void Deserialize() {
//        // From
//
//    }
//}
