package edu.scu.broker;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import edu.scu.utils.Connector;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static edu.scu.utils.Utils.ResponseBase;
import static edu.scu.utils.Utils.SendTo;
import static java.lang.Thread.sleep;

public class Broker {
    private static int localPort = 6000;
    // A well-known address.
    private static int frontEndPort = 8800;
    private static int heartBeatIntervalSec = 1;
    private static int brokerSyncIntervalSec = 2;

    private static int currentLeaderBroker;

    // Key: topic
    // Value: MsgList {timestamp -> message}
    private static ConcurrentHashMap<String, TreeMap<LocalDateTime, String>> topicToMsgList;

    // Key: subscriber id (socket)
    // Value: The time of message read last time by this subscriber.
    //        {topic -> last read time}
    private static ConcurrentHashMap<Socket, HashMap<String, LocalDateTime>> subscriberToTopic;

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
        if(currentLeaderBroker < 0){
            System.out.println("Broker " + localPort + " couldn't find a leader broker to update");
        } else if (currentLeaderBroker == localPort) {
            System.out.println("Broker " + localPort + " is the leader broker");
        }else{
            SyncWithLeaderBroker(currentLeaderBroker);
        }


        // if self is not leader, synchronize data with leader broker


        // Start the HeartBeat thread.
        StartHeartBeatThread();
        StartMessageUpdateThread();
        StartBrokerSyncThread();

        // Start broker server.
        System.out.println("Starting FrontEnd Server at port " + localPort);
        ServerSocket serverSocket = new ServerSocket(localPort);

        while (true) {
            Socket socket = serverSocket.accept();
            Connector connector = new Connector(socket);
            System.out.println("Accepted connection from + " + connector.getRemotePort());

            String action = connector.readLine();
            String data = connector.readLine();
            System.out.println("Initial list size: " + topicToMsgList.size());
            String status = "OK";
            String dResult = "";
            String[] tokens = data.split(",");
            switch (action) {
                case "Pub": {
                    System.out.println("Received Pub: " + data);
                    String topic = tokens[0];
                    String message = tokens[1];
                    LocalDateTime timestamp = LocalDateTime.now();
                    if (!topicToMsgList.containsKey(topic)) {
                        System.out.println("Create new Message list");
                        topicToMsgList.put(topic, new TreeMap<>());
                    }
                    topicToMsgList.get(topic).put(timestamp, message);
                    System.out.println("Current Messages: " + topicToMsgList.get(topic).toString());
                    // Sync this particular message to all peer brokers;
                    break;
                }
                case "Sub": {
                    System.out.println("Received Sub: " + data);
                    // Save this socket and use it to send messages back;
                    System.out.println("Topic size: " + tokens.length);
                    for (String topic : tokens) {
                        if (!subscriberToTopic.containsKey(socket)) {
                            System.out.println("Create new Topic list for new socket " + socket.toString());
                            subscriberToTopic.put(socket, new HashMap<>());
                        }
                        subscriberToTopic.get(socket).put(topic, LocalDateTime.now());
                        System.out.println("Sub Size: " + subscriberToTopic.get(socket));
                    }
                    break;
                }
                case "Sync":
                    //TODO: sync status between brokers.
                    System.out.println("Received Sync: " + data);


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

    private static void StartMessageUpdateThread() {
        new Thread( ()-> {
            while (true) {
                try {
                    subscriberToTopic.forEach((subSocket, val) -> {
                        ArrayList<String> rtn = new ArrayList<>();
                        for (Map.Entry<String, LocalDateTime> set: val.entrySet()) {
                            String topic = set.getKey();
                            LocalDateTime tSub = set.getValue();
                            if (topicToMsgList.containsKey(topic)) {
                                TreeMap<LocalDateTime, String> messageList = topicToMsgList.get(topic);
                                // {timestamp -> message}
                                for(Map.Entry<LocalDateTime, String> msgEntry: messageList.entrySet()) {
                                    String msg = msgEntry.getValue();
                                    LocalDateTime tMsg = msgEntry.getKey();
                                    if (tSub.isBefore(tMsg)) {
                                        System.out.println("Find message with in time range " + msg);
                                        rtn.add(topic + ": " + msg);
                                    }
                                }
                                // Update subscriber's last read time.
                                set.setValue(LocalDateTime.now());
                            }
                        }
                        if (!rtn.isEmpty()) {
                            System.out.println("Updating subscriber " + subSocket.toString());
                            Connector connector1 = new Connector(subSocket);
                            connector1.writeLine(String.join("\n", rtn));
                        }
                    });
                    sleep(500);
                } catch ( InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }).start();
    }

    private static void StartHeartBeatThread() {
        new Thread( ()-> {
            while (true) {
                // REMOVE ME!
                BrokerState brokerState = new BrokerState(topicToMsgList, subscriberToTopic);
                brokerState.getSerializedData();

                try {
                    SendHeartBeat();
                    Thread.sleep(1000 * heartBeatIntervalSec);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }).start();
    }

    private static void StartBrokerSyncThread(){
        new Thread( ()-> {
            System.out.println("Broker sync thread started.");
            while (true) {
                try {
                    currentLeaderBroker = GetLeaderBroker();
                    if(currentLeaderBroker > 0 && currentLeaderBroker != localPort){
                        SyncWithLeaderBroker(currentLeaderBroker);
                    }
                    sleep(1000 * brokerSyncIntervalSec);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }).start();
    }


    private static void SendHeartBeat() throws IOException {
        String payload = localPort + ","+ LocalDateTime.now().toString();
        ResponseBase resp = SendTo("127.0.0.1", frontEndPort, "HeartBeat", payload);
        if (resp.Ok()) {
            System.out.println("HeartBeat! " + payload);
        }
        else {
            System.out.println(resp.status);
        }
    }
    private static void Register() throws IOException {
        ResponseBase resp = SendTo("127.0.0.1", frontEndPort, "RegisterBroker", Integer.toString(localPort));
        if (resp.Ok()) {
            System.out.println(resp.data);
        }
        else {
            System.out.println(resp.status);
        }
    }

    private static Integer GetLeaderBroker() throws IOException {

        ResponseBase resp = SendTo("127.0.0.1", frontEndPort, "GetLeaderBroker", Integer.toString(localPort));
        if (resp.Ok()) {
            return currentLeaderBroker = Integer.parseInt(resp.data);
        }
        else {
            System.out.println(resp.status);
            return -1;
        }
    }


    public static void SyncWithLeaderBroker(Integer brokerPort) throws IOException {
        ResponseBase resp = SendTo("127.0.0.1", brokerPort, "Sync", "");
        if (resp.Ok()) {
            System.out.println(resp.data);
            //TODO save the resp.data
        }
        else {
            System.out.println(resp.status);
        }
    }


};

class BrokerState {
    private String serializedData = null;
    private ConcurrentHashMap<String, TreeMap<LocalDateTime, String>> topicToMsgList = null;
    private ConcurrentHashMap<Socket, HashMap<String, LocalDateTime>> subscriberToTopic = null;


    public BrokerState(String serializedData) {
        this.serializedData = serializedData;
    }

    public BrokerState(ConcurrentHashMap<String, TreeMap<LocalDateTime, String>> topicToMsgList,ConcurrentHashMap<Socket, HashMap<String, LocalDateTime>> subscriberToTopic){
        this.subscriberToTopic = subscriberToTopic;
        this.topicToMsgList = topicToMsgList;
    }


    public String getSerializedData() {
        if (serializedData == null) {
            Serialize();
        }
        return serializedData;
    }

    public ConcurrentHashMap<String, TreeMap<LocalDateTime, String>> getTopicToMsgList() {
        if (topicToMsgList == null) {
            Deserialize();
        }
        return topicToMsgList;
    }

    public ConcurrentHashMap<Socket, HashMap<String, LocalDateTime>> getSubscriberToTopic() {
        if (subscriberToTopic == null) {
            Deserialize();
        }
        return subscriberToTopic;
    }

    // From data structure to string
    private void Serialize() {
//        Gson gson = new Gson();
        JsonObject json1 = new JsonObject();

        for(Socket key: subscriberToTopic.keySet()){
            JsonObject jsonObject = new Gson().fromJson(String.valueOf(subscriberToTopic.get(key)),JsonObject.class);
            json1.add(key.toString(),jsonObject);
//            HashMap val = subscriberToTopic.get(key);
//
//            for(Object subkey: val.keySet()){
//                String topicToLastReadtime = subkey.toString()+val.get(subkey).toString();
//                JsonObject jsonObject = new Gson().fromJson(topicToLastReadtime,JsonObject.class);
//                json1.add(key.toString(),jsonObject);
//            }
        }
        System.out.println(json1);
    }

    // From string to data structure.
    private void Deserialize() {
        // From

    }
}
