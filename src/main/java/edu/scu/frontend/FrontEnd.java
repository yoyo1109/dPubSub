package edu.scu.frontend;

import edu.scu.utils.Connector;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FrontEnd {
    private static int port = 8800;
    private static int currBrokerLeader = -1;
    // Key: broker id
    // Value:
    private static ConcurrentHashMap<Integer,
            LocalDateTime> brokerMap;

//    private static volatile boolean isBrokerAlive;
    private static Duration failureTimeout = Duration.ofSeconds(5);

    public static void main(String[] args) throws IOException {
        brokerMap = new ConcurrentHashMap<>();


        System.out.println("Starting FrontEnd Server at port " + port);
        ServerSocket serverSocket = new ServerSocket(port);

        while (true) {
            Socket socket = serverSocket.accept();

            Connector connector = new Connector(socket);
            String action = connector.readLine();
            String data = connector.readLine();

            String status = "OK";

            String dResult = "";
            String[] tokens = data.split(",");

            switch (action) {
                case "HeartBeat":
                    // TODO
                    System.out.println("Received HeartBeat: " + data);
                    // update the Broker's local date time
                    brokerMap.put(Integer.parseInt(tokens[0]), LocalDateTime.parse(tokens[1]));
                    break;
                case "GetBroker":
                    // figure out a leader broker and return.
                    int leader = leaderElection();
                    if(leader < 0){
                        status = "Error: no active broker";
                        System.out.println(status);
                    }
                    dResult = leader + "";
                    System.out.println("Responding GetBroker with " + dResult);


                    break;
                case "GetBrokerList":
                    // Return a list of all active broker.
                    break;

                case "GetLeaderBroker":
                    // Return port of leader broker.
                    int leaderBroker = leaderElection();
                    if(leaderBroker < 0){
                        status = "Error: no active broker";
                    }
                    dResult = leaderBroker + "";
                    System.out.println("Responding GetLeaderBroker with " + dResult);
                    break;


                case "RegisterBroker":
                    // TODO
                    dResult = "Registered successfully!";
                    // This will overwrite existing registration.
                    int remotePort = Integer.parseInt(data);
                    brokerMap.put(remotePort, LocalDateTime.now());
                    System.out.println("Registered broker " + remotePort + "; total brokers: " + brokerMap.size());
                    break;
                default:
                    status = "Not supported";
                    break;
            }

            String result = dResult + "";
            connector.writeLine(status);
            connector.writeLine(result);
        }
    }

    public static Integer leaderElection(){
        if( currBrokerLeader > 0 && isBrokerAlive(currBrokerLeader)){
            return currBrokerLeader;
        }
        List<Integer> brokers = detectActiveBrokers();
        if(brokers.isEmpty()){
            System.out.println("No active broker!");
            return -1;
        }
        return brokers.get(0);
        //
    }
    
    public static List<Integer> detectActiveBrokers(){
        List<Integer> availableBroker = new ArrayList<>();

        for(Map.Entry<Integer, LocalDateTime> set : brokerMap.entrySet()){
            if(isBrokerAlive(set.getKey())){
                availableBroker.add(set.getKey());
            }
        }
        return availableBroker;
    }

    public static boolean isBrokerAlive(Integer brokerId){
        LocalDateTime lastUpdateTime = brokerMap.get(brokerId);
        LocalDateTime timeout = lastUpdateTime.plus(failureTimeout);
        LocalDateTime now = LocalDateTime.now();
        return now.isBefore(timeout);
    }
    
}
