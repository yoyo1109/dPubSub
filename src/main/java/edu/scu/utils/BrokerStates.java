package edu.scu.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;


public class BrokerStates {

    public static void main(String[] args) {
        ConcurrentHashMap<String, HashMap< String, String>> subscriberToTopic = new ConcurrentHashMap<>();
        subscriberToTopic.put("234",new HashMap<>());
        subscriberToTopic.get("234").put("1111",LocalDateTime.now().toString());
        subscriberToTopic.get("234").put("1112",LocalDateTime.now().toString());
        subscriberToTopic.put("345",new HashMap<>());
        subscriberToTopic.get("345").put("2222",LocalDateTime.now().toString());
        System.out.println("INPUT: " + subscriberToTopic);
        BrokerState brokerState = new BrokerState(subscriberToTopic);
        brokerState.Serialize();
        brokerState.Deserialize();
        System.out.println("OUTPUT: " + brokerState.getSubscriberToTopic());
    }


    public static class BrokerState {
        private String serializedData = null;
//        private ConcurrentHashMap<String, TreeMap<LocalDateTime, String>> topicToMsgList = null;
        private ConcurrentHashMap<String, HashMap< String, String>> subscriberToTopic = null;
        private String jString;

        public BrokerState(String serializedData) {
            this.serializedData = serializedData;
        }

//        public BrokerState(ConcurrentHashMap<String, TreeMap<LocalDateTime, String>> topicToMsgList, ConcurrentHashMap<Socket, HashMap<String, LocalDateTime>> subscriberToTopic) {
//            this.subscriberToTopic = subscriberToTopic;
//            this.topicToMsgList = topicToMsgList;
//        }

        public BrokerState(ConcurrentHashMap<String, HashMap< String, String>> subscriberToTopic) {
            this.subscriberToTopic = subscriberToTopic;
//            this.topicToMsgList = topicToMsgList;
        }


        public String getSerializedData() {
            if (serializedData == null) {
                serializedData = Serialize();
            }
            return serializedData;
        }

//        public ConcurrentHashMap<String, TreeMap<LocalDateTime, String>> getTopicToMsgList() {
//            if (topicToMsgList == null) {
//                Deserialize();
//            }
//            return topicToMsgList;
//        }

        public ConcurrentHashMap<String, HashMap< String, String>> getSubscriberToTopic() {
            if (subscriberToTopic == null) {
                Deserialize();
            }
            return subscriberToTopic;
        }

        // From data structure to string
        private String Serialize() {
        Gson gson = new Gson();
            JsonObject root = new JsonObject();

            subscriberToTopic.forEach((key, value) -> {
                root.add(String.valueOf(key), gson.toJsonTree(value));
            });
            jString = root.toString();
            System.out.println(jString);
            System.out.println("subscriberToTopic " +subscriberToTopic);
            return jString;
        }

        // From string to data structure.
        private void Deserialize() {
            Gson g = new Gson();
            subscriberToTopic = g.fromJson(jString, ConcurrentHashMap.class);
            System.out.println("Converted " + subscriberToTopic.toString());
        }
    }
}
