package edu.scu.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class BrokerStates {

    // Testing only, remove.
    public static void main(String[] args) {
        ConcurrentHashMap<String, HashMap<String, String>> subscriberToTopic = new ConcurrentHashMap<>();
        subscriberToTopic.put("234", new HashMap<>());
        subscriberToTopic.get("234").put("1111", LocalDateTime.now().toString());
        subscriberToTopic.get("234").put("1112", LocalDateTime.now().toString());
        subscriberToTopic.put("345", new HashMap<>());
        subscriberToTopic.get("345").put("2222", LocalDateTime.now().toString());
        System.out.println("INPUT: " + subscriberToTopic);
        BrokerStates brokerState = new BrokerStates(subscriberToTopic, subscriberToTopic);
        brokerState.getSerializedData();
        System.out.println("OUTPUT: " + brokerState.getSubscriberToTopic());
    }

    private String serializedData = null;
    private ConcurrentHashMap<String, HashMap<String, String>> subscriberToTopic = null;
    private ConcurrentHashMap<String, HashMap<String, String>> topicToMsgList = null;
    public BrokerStates(String serializedData) {
        this.serializedData = serializedData;
    }

    public BrokerStates(ConcurrentHashMap<String, HashMap<String, String>> subscriberToTopic,
                        ConcurrentHashMap<String, HashMap<String, String>> topicToMsgList) {
        this.subscriberToTopic = subscriberToTopic;
        this.topicToMsgList = topicToMsgList;
    }


    public String getSerializedData() {
        if (serializedData == null) {
            String strSubscriberToTopic = null;
            String strTopicToMsg = null;
            if (serializedData == null) {
                strSubscriberToTopic = SerializeMap(subscriberToTopic);
                strTopicToMsg = SerializeMap(topicToMsgList);
            }
            Gson g = new Gson();
            JsonObject root = new JsonObject();
            root.add("subscriberToTopic", g.toJsonTree(strSubscriberToTopic));
            root.add("topicToMsgList", g.toJsonTree(strTopicToMsg));
            serializedData = root.toString();
        }
        return serializedData;
    }

    public ConcurrentHashMap<String, HashMap<String, String>> getSubscriberToTopic() {
        if (subscriberToTopic == null) {
            JsonObject jsonObject = new Gson().fromJson(serializedData, JsonObject.class);
            subscriberToTopic = DeserializeToMap(jsonObject.get("subscriberToTopic").getAsString());
        }
        return subscriberToTopic;
    }

    public ConcurrentHashMap<String, HashMap<String, String>> getTopicToMsgList() {
        if (topicToMsgList == null) {
            JsonObject jsonObject = new Gson().fromJson(serializedData, JsonObject.class);
            topicToMsgList = DeserializeToMap(jsonObject.get("topicToMsgList").getAsString());
        }
        return topicToMsgList;
    }


    // Convert a given 2D map to Json string.
    private String SerializeMap(ConcurrentHashMap<String, HashMap<String, String>> map) {
        Gson gson = new Gson();
        JsonObject root = new JsonObject();
        map.forEach((key, value) -> {
            root.add(String.valueOf(key), gson.toJsonTree(value));
        });
        return root.toString();
    }

    // Convert a given Json string to 2D map {String: {String: String}}
    private ConcurrentHashMap<String, HashMap<String, String>> DeserializeToMap(String jString) {
        ConcurrentHashMap<String, HashMap<String, String>> result = new ConcurrentHashMap<>();
        JsonObject jsonObject = new Gson().fromJson(jString, JsonObject.class);
        Set<String> keySet = jsonObject.keySet();
        for (String key : keySet) {
            JsonObject innerMap = jsonObject.get(key).getAsJsonObject();
            result.put(key, new HashMap<>());
            for (String innerKey : innerMap.keySet()) {
                String innerVal = innerMap.get(innerKey).getAsString();
                result.get(key).put(innerKey, innerVal);
            }
        }
        return result;
    }
}
