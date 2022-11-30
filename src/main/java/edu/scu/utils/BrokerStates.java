package edu.scu.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;


public class BrokerStates {

    public static void main(String[] args) {
        ConcurrentHashMap<Integer, TreeMap< Integer,String>> subscriberToTopic = new ConcurrentHashMap<>();
        subscriberToTopic.put(234,new TreeMap<>());
        subscriberToTopic.get(234).put(1111,"topic1");
        subscriberToTopic.get(234).put(1112,"topic2");
        subscriberToTopic.put(345,new TreeMap<>());
        subscriberToTopic.get(345).put(2222,"topic2");
        BrokerState brokerState = new BrokerState(subscriberToTopic);
        brokerState.Serialize();
        brokerState.Deserialize();
    }


    public static class BrokerState {
        private String serializedData = null;
//        private ConcurrentHashMap<String, TreeMap<LocalDateTime, String>> topicToMsgList = null;
        private ConcurrentHashMap<Integer, TreeMap< Integer,String>>  subscriberToTopic = null;
        private String jString;

        public BrokerState(String serializedData) {
            this.serializedData = serializedData;
        }

//        public BrokerState(ConcurrentHashMap<String, TreeMap<LocalDateTime, String>> topicToMsgList, ConcurrentHashMap<Socket, HashMap<String, LocalDateTime>> subscriberToTopic) {
//            this.subscriberToTopic = subscriberToTopic;
//            this.topicToMsgList = topicToMsgList;
//        }

        public BrokerState(ConcurrentHashMap<Integer, TreeMap<Integer, String>>  subscriberToTopic) {
            this.subscriberToTopic = subscriberToTopic;
//            this.topicToMsgList = topicToMsgList;
        }


        public String getSerializedData() {
            if (serializedData == null) {
                Serialize();
            }
            return serializedData;
        }

//        public ConcurrentHashMap<String, TreeMap<LocalDateTime, String>> getTopicToMsgList() {
//            if (topicToMsgList == null) {
//                Deserialize();
//            }
//            return topicToMsgList;
//        }

        public ConcurrentHashMap<Integer, TreeMap< Integer,String>>  getSubscriberToTopic() {
            if (subscriberToTopic == null) {
                Deserialize();
            }
            return subscriberToTopic;
        }

        // From data structure to string
        private String Serialize() {
//        Gson gson = new Gson();
            JsonObject json1 = new JsonObject();

            for (Integer key : subscriberToTopic.keySet()) {
                JsonObject jsonObject = new Gson().fromJson(String.valueOf(subscriberToTopic.get(key)), JsonObject.class);
                json1.add(key.toString(), jsonObject);
//            HashMap val = subscriberToTopic.get(key);
//
//            for(Object subkey: val.keySet()){
//                String topicToLastReadtime = subkey.toString()+val.get(subkey).toString();
//                JsonObject jsonObject = new Gson().fromJson(topicToLastReadtime,JsonObject.class);
//                json1.add(key.toString(),jsonObject);
//            }
            }
            jString = json1.toString();
            System.out.println(jString);
            System.out.println("subscriberToTopic " +subscriberToTopic);
            return jString;
        }

        // From string to data structure.
        private void Deserialize() {
            ConcurrentHashMap<Integer, TreeMap< Integer,String>> copyFromLeader = new ConcurrentHashMap<>();
            Gson g = new Gson();
            JsonObject jsonObject = g.fromJson(jString,JsonObject.class);
            System.out.println("jsonobject "+jsonObject);
            System.out.println("jstring "+jString);
            Set<String> keys = jsonObject.keySet();
            System.out.println(keys);

            for (String key:keys) {
                JsonObject vals  = (JsonObject) jsonObject.get(key);
                System.out.println("vals " + vals);
                String valStr = vals.toString().replace("{","").replace("}","");
                String [] array = valStr.toString().split(",");
                for (int i = 0; i < array.length; i++) {
                    String[] top = array[i].split(":");
                    if(!copyFromLeader.containsKey(top[0].substring(1,top[0].length()-1))){
                        copyFromLeader.put(Integer.parseInt(key),new TreeMap<>());
                    }
                    copyFromLeader.get(Integer.parseInt(key)).put(Integer.parseInt(top[0].substring(1,top[0].length()-1)),top[1].substring(1,top[1].length()-1));
                }
            }
            System.out.println(copyFromLeader);



//            for (copyFromLeader.Entry<String, LocalDateTime> set: val.entrySet()) {
//                String topic = set.getKey();
//                LocalDateTime tSub = set.getValue();
//                if (topicToMsgList.containsKey(topic)) {
//                    TreeMap<LocalDateTime, String> messageList = topicToMsgList.get(topic);
//                    // {timestamp -> message}
//                    for(Map.Entry<LocalDateTime, String> msgEntry: messageList.entrySet()) {
//                        String msg = msgEntry.getValue();
//                        LocalDateTime tMsg = msgEntry.getKey();
//                        if (tSub.isBefore(tMsg)) {
//                            System.out.println("Find message with in time range " + msg);
//                            rtn.add(topic + ": " + msg);
//                        }
//                    }
//                    // Update subscriber's last read time.
//                    set.setValue(LocalDateTime.now());
//                }
//            }
        }
    }
}
