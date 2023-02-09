# dPubSub
publisher and subscriber
In order to implement the entire Publisher and Subscriber system, our team designed 5 individual packages and classes which are Frontend, 
Broker, Publisher, Subscriber and Utils (helper function). 
- The Publisher class is more like a sensor in IoT which inputs topics and messages. 
- The Subscriber class has a little difference from Publish, after inputting the subscribe topic, it will call the Broker to update the topic message and show up the new message. 
- Frontend class is a middleware between the Publisher, Subscriber and Broker. 
Frontend will periodically check if the broker is alive and do leader election for Brokers. 
Publisher, Subscriber, Broker they will connect to the Frontend server when they are started and get the current lead broker, then they will communicate with the lead broker. 
- Broker is the center for information processing, it will process the topic message and pull the update message to the subscriber. And Brokers communicate with the lead broker and exchange the data.
