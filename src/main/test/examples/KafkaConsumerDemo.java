package examples;

public class KafkaConsumerDemo {

    public static void main(String[] args) throws Exception {
        Consumer consumerThread = new Consumer(KafkaProperties.topic);
        consumerThread.start();

    }
}
