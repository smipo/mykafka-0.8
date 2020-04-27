package examples;

public class KafkaProducerDemo {

    public static void main(String[] args) throws Exception {
        Producer producerThread = new Producer(KafkaProperties.topic);
        producerThread.start();
    }
}
