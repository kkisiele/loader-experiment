package training;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TxLogProducer {
    /*
    records: 995000
    messages: 1054501
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);
        final String topic = "txlog_1m";

        for (long i = 1; i <= 1_000_000; i++) {
            if(i % 10000 == 0) {
                System.out.println(i);
            }
            producer.send(new ProducerRecord<>(topic, "" + i, "I;" + i + ";foo" + i)).get();

            if(i >= 5_000 && i <= 500_000 && i % 10 == 0) {
                long c = i - 4_999;
                producer.send(new ProducerRecord<>(topic, "" + c, "U;" + c + ";bar" + c)).get();
            }
            if(i > 500_000 && i % 100 == 0) {
                long c = i - 500_000;
                producer.send(new ProducerRecord<>(topic, "" + c, "D;" + c + ";bar" + c)).get();
            }
        }
        producer.flush();
        producer.close();
    }


}
