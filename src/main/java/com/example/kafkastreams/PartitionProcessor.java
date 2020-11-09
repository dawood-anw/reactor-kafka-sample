package com.example.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.time.Duration;
import java.util.*;

public class PartitionProcessor extends AbstractScenario {
    private final String topic;

    public PartitionProcessor(String bootstrapServers, String topic) {
        super(bootstrapServers);
        this.topic = topic;
    }

    public Flux<?> flux() {
        Scheduler scheduler = Schedulers.newBoundedElastic(60, Integer.MAX_VALUE, "sample", 60, true);
        return KafkaReceiver.create(receiverOptions(Collections.singleton(topic)).commitInterval(Duration.ZERO))
                .receive()
                .groupBy(m -> m.receiverOffset().topicPartition())
                .flatMap(partitionFlux -> partitionFlux.publishOn(scheduler)
                        .map(r -> processRecord(partitionFlux.key(), r))
                        .sample(Duration.ofMillis(5000))
                        .concatMap(offset -> offset.commit()))
                .doOnCancel(() -> close());
    }

    public ReceiverOffset processRecord(TopicPartition topicPartition,
                                        ReceiverRecord<String, String> message) {
        System.out.println(String.format("Processing record %s from partition %d in thread %s",
                message.value(), topicPartition, Thread.currentThread().getName()));
        return message.receiverOffset();
    }

    public static void main(String[] args) throws InterruptedException {
        PartitionProcessor partitionProcessor = new PartitionProcessor("10.0.1.207:9092", "taxilla-events");
        partitionProcessor.runScenario();
    }
}

abstract class AbstractScenario {
    String bootstrapServers = "";
    String groupId = "sample-group";
    KafkaSender<String, String> sender;
    List<Disposable> disposables = new ArrayList<>();

    AbstractScenario(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
    public abstract Flux<?> flux();

    public void runScenario() throws InterruptedException {
        flux().blockLast();
        close();
    }

    public void close() {
        if (sender != null)
            sender.close();
        for (Disposable disposable : disposables)
            disposable.dispose();
    }

    public SenderOptions<String, String> senderOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return SenderOptions.create(props);
    }

    public KafkaSender<String, String> sender(SenderOptions<String, String> senderOptions) {
        sender = KafkaSender.create(senderOptions);
        return sender;
    }

    public ReceiverOptions<String, String> receiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return ReceiverOptions.<String, String>create(props);
    }

    public ReceiverOptions<String, String> receiverOptions(Collection<String> topics) {
        return receiverOptions()
                .addAssignListener(p -> System.out.println(
                        String.format("Group %s partitions assigned %s", groupId, p)))
                .addRevokeListener(p -> String.format("Group %s partitions assigned %s", groupId, p))
                .subscription(topics);
    }

}