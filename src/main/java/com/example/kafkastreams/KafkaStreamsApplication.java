package com.example.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

//@SpringBootApplication
//@EnableKafkaStreams
//@EnableKafka
public class KafkaStreamsApplication {

	public static void main1(String[] args) {
		SpringApplication.run(KafkaStreamsApplication.class, args);
	}

	public static void main(String[] args) throws Exception {
		int count = 20;
		KafkaStreamsApplication consumer = new KafkaStreamsApplication(BOOTSTRAP_SERVERS);
		Disposable disposable = consumer.consumeMessages(TOPIC);
		disposable.dispose();
	}

	private static final String BOOTSTRAP_SERVERS = "10.0.1.207:9092";
	private static final String TOPIC = "taxilla-events";

	private final ReceiverOptions<Integer, String> receiverOptions;
	private final SimpleDateFormat dateFormat;
	private final Scheduler scheduler;

	public KafkaStreamsApplication(String bootstrapServers) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		receiverOptions = ReceiverOptions.create(props);
		dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");
		this.scheduler = Schedulers.newSingle("sample", true);
	}

	public Disposable consumeMessages(String topic) {
		ReceiverOptions<Integer, String> options = receiverOptions
				.commitBatchSize(100)
				.commitInterval(Duration.ofSeconds(30))
				.subscription(Collections.singleton(topic));
		Flux<ReceiverRecord<Integer, String>> kafkaFlux = KafkaReceiver.create(options).receive();
		return kafkaFlux.publishOn(scheduler).concatMap(m -> storeInDB(m.value())
				.thenEmpty(m.receiverOffset().commit()))
				.subscribe();
	}

	public Mono<Void> storeInDB(String person) {
		System.out.println("Successfully processed person with id {} from Kafka" + person);
		return Mono.empty();
	}

	// kafka streams
/*
	@Component
	class Processor {

		@Autowired
		public void process(StreamsBuilder builder) {
			Serde<String> stringSer = Serdes.String();
			builder.stream("taxilla-events", Consumed.with(stringSer, stringSer))
					.groupByKey()
		}
	}
*/

/*	@Component
	class Consumer {
		@KafkaListener(id = "myconsumer", topics = {"taxilla-events"})
		public void consumer(ConsumerRecord<String, String> myRecord) {
			System.out.println("offset = " + myRecord.offset()+ ": partition" + myRecord.partition());
		}
	}*/
}
