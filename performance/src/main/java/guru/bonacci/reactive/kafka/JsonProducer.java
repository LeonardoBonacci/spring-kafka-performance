package guru.bonacci.reactive.kafka;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import io.confluent.kafka.serializers.KafkaJsonSerializer;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

@Slf4j
@Component
public class JsonProducer implements Closeable {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "json-topic";
    private static final String TOPIC_TX = "json-tx-topic";

    private final KafkaSender<String, Output> txSender;
    private final KafkaSender<String, Output> sender;

    public JsonProducer() {
      Map<String, Object> txProps = new HashMap<>();
      txProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
      txProps.put(ProducerConfig.CLIENT_ID_CONFIG, "reactive-tx-json-producer-"+Uuid.randomUuid().toString().substring(0, 5));
      txProps.put(ProducerConfig.ACKS_CONFIG, "all");
      txProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "reactive-json-tx-"+Uuid.randomUuid().toString().substring(0, 5));  
      txProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      txProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
      SenderOptions<String, Output> txSenderOptions = SenderOptions.create(txProps);

      txSender = KafkaSender.create(txSenderOptions);

      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
      props.put(ProducerConfig.CLIENT_ID_CONFIG, "reactive-json-producer-"+Uuid.randomUuid().toString().substring(0, 5));
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
      SenderOptions<String, Output> senderOptions = SenderOptions.create(props);

      sender = KafkaSender.create(senderOptions);
    }

    public Flux<String> sendTxMessage(Output... out) {
        Flux<Output> srcFlux = Flux.just(out);
        return txSender.<String>
                 sendTransactionally(srcFlux.map(p -> kafkaRecord(p, TOPIC_TX)))
                .concatMap(r -> r)
                .doOnNext(r -> log.info("Sent tx record successfully {}", r))
                .doOnError(e -> log.error("Send tx failed, terminating.", e))
                .doOnCancel(() -> close())
                .map(result -> result.correlationMetadata());
    }
    
    private Mono<SenderRecord<String, Output, String>> kafkaRecord(Output out, String topic) {
        return Mono.just(SenderRecord.create(new ProducerRecord<>(topic, out.getId(), out), out.getId()));
    }
    
    
    public Flux<String> sendMessage(Output... out) {
       return sender.<String>send(Flux.just(out)
      		 								.map(output -> SenderRecord.create(new ProducerRecord<>(TOPIC, output.getId(), output), 
		 																		 output.getId())))
      		 		.doOnError(e -> log.error("Send failed", e))
              .doOnNext(r -> log.info("Send completed {}", r.correlationMetadata()))
              .doOnCancel(() -> close())
              .map(result -> result.correlationMetadata());
    }

    @Override
    public void close() {
      if (sender != null)
        sender.close();
    }
}    