package guru.bonacci.reactive.kafka;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import guru.bonacci.reactive.Input;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

@Slf4j
@Component
public class ProtobufProducer implements Closeable {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "protobuf-topic";

    private final KafkaSender<String, ProtoOutput.Output> sender;

    public ProtobufProducer() {
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
      props.put(ProducerConfig.CLIENT_ID_CONFIG, "reactive-proto-producer-"+Uuid.randomUuid().toString().substring(0, 5));
      props.put(ProducerConfig.ACKS_CONFIG, "all");
      props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
      props.put("schema.registry.url", "http://localhost:8081");
      SenderOptions<String, ProtoOutput.Output> senderOptions = SenderOptions.create(props);

      sender = KafkaSender.create(senderOptions);
    }
    
    public Flux<String> sendMessages(Input... ins) {
       return sender.<String>send(Flux.just(ins).map(RecordHelper::toProto)
      		 								.map(output -> SenderRecord.create(new ProducerRecord<>(TOPIC, output.getId(), output), 
      		 											output.getId())))
      		 		.doOnError(e -> log.error("Send failed", e))
              .doOnNext(r -> log.info("Send completed {}", r.correlationMetadata()))
              .doOnCancel(() -> close())
              .map(result -> result.correlationMetadata());
    }

    
    @UtilityClass
    static class RecordHelper {
    	
    	ProtoOutput.Output toProto(Input in) {
    		var id = Uuid.randomUuid().toString();

    		return ProtoOutput.Output.newBuilder()
    			.setId(id)
					.setFoo(in.getFoo())
					.setGoo(in.getGoo())
					.setBar(in.getBar())
					.setBaz(in.getBaz())
				.build();
    	}
    }
    
    @Override
    public void close() {
      if (sender != null)
        sender.close();
    }
}    