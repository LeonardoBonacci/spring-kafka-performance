package guru.bonacci.reactive.kafka;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import guru.bonacci.reactive.Input;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

@Slf4j
@Component
public class AvroProducer implements Closeable {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "avro-topic";

    private final KafkaSender<String, GenericRecord> sender;

    public AvroProducer() {
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
      props.put(ProducerConfig.CLIENT_ID_CONFIG, "reactive-avro-producer-"+Uuid.randomUuid().toString().substring(0, 5));
      props.put(ProducerConfig.ACKS_CONFIG, "all");
      props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
      props.put("schema.registry.url", "http://localhost:8081");
      SenderOptions<String, GenericRecord> senderOptions = SenderOptions.create(props);

      sender = KafkaSender.create(senderOptions);
    }
    
    public Flux<String> sendMessages(Input... ins) {
       return sender.<String>send(Flux.just(ins).map(RecordHelper::toAvro)
      		 								.map(output -> SenderRecord.create(new ProducerRecord<>(TOPIC, (String)output.get("id"), output), 
      		 											(String)output.get("id"))))
      		 		.doOnError(e -> log.error("Send failed", e))
              .doOnNext(r -> log.info("Send completed {}", r.correlationMetadata()))
              .doOnCancel(() -> close())
              .map(result -> result.correlationMetadata());
    }

    
    @UtilityClass
    static class RecordHelper {
    	
    	final Schema schema = SchemaBuilder.record("foorecord")
  			  .namespace("guru.bonacci.avro")
  			  .fields()
  			  	.requiredString("id")
  			  	.requiredString("foo")
  			  	.requiredInt("goo")
  			  	.requiredDouble("bar")
  			  	.requiredBoolean("baz")
  			  .endRecord();

    	
    	GenericRecord toAvro(Input in) {
    		var id = Uuid.randomUuid().toString();

				GenericRecord out = new GenericData.Record(schema);
				out.put("id", id);
				out.put("foo", in.getFoo());
				out.put("goo", in.getGoo());
				out.put("bar", in.getBar());
				out.put("baz", in.getBaz());
				return out;
    	}
    }
    
    @Override
    public void close() {
      if (sender != null)
        sender.close();
    }
}    