package guru.bonacci.reactive.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import io.confluent.kafka.serializers.KafkaJsonSerializer;

@Configuration
public class KafkaProducerConfig {

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";

  @Bean
  public ProducerFactory<String, Output> producerFactory() {
      Map<String, Object> configProps = new HashMap<>();
      configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
      configProps.put(ProducerConfig.CLIENT_ID_CONFIG, "blocking-json-producer-"+Uuid.randomUuid().toString().substring(0, 5));
      configProps.put(ProducerConfig.ACKS_CONFIG, "all");
      configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  
      configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
      return new DefaultKafkaProducerFactory<>(configProps);
  }

  @Bean
  public KafkaTemplate<String, Output> kafkaTemplate() {
      return new KafkaTemplate<>(producerFactory());
  }
}