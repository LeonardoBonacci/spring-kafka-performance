package guru.bonacci.reactive.kafka;

import java.io.Closeable;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class JsonProducer implements Closeable {

  private final KafkaTemplate<String, Output> template;;

  private static final String TOPIC = "json-topic";

  public String sendMessage(Output out) {
   template.send(TOPIC, out.getId(), out);
   return out.getId();
  }

  @Override
  public void close() {
    if (template != null)
    	template.destroy();
  }
}    