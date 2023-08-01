package guru.bonacci.reactive.kafka;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class JsonProducer implements Closeable {

  private final KafkaTemplate<String, Output> template;;

  private static final String TOPIC = "json-topic";

  public String sendMessage(Output out) {
   template.send(TOPIC, out.getId(), out);
   log.info("sending {}", out.getId());
   return out.getId();
  }

  public String sendAndWaitMessage(Output out) throws InterruptedException, ExecutionException {
    template.send(TOPIC, out.getId(), out).get();
    log.info("sent {}", out.getId());
    return out.getId();
  }

  @Override
  public void close() {
    if (template != null)
    	template.destroy();
  }
}    