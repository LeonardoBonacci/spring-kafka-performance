package guru.bonacci.reactive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import guru.bonacci.reactive.kafka.AvroProducer;
import guru.bonacci.reactive.kafka.JsonProducer;
import guru.bonacci.reactive.kafka.ProtobufProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RestController
@SpringBootApplication
@RequiredArgsConstructor
public class ReactiveApp {

	public static void main(String[] args) {
		SpringApplication.run(ReactiveApp.class, args);
	}

	private final JsonProducer kafkaJsonClient;
	private final AvroProducer kafkaAvroClient;
	private final ProtobufProducer kafkaProtoClient;

	@PostMapping("/json/tx")
	public Flux<String> postTxRequest(@RequestBody @Valid Input in) {
		return kafkaJsonClient.sendTxMessage(in);
	}

	@PostMapping("/json")
	public Flux<String> postJsonRequest(@RequestBody @Valid Input in) {
		return kafkaJsonClient.sendMessages(in);
	}

	@PostMapping("/avro")
	public Flux<String> postAvroRequest(@RequestBody @Valid Input in) {
		return kafkaAvroClient.sendMessages(in);
	}

	@PostMapping("/proto")
	public Flux<String> postProtoRequest(@RequestBody @Valid Input in) {
		return kafkaProtoClient.sendMessages(in);
	}
}
