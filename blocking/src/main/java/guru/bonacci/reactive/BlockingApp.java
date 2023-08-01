package guru.bonacci.reactive;

import java.util.concurrent.ExecutionException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import guru.bonacci.reactive.kafka.JsonProducer;
import guru.bonacci.reactive.kafka.Output;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

@RestController
@SpringBootApplication
@RequiredArgsConstructor
public class BlockingApp {

	public static void main(String[] args) {
		SpringApplication.run(BlockingApp.class, args);
	}

	
	private final JsonProducer kafkaClient;
	
	@PostMapping("/json")
	public String postRequest(@RequestBody @Valid Input in) {
		return kafkaClient.sendMessage(Output.from(in));
	}
	
	@PostMapping("/block")
	public String postRequestBlock(@RequestBody @Valid Input in) throws InterruptedException, ExecutionException {
		return kafkaClient.sendAndWaitMessage(Output.from(in));
	}
}
