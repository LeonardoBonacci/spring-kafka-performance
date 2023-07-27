package guru.bonacci.reactive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import guru.bonacci.reactive.kafka.JsonProducer;
import guru.bonacci.reactive.kafka.Output;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
@RestController
@SpringBootApplication
@RequiredArgsConstructor
public class ReactiveApp {

	public static void main(String[] args) {
		SpringApplication.run(ReactiveApp.class, args);
	}

	
	private final JsonProducer kafkaClient;
	
	 @PostMapping("/json/tx")
   public Flux<String> postTxRequest(@RequestBody Input in) throws InterruptedException {
		 return kafkaClient.sendTxMessage(Output.from(in));
   }
	 
	 @PostMapping("/json")
   public Flux<String> postRequest(@RequestBody Input in) throws InterruptedException {
		 return kafkaClient.sendMessage(Output.from(in));
   }

}
