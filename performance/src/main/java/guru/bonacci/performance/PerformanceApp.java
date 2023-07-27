package guru.bonacci.performance;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@SpringBootApplication
public class PerformanceApp {

	public static void main(String[] args) {
		SpringApplication.run(PerformanceApp.class, args);
	}
	
	 @PostMapping("/foo")
   public Mono<Output> postRequest(@RequestBody Input in) {
		 long now = System.currentTimeMillis();
		 var output = new Output(in.getFoo(), in.getGoo(), in.getBar(), in.getBaz(), now);
		 log.info(output.toString());
     return Mono.just(output);
   }

}
