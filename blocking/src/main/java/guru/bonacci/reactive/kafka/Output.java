package guru.bonacci.reactive.kafka;

import org.apache.kafka.common.Uuid;

import guru.bonacci.reactive.Input;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Output {
	
	private String id;
	private String foo;
	private Integer goo;
	private Double bar;
	private Boolean baz;
	
	public static Output from(Input in) {
		var id = Uuid.randomUuid().toString();
		return Output.builder()
						.id(id)
						.foo(in.getFoo())
						.goo(in.getGoo())
						.bar(in.getBar())
						.baz(in.getBaz())
						.build();
	}
}
