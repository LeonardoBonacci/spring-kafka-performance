package guru.bonacci.reactive;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Input {
	
	@NotNull
	@Size(message = "First name must be between 2 and 25 characters", min = 2, max = 25)
	private String foo;
	
	@NotNull
	private Integer goo;
	
	@NotNull
	private Double bar;
	
	@NotNull
	private Boolean baz;
}
