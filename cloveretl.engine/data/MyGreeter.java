import org.jetel.component.Greeter;

//used by TransformFactoryTest

public class NewGreeter extends Greeter {
	public String getGreeting(String message) {
		return "New external hello " + message;
	}
}