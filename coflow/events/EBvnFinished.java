package coflow.events;

public class EBvnFinished extends Event {

	@Override
	public void run() {
		// states has been updated
		backfillingBvnSlice();
		transmit();		
	}

}
