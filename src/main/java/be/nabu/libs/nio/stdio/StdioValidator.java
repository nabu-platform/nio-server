package be.nabu.libs.nio.stdio;

public class StdioValidator {
	private long maxMessageLength;

	
	public StdioValidator(long maxMessageLength) {
		this.maxMessageLength = maxMessageLength;
	}

	public long getMaxMessageLength() {
		return maxMessageLength;
	}

}
