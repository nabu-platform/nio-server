package be.nabu.libs.nio.api;

public interface MessageProcessorFactory<T, R> {
	public MessageProcessor<T, R> newProcessor(T request);
}
