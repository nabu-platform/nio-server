package be.nabu.libs.nio.api;

public interface StandardizedMessagePipeline<T, R> extends MessagePipeline<T, R> {
	public MessageParserFactory<T> getRequestParserFactory();
	public MessageFormatterFactory<R> getResponseFormatterFactory();
	public MessageProcessorFactory<T, R> getMessageProcessorFactory();
	public KeepAliveDecider<R> getKeepAliveDecider();
	public ExceptionFormatter<T, R> getExceptionFormatter();
}
