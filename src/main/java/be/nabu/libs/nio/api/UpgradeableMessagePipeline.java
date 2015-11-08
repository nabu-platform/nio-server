package be.nabu.libs.nio.api;

public interface UpgradeableMessagePipeline<T, R> extends MessagePipeline<T, R> {
	public <Q, S> MessagePipeline<Q, S> upgrade(MessageParserFactory<Q> requestParserFactory, MessageFormatterFactory<S> responseFormatterFactory, MessageProcessorFactory<Q, S> messageProcessorFactory, KeepAliveDecider<S> keepAliveDecider, ExceptionFormatter<Q, S> exceptionFormatter);
}
