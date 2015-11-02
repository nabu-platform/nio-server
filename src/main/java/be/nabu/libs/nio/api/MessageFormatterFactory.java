package be.nabu.libs.nio.api;

public interface MessageFormatterFactory<T> {
	public MessageFormatter<T> newMessageFormatter();
}
