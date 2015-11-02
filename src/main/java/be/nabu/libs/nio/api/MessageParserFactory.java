package be.nabu.libs.nio.api;

public interface MessageParserFactory<T> {
	public MessageParser<T> newMessageParser();
}
