package be.nabu.libs.nio.api;

/**
 * A streaming message parser will parse something initially, then create a result prematurely
 * This result is already sent back to somewhere which is assumed to continue reading from the incoming stream of bytes
 * Only once the stream is fully read do we consider the message fully handled
 */
public interface StreamingMessageParser<T> extends MessageParser<T> {
	public boolean isStreamed();
}
