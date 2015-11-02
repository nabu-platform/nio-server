package be.nabu.libs.nio.api;

import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;

public interface MessageFormatter<T> {
	public ReadableContainer<ByteBuffer> format(T message);
}
