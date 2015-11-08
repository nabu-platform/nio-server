package be.nabu.libs.nio.impl;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.nio.api.MessageParser;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.PushbackContainer;
import be.nabu.utils.io.api.ReadableContainer;

public class RequestFramer<T> implements Runnable, Closeable {

	private static final int BUFFER_SIZE = 512000;
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	private PushbackContainer<ByteBuffer> readable;
	private MessageParser<T> framer;

	private MessagePipelineImpl<T, ?> pipeline;

	RequestFramer(MessagePipelineImpl<T, ?> pipeline, ReadableContainer<ByteBuffer> readable) {
		this.pipeline = pipeline;
		this.readable = IOUtils.pushback(IOUtils.bufferReadable(readable, IOUtils.newByteBuffer(BUFFER_SIZE, true)));
	}
	
	@Override
	public void close() throws IOException {
		pipeline.close();
	}

	@Override
	public void run() {
		T request = null;
		boolean closeConnection = false;
		try {
			if (framer == null) {
				framer = pipeline.getRequestParserFactory().newMessageParser();
			}
			framer.push(readable);
			if (framer.isDone()) {
				request = framer.getMessage();
				framer = null;
			}
			else if (framer.isClosed()) {
				closeConnection = true;
			}
			if (request != null) {
				logger.trace("Parsed request {}", request);
				pipeline.getRequestQueue().add(request);
			}
		}
		catch (Exception e) {
			closeConnection = true;
			logger.error("Could not process incoming data", e);
		}
		if (closeConnection) {
			try {
				close();
			}
			catch (IOException e) {
				logger.error("Failed to close connection", e);
			}
		}
	}

}
