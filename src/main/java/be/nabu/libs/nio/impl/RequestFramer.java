package be.nabu.libs.nio.impl;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.metrics.api.MetricTimer;
import be.nabu.libs.nio.api.MessageParser;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.PushbackContainer;
import be.nabu.utils.io.api.ReadableContainer;

public class RequestFramer<T> implements Runnable, Closeable {

	public static final String TOTAL_PARSE_TIME = "totalParseTime";
	public static final String USER_PARSE_TIME = "userParseTime";
	
	private static final int BUFFER_SIZE = 512000;
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	private PushbackContainer<ByteBuffer> readable;
	private MessageParser<T> framer;
	private MessagePipelineImpl<T, ?> pipeline;
	private MetricTimer timer;

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
				MetricInstance metrics = pipeline.getServer().getMetrics();
				if (metrics != null) {
					timer = metrics.start(TOTAL_PARSE_TIME);
				}
			}
			framer.push(readable);
			if (framer.isClosed()) {
				closeConnection = true;
			}
			if (framer.isDone()) {
				if (timer != null) {
					timer.getMetrics().increment(USER_PARSE_TIME + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocket()), timer.stop());
					timer = null;
				}
				request = framer.getMessage();
				framer = null;
			}
			if (request != null) {
				logger.trace("Parsed request {}", request);
				pipeline.getRequestQueue().add(request);
			}
		}
		catch (IOException e) {
			closeConnection = true;
			logger.warn("Could not process incoming data", e);
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
