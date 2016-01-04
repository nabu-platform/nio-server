package be.nabu.libs.nio.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.metrics.api.MetricTimer;
import be.nabu.libs.nio.api.MessageParser;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.containers.CountingReadableContainerImpl;
import be.nabu.utils.io.containers.PushbackContainerImpl;

public class RequestFramer<T> implements Runnable, Closeable {

	public static final String PARSE_TIME = "parseTime";
	public static final String REQUEST_SIZE = "requestSize";
	public static final String TRANSFER_RATE = "requestTransferRate";
	
	private static final int BUFFER_SIZE = 512000;
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	private PushbackContainerImpl<ByteBuffer> readable;
	private CountingReadableContainerImpl<ByteBuffer> counting;
	private MessageParser<T> framer;
	private MessagePipelineImpl<T, ?> pipeline;
	private MetricTimer timer;

	RequestFramer(MessagePipelineImpl<T, ?> pipeline, ReadableContainer<ByteBuffer> readable) {
		this.pipeline = pipeline;
		this.counting = new CountingReadableContainerImpl<ByteBuffer>(IOUtils.bufferReadable(readable, IOUtils.newByteBuffer(BUFFER_SIZE, true)));
		this.readable = new PushbackContainerImpl<ByteBuffer>(counting);
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
				// regardless of metrics, reset the counter
				counting.setReadTotal(0);
				MetricInstance metrics = pipeline.getServer().getMetrics();
				if (metrics != null) {
					timer = metrics.start(PARSE_TIME + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocket()));
				}
			}
			framer.push(readable);
			if (framer.isClosed()) {
				closeConnection = true;
			}
			if (framer.isDone()) {
				if (timer != null) {
					long timed = timer.stop();
					String userId = NIOServerImpl.getUserId(pipeline.getSourceContext().getSocket());
					long readSize = counting.getReadTotal() - readable.getBufferSize();
					long transferRate = readSize / timer.getTimeUnit().convert(timed, TimeUnit.MILLISECONDS);
					timer.getMetrics().log(REQUEST_SIZE + ":" + userId, readSize);
					timer.getMetrics().log(TRANSFER_RATE + ":" + userId, transferRate);
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
