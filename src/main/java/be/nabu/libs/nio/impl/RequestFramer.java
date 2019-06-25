package be.nabu.libs.nio.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.metrics.api.MetricTimer;
import be.nabu.libs.nio.api.MessageParser;
import be.nabu.libs.nio.api.events.ConnectionEvent;
import be.nabu.libs.nio.impl.events.ConnectionEventImpl;
import be.nabu.utils.cep.api.EventSeverity;
import be.nabu.utils.cep.impl.CEPUtils;
import be.nabu.utils.cep.impl.NetworkedComplexEventImpl;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.containers.CountingReadableContainerImpl;
import be.nabu.utils.io.containers.EOFReadableContainer;
import be.nabu.utils.io.containers.PushbackContainerImpl;

public class RequestFramer<T> implements Runnable, Closeable {

	public static final String PARSE_TIME = "parseTime";
	public static final String REQUEST_SIZE = "requestSize";
	public static final String TRANSFER_RATE = "requestTransferRate";
	
	private static final int BUFFER_SIZE = 16384;
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	private PushbackContainerImpl<ByteBuffer> readable;
	private CountingReadableContainerImpl<ByteBuffer> counting;
	private MessageParser<T> framer;
	private MessagePipelineImpl<T, ?> pipeline;
	private MetricTimer timer;
	private Date started;
	private EOFReadableContainer<ByteBuffer> eof;
	private volatile boolean closeWhenDone = false;

	RequestFramer(MessagePipelineImpl<T, ?> pipeline, ReadableContainer<ByteBuffer> readable) {
		this.pipeline = pipeline;
		this.eof = new EOFReadableContainer<ByteBuffer>(readable);
		this.counting = new CountingReadableContainerImpl<ByteBuffer>(IOUtils.bufferReadable(eof, IOUtils.newByteBuffer(BUFFER_SIZE, true)));
		this.readable = new PushbackContainerImpl<ByteBuffer>(counting);
	}
	
	@Override
	public void close() throws IOException {
		// it might already have been closed by the remote party, but now we are also of the opinion that everything has been processed
		pipeline.getServer().getDispatcher().fire(new ConnectionEventImpl(pipeline.getServer(), pipeline, ConnectionEvent.ConnectionState.EMPTY), this);
		pipeline.close();
	}

	@Override
	public void run() {
		// we don't put the context anymore to avoid GDPR issues with logging addresses
		//pipeline.putMDCContext();
		T request = null;
		boolean closeConnection = false;
		long originalBufferSize = 0, newBufferSize = 0, originalCount = 0, newCount = 0;
		try {
			if (framer == null) {
				framer = pipeline.getRequestParserFactory().newMessageParser();
				// mark when we started reading for timeout purposes
				started = new Date();
				// regardless of metrics, reset the counter
				counting.setReadTotal(0);
				MetricInstance metrics = pipeline.getServer().getMetrics();
				if (metrics != null) {
					timer = metrics.start(PARSE_TIME + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocketAddress()));
				}
			}
			originalBufferSize = readable.getBufferSize();
			originalCount = counting.getReadTotal();
			framer.push(readable);
			newBufferSize = readable.getBufferSize();
			newCount = counting.getReadTotal();
			
			if (framer.isClosed()) {
				closeConnection = true;
			}
			// if we encountered an end of file and nothing was read, close the connection
			else if (eof.isEOF() && counting.getReadTotal() == 0) {
				closeConnection = true;
			}
			else if (closeWhenDone) {
				closeConnection = true;
			}
			if (framer.isDone()) {
				if (timer != null) {
					long timed = timer.stop();
					String userId = NIOServerImpl.getUserId(pipeline.getSourceContext().getSocketAddress());
					long readSize = counting.getReadTotal() - readable.getBufferSize();
					long transferRate = readSize / Math.max(1, timer.getTimeUnit().convert(timed, TimeUnit.MILLISECONDS));
					timer.getMetrics().log(REQUEST_SIZE + ":" + userId, readSize);
					timer.getMetrics().log(TRANSFER_RATE + ":" + userId, transferRate);
					timer = null;
					started = null;
				}
				request = framer.getMessage();
				framer = null;
			}
			else if (started != null && pipeline.getReadTimeout() > 0 && started.getTime() < new Date().getTime() - pipeline.getReadTimeout()) {
				logger.warn("Read timed out, started at {} with a timeout value of {}", started, pipeline.getReadTimeout());
				
				NetworkedComplexEventImpl event = CEPUtils.newServerNetworkEvent(getClass(), "request-parse-timeout", pipeline.getSourceContext().getSocketAddress());
				event.setStarted(started);
				event.setStopped(new Date());
				event.setSeverity(EventSeverity.WARNING);
				pipeline.getServer().fire(event, pipeline.getServer());
				
				pipeline.close();
			}
			if (request != null) {
				logger.trace("Parsed request {}", request);
				pipeline.getRequestQueue().add(request);
			}
		}
		catch (IOException e) {
			closeConnection = true;
			logger.debug("Could not process incoming data", e);
		}
		catch (Exception e) {
			closeConnection = true;
			logger.error("Could not process incoming data", e);
			pipeline.getServer().fire(CEPUtils.newServerNetworkEvent(getClass(), "request-parse", pipeline.getSourceContext().getSocketAddress(), "Could not process incoming data", e), pipeline.getServer());
		}
		if (closeConnection) {
			try {
				close();
			}
			catch (IOException e) {
				logger.error("Failed to close connection", e);
				pipeline.getServer().fire(CEPUtils.newServerNetworkEvent(getClass(), "connection-close", pipeline.getSourceContext().getSocketAddress(), "Failed to close connection", e), pipeline.getServer());
			}
		}
		// if the buffer sizes don't match, _something_ changed, either there is new data or data disappeared to form a message
		// if the buffer size remains the same and > 0, there is a partial message (or garbage) in there and we don't want to keep scheduling reads
		else if (pipeline.rescheduleRead() || (originalCount != newCount) || (originalBufferSize != newBufferSize)) {
			pipeline.read(true);
		}
	}

	public long remainingData() {
		return readable.getBufferSize();
	}
	
	public void drain() {
		closeWhenDone = true;
	}
}
