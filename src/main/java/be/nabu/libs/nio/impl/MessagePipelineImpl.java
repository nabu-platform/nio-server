package be.nabu.libs.nio.impl;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import be.nabu.libs.nio.api.ExceptionFormatter;
import be.nabu.libs.nio.api.KeepAliveDecider;
import be.nabu.libs.nio.api.MessageFormatterFactory;
import be.nabu.libs.nio.api.MessageParser;
import be.nabu.libs.nio.api.MessageParserFactory;
import be.nabu.libs.nio.api.MessagePipeline;
import be.nabu.libs.nio.api.MessageProcessorFactory;
import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.PipelineState;
import be.nabu.libs.nio.api.PipelineWithMetaData;
import be.nabu.libs.nio.api.PipelineWithTimeout;
import be.nabu.libs.nio.api.SecurityContext;
import be.nabu.libs.nio.api.SourceContext;
import be.nabu.libs.nio.api.StreamingMessageParser;
import be.nabu.libs.nio.api.UpgradeableMessagePipeline;
import be.nabu.libs.nio.api.events.ConnectionEvent;
import be.nabu.libs.nio.impl.events.ConnectionEventImpl;
import be.nabu.libs.nio.impl.udp.UDPChannel;
import be.nabu.utils.cep.impl.CEPUtils;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.SSLServerMode;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.Container;
import be.nabu.utils.io.containers.bytes.ByteChannelContainer;
import be.nabu.utils.io.containers.bytes.SSLSocketByteContainer;
import be.nabu.utils.io.containers.bytes.SocketByteContainer;

/**
 * Note to self: do _not_ buffer the output at this level
 * The response writer attempts a flush() at the very end but can't tell whether it was successful or not
 * This means the response writer will assume it succeeded (because in the past there was no buffer) and unregister the write interest
 * This can lead (in some cases) to missed data _if_ the buffer contained more content than the socket buffers could handle.
 */
public class MessagePipelineImpl<T, R> implements UpgradeableMessagePipeline<T, R>, PipelineWithMetaData, PipelineWithTimeout {
	
	private Date created = new Date();
	private Date lastRead, lastWritten, lastProcessed;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private Future<?> futureRead, futureWrite, futureProcess;
	private NIOServer server;
	private SelectionKey selectionKey;
	private Queue<T> requestQueue;
	private Queue<R> responseQueue;
	private Channel channel;
	private SSLSocketByteContainer sslContainer;
	private Container<ByteBuffer> container;
	
	private RequestFramer<T> requestFramer;
	private ResponseWriter<R> responseWriter;
	private RequestProcessor<T, R> requestProcessor;
	private MessageFormatterFactory<R> responseFormatterFactory;
	private MessageParserFactory<T> requestParserFactory;
	private MessageProcessorFactory<T, R> messageProcessorFactory;
	private KeepAliveDecider<R> keepAliveDecider;
	private ExceptionFormatter<T, R> exceptionFormatter;
	private long readTimeout, writeTimeout;
	private int requestLimit, responseLimit;
	private Map<String, Object> metaData = Collections.synchronizedMap(new HashMap<String, Object>());
	private Map<String, Object> context = Collections.synchronizedMap(new HashMap<String, Object>());
	
	private Long maxLifeTime, maxIdleTime;
	
	/**
	 * It is possible that this pipeline replaced an existing pipeline (for example after a connection upgrade or a live protocol switch)
	 * This is the parent pipeline that was handling the data before it was supplanted, it is mostly for allowing drainage
	 */
	private MessagePipelineImpl<?, ?> parentPipeline;
	
	private boolean closed;
	private boolean useSsl;
	private boolean debug = Boolean.parseBoolean(System.getProperty("nio.debug.pipeline", "false"));
	private SocketAddress remoteAddress, originalRemoteAddress;
	private Integer localPort;
	private volatile boolean shouldRescheduleRead = false;
	private boolean streamingMode;
	
	public MessagePipelineImpl(NIOServer server, SelectionKey selectionKey, MessageParserFactory<T> requestParserFactory, MessageFormatterFactory<R> responseFormatterFactory, MessageProcessorFactory<T, R> messageProcessorFactory, KeepAliveDecider<R> keepAliveDecider, ExceptionFormatter<T, R> exceptionFormatter) throws IOException {
		this(server, selectionKey, requestParserFactory, responseFormatterFactory, messageProcessorFactory, keepAliveDecider, exceptionFormatter, false, server.getSSLContext() != null, 0);
	}
	
	public MessagePipelineImpl(NIOServer server, SelectionKey selectionKey, MessageParserFactory<T> requestParserFactory, MessageFormatterFactory<R> responseFormatterFactory, MessageProcessorFactory<T, R> messageProcessorFactory, KeepAliveDecider<R> keepAliveDecider, ExceptionFormatter<T, R> exceptionFormatter, boolean isClient, boolean useSsl, int outputBufferSize) throws IOException {
		this.server = server;
		this.selectionKey = selectionKey;
		this.requestParserFactory = requestParserFactory;
		this.responseFormatterFactory = responseFormatterFactory;
		this.messageProcessorFactory = messageProcessorFactory;
		this.keepAliveDecider = keepAliveDecider;
		this.exceptionFormatter = exceptionFormatter;
		this.useSsl = useSsl;
		this.requestQueue = new PipelineRequestQueue<T>(this);
		this.responseQueue = new PipelineResponseQueue<R>(this);
		this.channel = selectionKey.channel();
		this.container = this.channel instanceof SocketChannel ? new SocketByteContainer((SocketChannel) channel) : new ByteChannelContainer<UDPChannel>((UDPChannel) this.channel);
		// perform SSL if required
		if (useSsl) {
			try {
				if (isClient) {
					sslContainer = new SSLSocketByteContainer(container, server.getSSLContext() == null ? SSLContext.getDefault() : server.getSSLContext(), true);
				}
				else {
					sslContainer = new SSLSocketByteContainer(container, server.getSSLContext() == null ? SSLContext.getDefault() : server.getSSLContext(), server.getSSLServerMode() == null ? SSLServerMode.NO_CLIENT_CERTIFICATES : server.getSSLServerMode());
				}
			}
			catch (NoSuchAlgorithmException e) {
				throw new RuntimeException(e);
			}
			container = sslContainer;
		}
		if (outputBufferSize > 0) {
			container = IOUtils.wrap(
				container,
				IOUtils.bufferWritable(container, IOUtils.newByteBuffer(outputBufferSize, true))
			);
		}
		if (debug || (server.getDebugger() != null && server.getDebugger().debug(this))) {
			container = ContainerDebugger.debug(container);
		}
		this.requestFramer = new RequestFramer<T>(this, container);
		this.responseWriter = new ResponseWriter<R>(this, container);
		this.requestProcessor = new RequestProcessor<T, R>(this);
		
		// attempt to deduce streaming mode (should be refactored!)
		MessageParser<T> newMessageParser = requestParserFactory.newMessageParser();
		streamingMode = newMessageParser instanceof StreamingMessageParser && ((StreamingMessageParser<?>) newMessageParser).isStreaming();
		newMessageParser = null;
		
		initMetadata();
	}
	
	private MessagePipelineImpl(MessagePipelineImpl<?, ?> parentPipeline, MessageParserFactory<T> requestParserFactory, MessageFormatterFactory<R> responseFormatterFactory, MessageProcessorFactory<T, R> messageProcessorFactory, KeepAliveDecider<R> keepAliveDecider, ExceptionFormatter<T, R> exceptionFormatter) {
		this.parentPipeline = parentPipeline;
		this.server = parentPipeline.getServer();
		this.selectionKey = parentPipeline.selectionKey;
		this.container = parentPipeline.container;
		this.requestParserFactory = requestParserFactory;
		this.responseFormatterFactory = responseFormatterFactory;
		this.messageProcessorFactory = messageProcessorFactory;
		this.keepAliveDecider = keepAliveDecider;
		this.exceptionFormatter = exceptionFormatter;
		this.requestQueue = new PipelineRequestQueue<T>(this);
		this.responseQueue = new PipelineResponseQueue<R>(this);
		this.channel = selectionKey.channel();
		this.requestFramer = new RequestFramer<T>(this, container);
		this.responseWriter = new ResponseWriter<R>(this, container);
		this.requestProcessor = new RequestProcessor<T, R>(this);
		this.streamingMode = parentPipeline.streamingMode;
		
		initMetadata();
	}
	
	public void drainOutput() {
		responseWriter.drain();
		// could be that you're already writing or that you need to start writing specifically to close it
		write();
	}
	
	public void drainInput() {
		requestFramer.drain();
		read();
	}
	
	private void initMetadata() {
		if (remoteAddress == null) {
			if (getChannel() instanceof SocketChannel) {
				remoteAddress = ((SocketChannel) getChannel()).socket().getRemoteSocketAddress();
			}
			else if (getChannel() instanceof UDPChannel) {
				remoteAddress = ((UDPChannel) getChannel()).getTarget();
			}
		}
		if (localPort == null) {
			if (getChannel() instanceof SocketChannel) {
				localPort = ((SocketChannel) getChannel()).socket().getLocalPort();
			}
			else if (getChannel() instanceof UDPChannel) {
				localPort = ((UDPChannel) getChannel()).getServer().getPort();
			}
		}
	}

	public void registerWriteInterest() {
		server.setWriteInterest(selectionKey, true);
	}
	
	public void unregisterWriteInterest() {
		server.setWriteInterest(selectionKey, false);
	}
	
	public void registerReadInterest() {
		server.setReadInterest(selectionKey, true);
	}
	
	public void unregisterReadInterest() {
		server.setReadInterest(selectionKey, false);
	}
	
	@Override
	public void read() {
		read(false);
	}

	void read(boolean force) {
		lastRead = new Date();
		if (force || futureRead == null || futureRead.isDone()) {
			synchronized(this) {
				if (force || futureRead == null || futureRead.isDone()) {
					futureRead = server.submitIOTask(requestFramer);
				}
				else {
					shouldRescheduleRead = true;
					registerDelayedReadInterest();
				}
			}
		}
		else {
			shouldRescheduleRead = true;
			registerDelayedReadInterest();
		}
	}
	// IMPORTANT: a performance test was severely impacted by this piece of code. printing out the amount of I/O tasks, 10.000 incoming calls (new connections) without this bit of code let to 41.000 I/O tasks
	// With this bit of code enabled, the same load generated 350.000 I/O tasks, this actually exploded the memory usage of the tasks waiting to be run, and we ran into GC death
	// TODO: refactor this code for streaming mode!!
	// there is an edge case where the reading stops, we want to make sure we always kick start it, preferably one time too many than one time too little, we just want to avoid a CPU-gone-mad scenario in non-streaming mode, the read interest is always on, so doesn't really matter
	private void registerDelayedReadInterest() {
		if (streamingMode) {
			server.submitIOTask(new Runnable() {
				public void run() {
					registerReadInterest();
				}
			});
		}
	}
	
	@Override
	public void write() {
		write(false);
	}

	void write(boolean force) {
		lastWritten = new Date();
		if (force || futureWrite == null || futureWrite.isDone()) {
			synchronized(this) {
				if (force || futureWrite == null || futureWrite.isDone()) {
					responseWriter.setWriting();
					futureWrite = server.submitIOTask(responseWriter);
				}
			}
		}
	}
	
	public void process() {
		process(false, true);
	}

	void process(boolean force, boolean submitChecker) {
		lastProcessed = new Date();
		boolean submitted = false;
		if (force || futureProcess == null || futureProcess.isDone()) {
			synchronized(this) {
				if (force || futureProcess == null || futureProcess.isDone()) {
					futureProcess = server.submitProcessTask(requestProcessor);
					submitted = true;
				}
			}
		}
		if (!submitted && submitChecker) {
			checkForWaiting();
		}
	}

	public void checkForWaiting() {
		// if requests are coming in _really_ fast (e.g. burst of websocket messages)
		// it is possible that between ending the processing while loop (and running the finally) during which the future is not yet done, something new is pushed to the queue
		// this queue push will check that something is running and it still is and not submit a new one
		// we need something that runs _after_ this future is resolved that can check if new messages were returned
		server.submitProcessTask(new Runnable() {
			@Override
			public void run() {
				if (!getRequestQueue().isEmpty()) {
					process(false, false);
				}
			}
		});
	}

	/**
	 * Tiny hack to reschedule reading of the request framer for udp messages
	 */
	boolean rescheduleRead() {
		if (shouldRescheduleRead) {
			shouldRescheduleRead = false;
			return true;
		}
		return getChannel() instanceof UDPChannel && (((UDPChannel) getChannel()).hasPending() || requestFramer.remainingData() > 0);
	}
	
	public void setRescheduleRead(boolean reschedule) {
		this.shouldRescheduleRead = reschedule;
	}
	
	/**
	 * If you simply "close" the channel only the input and output processing stops, not the message processing
	 * This method also stops the message processing
	 */
	public void die() {
		if (futureProcess != null && !futureProcess.isDone()) {
			futureProcess.cancel(true);
		}
		close();
	}
	
	@Override
	public void close() {
		try {
			closed = true;
			try {
				container.close();
			}
			catch (IOException e) {
				logger.debug("Failed to close the container", e);
			}
		
			if (futureRead != null && !futureRead.isDone()) {
				futureRead.cancel(true);
			}
			if (futureWrite != null && !futureWrite.isDone()) {
				futureWrite.cancel(true);
			}
		}
		finally {
			// remove it from the server map
			server.close(selectionKey);
		}
	}
	
	public Future<?> startHandshake() throws IOException {
		if (sslContainer == null) {
			throw new IllegalStateException("Not a secure container");
		}
		// we must start the handshake while blocking read() actions until it is done, otherwise the request framer might kick in on incoming handshake data
		futureRead = server.submitIOTask(new Runnable() {
			@Override
			public void run() {
				try {
					((SSLSocketByteContainer) sslContainer).shakeHands();
					Long handshakeDuration = ((SSLSocketByteContainer) sslContainer).getHandshakeDuration();
					if (handshakeDuration != null && getServer().getMetrics() != null) {
						getServer().getMetrics().duration("handshake", handshakeDuration, TimeUnit.MILLISECONDS);
					}
				}
				catch (IOException e) {
					logger.error("Could not finish handshake", e);
					getServer().fire(CEPUtils.newServerNetworkEvent(getClass(), "handshake", getSourceContext().getSocketAddress(), "Writing failed", e), getServer());					
					close();
				}
			}
		});
		return futureRead;
	}
	
	public void shakeHands() {
		if (sslContainer == null) {
			throw new IllegalStateException("Not a secure container");
		}
		try {
			((SSLSocketByteContainer) sslContainer).shakeHands();
			Long handshakeDuration = ((SSLSocketByteContainer) sslContainer).getHandshakeDuration();
			if (handshakeDuration != null && getServer().getMetrics() != null) {
				getServer().getMetrics().duration("handshake", handshakeDuration, TimeUnit.MILLISECONDS);
			}
		}
		catch (IOException e) {
			logger.error("Could not finish handshake", e);
			getServer().fire(CEPUtils.newServerNetworkEvent(getClass(), "handshake", getSourceContext().getSocketAddress(), "Writing failed", e), getServer());
			close();
			throw new RuntimeException(e);
		}
	}
	
	public void startTls(SSLContext context, SSLServerMode mode) throws SSLException {
		if (sslContainer == null) {
			sslContainer = new SSLSocketByteContainer(container, context, mode);
			sslContainer.setStartTls(true);
			container = sslContainer;
		}
		else {
			throw new IllegalStateException("Already encrypted");
		}
	}

	@Override
	public SecurityContext getSecurityContext() {
		return new SecurityContext() {
			@Override
			public SSLServerMode getSSLServerMode() {
				return server.getSSLServerMode();
			}
			@Override
			public SSLContext getSSLContext() {
				// if we have an ssl container (we should), use that context
				// we now allow the security context in the server to be updated dynamically but this is not (yet?) supported by pipelines who will continue to see the original context
				// hence the ssl container is more accurate
				if (sslContainer != null) {
					return sslContainer.getContext();
				}
				return server.getSSLContext();
			}
			@Override
			public Certificate[] getPeerCertificates() {
				return sslContainer == null ? null : sslContainer.getPeerCertificates();
			}
		};
	}

	@Override
	public Queue<T> getRequestQueue() {
		return requestQueue;
	}

	@Override
	public Queue<R> getResponseQueue() {
		return responseQueue;
	}

	Channel getChannel() {
		return channel;
	}

	@Override
	public MessageFormatterFactory<R> getResponseFormatterFactory() {
		return responseFormatterFactory;
	}
	
	@Override
	public MessageParserFactory<T> getRequestParserFactory() {
		return requestParserFactory;
	}
	
	@Override
	public MessageProcessorFactory<T, R> getMessageProcessorFactory() {
		return messageProcessorFactory;
	}
	
	@Override
	public KeepAliveDecider<R> getKeepAliveDecider() {
		return keepAliveDecider;
	}
	
	@Override
	public ExceptionFormatter<T, R> getExceptionFormatter() {
		return exceptionFormatter;
	}
	
	public boolean isClosed() {
		return closed;
	}
	@Override
	public NIOServer getServer() {
		return server;
	}

	@Override
	public SourceContext getSourceContext() {
		initMetadata();
		return new SourceContext() {
			@Override
			public SocketAddress getSocketAddress() {
				return remoteAddress;
			}
			@Override
			public Date getCreated() {
				return created;
			}
			@Override
			public int getLocalPort() {
				return localPort;
			}
		};
	}
	
	void putMDCContext() {
		MDC.put("socket", getChannel() instanceof SocketChannel ? ((SocketChannel) getChannel()).socket().toString() : ((UDPChannel) getChannel()).getTarget().toString());
	}
	
	public Future<?> getProcessFuture() {
		return futureProcess;
	}

	@Override
	public PipelineState getState() {
		// check if we have been explicitly closed
		if (closed) {
			return PipelineState.CLOSED;
		}
		// otherwise check if we are no longer actively registered on the server
		else if (!server.getPipelines().contains(this)) {
			// if we have no messages that are being processed, no messages left on the response queue and nothing left to be done by the response writer, we are closed
			if ((futureProcess == null || futureProcess.isDone()) && responseQueue.isEmpty() && responseWriter.isDone()) {
				closed = true;
				return PipelineState.CLOSED;
			}
			else {
				return PipelineState.DRAINING;
			}
		}
		// if we have no data pending, we are waiting for more
		else if (requestQueue.isEmpty() && responseQueue.isEmpty() && responseWriter.isDone() 
				&& (futureProcess == null || futureProcess.isDone())
				&& (futureRead == null || futureRead.isDone())
				&& (futureWrite == null || futureWrite.isDone())) {
			return PipelineState.WAITING;
		}
		// we are processing stuff!
		else {
			return PipelineState.RUNNING;
		}
	}

	@Override
	public <Q, S> MessagePipeline<Q, S> upgrade(MessageParserFactory<Q> requestParserFactory, MessageFormatterFactory<S> responseFormatterFactory, MessageProcessorFactory<Q, S> messageProcessorFactory, KeepAliveDecider<S> keepAliveDecider, ExceptionFormatter<Q, S> exceptionFormatter) {
		MessagePipelineImpl<Q, S> pipeline = new MessagePipelineImpl<Q, S>(this, requestParserFactory, responseFormatterFactory, messageProcessorFactory, keepAliveDecider, exceptionFormatter);
		// inherit whatever proxied address may have been set!
		pipeline.setRemoteAddress(remoteAddress);
		server.upgrade(selectionKey, pipeline);
		server.getDispatcher().fire(new ConnectionEventImpl(server, pipeline, ConnectionEvent.ConnectionState.UPGRADED), server);
		return pipeline;
	}

	MessagePipelineImpl<?, ?> getParentPipeline() {
		return parentPipeline;
	}

	ResponseWriter<R> getResponseWriter() {
		return responseWriter;
	}

	@Override
	public long getReadTimeout() {
		return readTimeout;
	}
	public void setReadTimeout(long readTimeout) {
		this.readTimeout = readTimeout;
	}

	@Override
	public long getWriteTimeout() {
		return writeTimeout;
	}
	public void setWriteTimeout(long writeTimeout) {
		this.writeTimeout = writeTimeout;
	}

	@Override
	public int getRequestLimit() {
		return requestLimit;
	}
	public void setRequestLimit(int requestLimit) {
		this.requestLimit = requestLimit;
	}

	@Override
	public int getResponseLimit() {
		return responseLimit;
	}
	public void setResponseLimit(int responseLimit) {
		this.responseLimit = responseLimit;
	}

	public Date getLastRead() {
		return lastRead;
	}
	public Date getLastWritten() {
		return lastWritten;
	}
	public Date getLastProcessed() {
		return lastProcessed;
	}

	@Override
	public Map<String, Object> getMetaData() {
		return metaData;
	}

	public boolean isUseSsl() {
		return useSsl;
	}

	public Container<ByteBuffer> getContainer() {
		return container;
	}

	@Override
	public Map<String, Object> getContext() {
		return context;
	}

	@Override
	public SelectionKey getSelectionKey() {
		return selectionKey;
	}

	@Override
	public void setRemoteAddress(SocketAddress address) {
		// we do keep track of the original
		if (originalRemoteAddress == null) {
			originalRemoteAddress = remoteAddress;
		}
		// if we set the address to null, we fall back to the original address
		// way too much code is not expecting this to be null
		remoteAddress = address == null ? originalRemoteAddress : address;
	}

	@Override
	public Long getMaxLifeTime() {
		return maxLifeTime;
	}
	public void setMaxLifeTime(Long maxLifeTime) {
		this.maxLifeTime = maxLifeTime;
	}

	@Override
	public Long getMaxIdleTime() {
		return maxIdleTime;
	}
	public void setMaxIdleTime(Long maxIdleTime) {
		this.maxIdleTime = maxIdleTime;
	}
	
}
