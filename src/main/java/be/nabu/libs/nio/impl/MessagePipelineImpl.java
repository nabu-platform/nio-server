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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import be.nabu.libs.nio.api.ExceptionFormatter;
import be.nabu.libs.nio.api.KeepAliveDecider;
import be.nabu.libs.nio.api.MessageFormatterFactory;
import be.nabu.libs.nio.api.MessageParserFactory;
import be.nabu.libs.nio.api.MessagePipeline;
import be.nabu.libs.nio.api.MessageProcessorFactory;
import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.PipelineState;
import be.nabu.libs.nio.api.PipelineWithMetaData;
import be.nabu.libs.nio.api.SecurityContext;
import be.nabu.libs.nio.api.SourceContext;
import be.nabu.libs.nio.api.UpgradeableMessagePipeline;
import be.nabu.libs.nio.api.events.ConnectionEvent;
import be.nabu.libs.nio.impl.events.ConnectionEventImpl;
import be.nabu.libs.nio.impl.udp.UDPChannel;
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
public class MessagePipelineImpl<T, R> implements UpgradeableMessagePipeline<T, R>, PipelineWithMetaData {
	
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
	
	/**
	 * It is possible that this pipeline replaced an existing pipeline (for example after a connection upgrade or a live protocol switch)
	 * This is the parent pipeline that was handling the data before it was supplanted, it is mostly for allowing drainage
	 */
	private MessagePipelineImpl<?, ?> parentPipeline;
	
	private boolean closed;
	private boolean useSsl;
	private boolean debug = Boolean.parseBoolean(System.getProperty("nio.debug.pipeline", "false"));
	private SocketAddress remoteAddress;
	private Integer localPort;
	
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
		
		initMetadata();
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
			}
		}
	}
	
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
		process(false);
	}

	void process(boolean force) {
		lastProcessed = new Date();
		if (force || futureProcess == null || futureProcess.isDone()) {
			synchronized(this) {
				if (force || futureProcess == null || futureProcess.isDone()) {
					futureProcess = server.submitProcessTask(requestProcessor);
				}
			}
		}
	}

	/**
	 * Tiny hack to reschedule reading of the request framer for udp messages
	 */
	boolean rescheduleRead() {
		return getChannel() instanceof UDPChannel && (((UDPChannel) getChannel()).hasPending() || requestFramer.remainingData() > 0);
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
				}
				catch (IOException e) {
					logger.error("Could not finish handshake", e);
					close();
				}
			}
		});
		return futureRead;
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
	
}
