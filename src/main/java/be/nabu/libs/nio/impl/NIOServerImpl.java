package be.nabu.libs.nio.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.events.api.EventTarget;
import be.nabu.libs.metrics.api.MetricGauge;
import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.nio.api.ConnectionAcceptor;
import be.nabu.libs.nio.api.NIODebugger;
import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.Pipeline;
import be.nabu.libs.nio.api.PipelineFactory;
import be.nabu.libs.nio.api.PipelineState;
import be.nabu.libs.nio.api.events.ConnectionEvent;
import be.nabu.libs.nio.impl.events.ConnectionEventImpl;
import be.nabu.utils.cep.api.EventSeverity;
import be.nabu.utils.cep.impl.CEPUtils;
import be.nabu.utils.cep.impl.NetworkedComplexEventImpl;
import be.nabu.utils.io.SSLServerMode;

/**
 * TODO: 
 * - track start of read (for a single request) with a timestamp, if read takes too long (especially in the synchronous part but also async) > kill connection
 * - same for start of write + timeout
 * - set limits on the size of the incoming/outgoing queues, not entirely sure if browsers have preconfigured limits on how many http requests they pipeline (https://en.wikipedia.org/wiki/HTTP_pipelining)
 * 		for firefox this is configured in "network.http.pipelining.maxrequests" which defaults to 32 but "network.http.pipelining" is disabled by default?
 */
public class NIOServerImpl implements NIOServer {
	
	public static String METRIC_ACCEPTED_CONNECTIONS = "acceptedConnections";
	public static String METRIC_REJECTED_CONNECTIONS = "rejectedConnections";
	public static String METRIC_CURRENT_CONNECTIONS = "currentConnections";
	public static String METRIC_ACTIVE_IO_THREADS = "activeIOThreads";
	public static String METRIC_ACTIVE_PROCESS_THREADS = "activeProcessThreads";
	public static String METRIC_IDLE_IO_THREADS = "idleIOThreads";
	public static String METRIC_IDLE_PROCESS_THREADS = "idleProcessThreads";
	
	private static String CHANNEL_TYPE_CLIENT = "client";
    private static String CHANNEL_TYPE_SERVER = "server";
    private static String CHANNEL_TYPE = "channelType";
    
	private ServerSocketChannel channel;
	private int port;
	protected Selector selector;
	protected Map<SocketChannel, Pipeline> channels = new ConcurrentHashMap<SocketChannel, Pipeline>();
	private volatile SSLContext sslContext;
	private MetricInstance metrics;
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected Date lastPrune;
	protected long pruneInterval = 5000;
	
	private ExecutorService ioExecutors, processExecutors;
	// in some case it is interesting to share executors with others to reduce overhead of pool management and/or explicitly share resources
	// if they are shared however, they don't need to be started nor stopped by this instance as someone else manages them
	private boolean executorsShared = false;
	private SSLServerMode sslServerMode;
	private PipelineFactory pipelineFactory;
	private ConnectionAcceptor connectionAcceptor;
	private EventDispatcher dispatcher;
	
	// this is the dispatcher used for low level events
	private EventTarget eventTarget;
	
	// by default an idle connection will time out after 5 minutes and even active connections will be dropped after 1 hour expecting a reconnect if necessary
	private Long maxIdleTime = 5l*60*1000, maxLifeTime = 60l*1000*60;
	private NIODebugger debugger;
	private volatile boolean stopping;
	private int ioPoolSize;
	private int processPoolSize;
	private ThreadFactory threadFactory;
	
	public NIOServerImpl(SSLContext sslContext, SSLServerMode sslServerMode, int port, ExecutorService ioExecutors, ExecutorService processExecutors, PipelineFactory pipelineFactory, EventDispatcher dispatcher) {
		this.sslContext = sslContext;
		this.sslServerMode = sslServerMode;
		this.port = port;
		this.ioExecutors = ioExecutors;
		this.processExecutors = processExecutors;
		this.pipelineFactory = pipelineFactory;
		this.dispatcher = dispatcher;
		this.executorsShared = true;
	}
	
	public NIOServerImpl(SSLContext sslContext, SSLServerMode sslServerMode, int port, int ioPoolSize, int processPoolSize, PipelineFactory pipelineFactory, EventDispatcher dispatcher, ThreadFactory threadFactory) {
		this.sslContext = sslContext;
		this.sslServerMode = sslServerMode;
		this.port = port;
		this.ioPoolSize = ioPoolSize;
		this.processPoolSize = processPoolSize;
		this.pipelineFactory = pipelineFactory;
		this.dispatcher = dispatcher;
		this.threadFactory = threadFactory;
	}
	
	@Override
	public Future<?> submitIOTask(Runnable runnable) {
		return ioExecutors.submit(runnable);
	}
	
	@Override
	public Future<?> submitProcessTask(Runnable runnable) {
		return processExecutors.submit(runnable);
	}
	
	protected ExecutorService getIoExecutors() {
		return ioExecutors;
	}

	protected ExecutorService getProcessExecutors() {
		return processExecutors;
	}

	@Override
	public void close(SelectionKey selectionKey) {
		if (selectionKey != null) {
			try {
				selectionKey.cancel();
				if (channels.containsKey(selectionKey.channel())) {
					Pipeline removed;
					synchronized(channels) {
						removed = channels.remove(selectionKey.channel());
					}
					if (removed != null) {
						dispatcher.fire(new ConnectionEventImpl(this, removed, ConnectionEvent.ConnectionState.CLOSED), this);
					}
				}
			}
			finally {
				try {
					selectionKey.channel().close();
				}
				catch (IOException e) {
					logger.error("Failed to close the channel", e);
				}
			}
		}
	}
	
	public static String getUserId(SocketAddress address) {
		InetSocketAddress remoteSocketAddress = ((InetSocketAddress) address);
		return remoteSocketAddress == null ? "unknown" : remoteSocketAddress.getAddress().getHostAddress() + ":" + remoteSocketAddress.getPort();
	}

	@SuppressWarnings("unchecked")
	public void start() throws IOException {
		startPools();
		
		channel = ServerSocketChannel.open();
		channel.bind(new InetSocketAddress(port));		// new InetSocketAddress("localhost", port)
		channel.configureBlocking(false);
		// http://www.unixguide.net/network/socketfaq/4.5.shtml
		// after releasing the socket on close() it may remain open on the system
		// this will allow us to quickly stop and restart the server, otherwise we run the risk that we get an "Address already in use" exception
		channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
		
		selector = Selector.open();
		
		// we're only interested in new (accepted) connections
		SelectionKey socketServerSelectionKey = channel.register(selector, SelectionKey.OP_ACCEPT);
		
		Map<String, String> properties = new HashMap<String, String>();
        properties.put(CHANNEL_TYPE, CHANNEL_TYPE_SERVER);
        socketServerSelectionKey.attach(properties);
        
        while(channel != null) {
        	// the selectNow() does NOT block whereas the select() does
        	// we want to make sure we update the interestOps() as soon as possible, we can't do a fully blocking wait here, unregistered write ops would simply be ignored
        	// the selectNow() however ends up in a permanent while loop and takes up 100% of at least one thread
        	// luckily the selector provides a wakeup() which unblocks the select() from another thread, this combines low overhead with quick interestops() updates
        	if (selector.select() == 0) {
        		continue;
        	}
        	Set<SelectionKey> selectedKeys = selector.selectedKeys();
        	Iterator<SelectionKey> iterator = selectedKeys.iterator();
        	while(iterator.hasNext()) {
        		SelectionKey key = iterator.next();
        		try {
	        		try {
		        		if (CHANNEL_TYPE_SERVER.equals(((Map<String, String>) key.attachment()).get(CHANNEL_TYPE))) {
		        			ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
		        			SocketChannel clientSocketChannel = serverSocketChannel.accept();
		        			MetricInstance metrics = this.metrics;
		        			if (clientSocketChannel != null) {
		        				NetworkedComplexEventImpl event = null;
		        				if (eventTarget != null) {
		        					InetSocketAddress socketAddress = (InetSocketAddress) clientSocketChannel.getRemoteAddress();
		        					event = CEPUtils.newServerNetworkEvent(getClass(), "server-connect", socketAddress);
		        					event.setDestinationPort(port);
		        				}
		        				if (connectionAcceptor != null && !connectionAcceptor.accept(this, clientSocketChannel)) {
		        					logger.warn("Connection rejected: " + clientSocketChannel.socket());
		        					if (metrics != null) {
										metrics.increment(METRIC_REJECTED_CONNECTIONS + ":" + getUserId(clientSocketChannel.socket().getRemoteSocketAddress()), 1l);
									}
		        					if (event != null) {
		        						event.setEventName("server-reject");
		        						event.setSeverity(EventSeverity.WARNING);
		        						eventTarget.fire(event, this);
		        					}
		        					dispatcher.fire(new ConnectionEventImpl(this, null, ConnectionEvent.ConnectionState.REJECTED), this);
		        					clientSocketChannel.close();
		        				}
		        				else {
			        				clientSocketChannel.configureBlocking(false);
			        				
			        				// initially we are only interested in available data from the remote user
			        				SelectionKey clientKey = clientSocketChannel.register(selector, SelectionKey.OP_READ);
			        				
			        				// set a key we can detect
			        				Map<String, String> clientproperties = new HashMap<String, String>();
			                        clientproperties.put(CHANNEL_TYPE, CHANNEL_TYPE_CLIENT);
			                        clientKey.attach(clientproperties);
			                        
			                        if (!channels.containsKey(clientSocketChannel)) {
			                        	try {
					                        synchronized(channels) {
				                        		if (!channels.containsKey(clientSocketChannel)) {
					                        		logger.debug("New connection: {}", clientSocketChannel);
													Pipeline newPipeline = pipelineFactory.newPipeline(this, clientKey);
													channels.put(clientSocketChannel, newPipeline);
													if (metrics != null) {
														metrics.increment(METRIC_ACCEPTED_CONNECTIONS + ":" + getUserId(clientSocketChannel.socket().getRemoteSocketAddress()), 1l);
													}
													if (event != null) {
														event.setSeverity(EventSeverity.DEBUG);
						        						eventTarget.fire(event, this);
						        					}
													dispatcher.fire(new ConnectionEventImpl(this, newPipeline, ConnectionEvent.ConnectionState.CONNECTED), this);
					                        	}
					                        }
			                        	}
			                        	catch (IOException e) {
			                        		logger.error("Failed pipeline", e);
			                        		if (event != null) {
				        						eventTarget.fire(CEPUtils.enrich(event, e), this);
				        					}
			                        		clientSocketChannel.close();
			                        	}
			                        }
		        				}
		        			}
		        		}
		        		else {
		        			SocketChannel clientChannel = (SocketChannel) key.channel();
		        			if (!channels.containsKey(clientChannel)) {
		        				logger.debug("No channel, cancelling key for: {}", clientChannel.socket());
		        				close(key);
		        			}
		        			else if (!key.isValid() || !clientChannel.isConnected() || !clientChannel.isOpen() || clientChannel.socket().isInputShutdown()) {
		        				logger.debug("Disconnected, cancelling key for: {}", clientChannel.socket());
		        				Pipeline pipeline = channels.get(clientChannel);
		        				if (pipeline != null) {
									pipeline.close();
		        				}
		        				else {
		        					close(key);
		        				}
		        			}
		        			else {
		        				if (key.isReadable() && channels.containsKey(clientChannel)) {
		        					Pipeline pipeline = channels.get(clientChannel);
			        				if (pipeline != null) {
			        					logger.trace("Scheduling pipeline, new data for: {}", clientChannel.socket());
			        					pipeline.read();
			        				}
		        				}
			        			if (key.isWritable() && channels.containsKey(clientChannel)) {
			        				logger.trace("Scheduling write processor, write buffer available for: {}", clientChannel.socket());
			        				Pipeline pipeline = channels.get(clientChannel);
			        				if (pipeline != null) {
			        					pipeline.write();
			        				}
			        			}
		        			}
		        		}
	        		}
	        		catch(CancelledKeyException e) {
	        			Pipeline pipeline = channels.get(key.channel());
	        			if (pipeline != null) {
		        			pipeline.close();
	        			}
	        			else {
	        				close(key);
	        			}
	        		}
        		}
        		catch (Exception e) {
        			logger.error("Unknown failure", e);
        		}
        		finally {
        			iterator.remove();
        		}
        	}
        	if (lastPrune == null || new Date().getTime() - lastPrune.getTime() > pruneInterval) {
        		pruneConnections();
        		lastPrune = new Date();
        	}
        }
	}

	public void startPools() {
		if (!executorsShared) {
			// start the pools
			ioExecutors = Executors.newFixedThreadPool(ioPoolSize, threadFactory);
			processExecutors = Executors.newFixedThreadPool(processPoolSize, threadFactory);
		}
	}
	
	protected void pruneConnections() {
		synchronized(channels) {
			Iterator<Entry<SocketChannel, Pipeline>> iterator = channels.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<SocketChannel, Pipeline> next = iterator.next();
				Date lastActivity = next.getValue().getSourceContext().getCreated();
				if (next.getValue().getLastRead() != null) {
					lastActivity = next.getValue().getLastRead();
				}
				if (next.getValue().getLastWritten() != null && (lastActivity == null || lastActivity.after(next.getValue().getLastWritten()))) {
					lastActivity = next.getValue().getLastWritten();
				}
				Date now = new Date();
				// the connection is gone
				if ((!next.getKey().isConnected() && !next.getKey().isConnectionPending())
					// the connection has exceeded its max lifetime and it is currently idle
					|| (maxLifeTime != null && maxLifeTime != 0 && PipelineState.WAITING.equals(next.getValue().getState()) && now.getTime() - next.getValue().getSourceContext().getCreated().getTime() > maxLifeTime)
					// the connection has exceeded its max idletime
					|| (maxIdleTime != null && maxIdleTime != 0 && PipelineState.WAITING.equals(next.getValue().getState()) && lastActivity != null && now.getTime() - lastActivity.getTime() > maxIdleTime)) {
					logger.warn("Pruning connection: [connected:" + next.getKey().isConnected() + "], [created:" + next.getValue().getSourceContext().getCreated() + "/" + maxLifeTime + "], [lastActivity:" + lastActivity + "/" + maxIdleTime + "]");
					try {
						next.getValue().close();
					}
					catch (IOException e) {
						logger.debug("Can not close connection", e);
					}
					iterator.remove();
				}
			}
		}
	}

	public SSLContext getSSLContext() {
		return sslContext;
	}
	
	// you can update the ssl context
	public void setSSLContext(SSLContext sslContext) {
		this.sslContext = sslContext;
	}

	public SSLServerMode getSSLServerMode() {
		return sslServerMode == null ? SSLServerMode.NO_CLIENT_CERTIFICATES : sslServerMode;
	}

	@Override
	public void setWriteInterest(SelectionKey selectionKey, boolean isInterested) {
		if (isInterested) {
			selectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
		}
		else {
			selectionKey.interestOps(SelectionKey.OP_READ);
		}
		if (selector != null) {
			selector.wakeup();
		}		
	}
	
	@Override
	public void setReadInterest(SelectionKey selectionKey, boolean isInterested) {
		int interestOps = selectionKey.interestOps();
		if (isInterested) {
			interestOps |= SelectionKey.OP_READ;
		}
		else {
			interestOps &= ~SelectionKey.OP_READ;
		}
		selectionKey.interestOps(interestOps);
		if (selector != null) {
			selector.wakeup();
		}		
	}
	
	@Override
	public void stop() {
		stopping = true;
		if (channel != null) {
			try {
				channel.close();
				channel = null;
			}
			catch (Exception e) {
				logger.error("Failed to close server", e);
			}
		}
		if (!channels.isEmpty()) {
			try {
				closePipelines();
			}
			catch (Exception e) {
				logger.error("Failed to close pipelines", e);
			}
		}
		if (selector != null) {
			try {
				selector.close();
				selector = null;
			}
			catch (Exception e) {
				logger.error("Failed to close selector", e);
			}
		}
		shutdownPools();
		stopping = false;
	}
	
	public void shutdownPools() {
		if (!executorsShared) {
			if (ioExecutors != null) {
				ioExecutors.shutdown();
				ioExecutors = null;
			}
			if (processExecutors != null) {
				processExecutors.shutdown();
				processExecutors = null;
			}
		}
	}
	
	public boolean isStopping() {
		return stopping;
	}
	
	public boolean isRunning() {
		return channel != null;
	}

	protected void closePipelines() {
		synchronized(channels) {
			for (Pipeline pipeline : channels.values()) {
				try {
					pipeline.close();
				}
				catch (Exception e) {
					logger.error("Could not close pipeline", e);
				}
			}
			channels.clear();
		}
	}
	
	@Override
	protected void finalize() {
		stop();
	}

	@Override
	public PipelineFactory getPipelineFactory() {
		return pipelineFactory;
	}

	@Override
	public ConnectionAcceptor getConnectionAcceptor() {
		return connectionAcceptor;
	}

	@Override
	public void setConnectionAcceptor(ConnectionAcceptor connectionAcceptor) {
		this.connectionAcceptor = connectionAcceptor;
	}

	@Override
	public Collection<Pipeline> getPipelines() {
		return channels.values();
	}

	@Override
	public void upgrade(SelectionKey key, Pipeline pipeline) {
		if (channels.containsKey(key.channel())) {
			synchronized(channels) {
				if (channels.containsKey(key.channel())) {
					channels.put((SocketChannel) key.channel(), pipeline);
				}
			}
		}
	}

	@Override
	public EventDispatcher getDispatcher() {
		return dispatcher;
	}

	@Override
	public MetricInstance getMetrics() {
		return metrics;
	}

	@Override
	public void setMetrics(MetricInstance metrics) {
		if (metrics != null) {
			metrics.set(METRIC_CURRENT_CONNECTIONS, new MetricGauge() {
				@Override
				public long getValue() {
					return channels.size();
				}
			});
			if (ioExecutors instanceof ThreadPoolExecutor) {
				metrics.set(METRIC_ACTIVE_IO_THREADS, new MetricGauge() {
					@Override
					public long getValue() {
						return ((ThreadPoolExecutor) ioExecutors).getActiveCount();
					}
				});
				metrics.set(METRIC_IDLE_IO_THREADS, new MetricGauge() {
					@Override
					public long getValue() {
						return ((ThreadPoolExecutor) ioExecutors).getMaximumPoolSize() - ((ThreadPoolExecutor) ioExecutors).getActiveCount();
					}
				});
			}
			if (processExecutors instanceof ThreadPoolExecutor) {
				metrics.set(METRIC_ACTIVE_PROCESS_THREADS, new MetricGauge() {
					@Override
					public long getValue() {
						return ((ThreadPoolExecutor) processExecutors).getActiveCount();
					}
				});
				metrics.set(METRIC_IDLE_PROCESS_THREADS, new MetricGauge() {
					@Override
					public long getValue() {
						return ((ThreadPoolExecutor) processExecutors).getMaximumPoolSize() - ((ThreadPoolExecutor) processExecutors).getActiveCount();
					}
				});
			}
		}
		this.metrics = metrics;
	}

	public Long getMaxIdleTime() {
		return maxIdleTime;
	}

	public void setMaxIdleTime(Long maxIdleTime) {
		this.maxIdleTime = maxIdleTime;
	}

	public Long getMaxLifeTime() {
		return maxLifeTime;
	}

	public void setMaxLifeTime(Long maxLifeTime) {
		this.maxLifeTime = maxLifeTime;
	}

	public long getPruneInterval() {
		return pruneInterval;
	}

	public void setPruneInterval(long pruneInterval) {
		this.pruneInterval = pruneInterval;
	}

	public int getPort() {
		return port;
	}

	@Override
	public NIODebugger getDebugger() {
		return debugger;
	}
	public void setDebugger(NIODebugger debugger) {
		this.debugger = debugger;
	}
	
	public EventTarget getEventTarget() {
		return eventTarget;
	}
	public void setEventTarget(EventTarget eventTarget) {
		this.eventTarget = eventTarget;
	}

	@Override
	public <E> void fire(E event, Object source) {
		if (eventTarget != null) {
			eventTarget.fire(event, source);
		}
	}
	
}
