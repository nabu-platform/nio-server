package be.nabu.libs.nio.impl.udp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.events.api.EventTarget;
import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.nio.api.ConnectionAcceptor;
import be.nabu.libs.nio.api.NIODebugger;
import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.Pipeline;
import be.nabu.libs.nio.api.PipelineFactory;
import be.nabu.utils.io.SSLServerMode;

/**
 * There is currently no support for DTLS
 */
public class UDPServerImpl implements NIOServer {

	private int port;
	private EventDispatcher dispatcher;
	private PipelineFactory pipelineFactory;
	private ExecutorService ioExecutors;
	private ExecutorService processExecutors;
	private MetricInstance metrics;
	private Selector selector;
	private Map<SocketAddress, UDPSelectionKey> keys = new ConcurrentHashMap<SocketAddress, UDPSelectionKey>();
	private Map<SocketAddress, Pipeline> pipelines = new ConcurrentHashMap<SocketAddress, Pipeline>();
	private DatagramChannel channel;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private NIODebugger debugger;
	private boolean stopping;
	private EventTarget eventTarget;

	public UDPServerImpl(int port, int ioPoolSize, int processPoolSize, PipelineFactory pipelineFactory, EventDispatcher dispatcher, ThreadFactory threadFactory) {
		this.port = port;
		this.pipelineFactory = pipelineFactory;
		this.dispatcher = dispatcher;
		this.ioExecutors = Executors.newFixedThreadPool(ioPoolSize, threadFactory);
		this.processExecutors = Executors.newFixedThreadPool(processPoolSize, threadFactory);
	}
	
	@Override
	public Future<?> submitIOTask(Runnable runnable) {
		return ioExecutors.submit(runnable);
	}
	
	@Override
	public Future<?> submitProcessTask(Runnable runnable) {
		return processExecutors.submit(runnable);
	}
	
	@Override
	public void start() throws IOException {
		channel = DatagramChannel.open();
		channel.bind(new InetSocketAddress(port));
		channel.configureBlocking(false);
		channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);

		selector = Selector.open();
		channel.register(selector, SelectionKey.OP_READ);
		
		int bufferSize = channel.getOption(StandardSocketOptions.SO_RCVBUF);
		while(true) {
			selector.select();
			ByteBuffer packet = ByteBuffer.allocate(bufferSize);
			SocketAddress socketAddress = channel.receive(packet);
			if (socketAddress != null) {
				if (!pipelines.containsKey(socketAddress)) {
					synchronized(pipelines) {
						if (!pipelines.containsKey(socketAddress)) {
							UDPSelectionKey key = new UDPSelectionKey(this, socketAddress);
							keys.put(socketAddress, key);
							pipelines.put(socketAddress, pipelineFactory.newPipeline(this, key));
						}
					}
				}
				// push the data to the correct channel
				UDPSelectionKey udpSelectionKey = keys.get(socketAddress);
				packet.flip();
				udpSelectionKey.channel().push(packet);
				
				// schedule a read
				Pipeline pipeline = pipelines.get(socketAddress);
				pipeline.read();
			}
		}
	}

	DatagramChannel getChannel() {
		return channel;
	}
	
	Selector getSelector() {
		return selector;
	}
	
	@Override
	public void stop() {
		if (channel != null) {
			stopping = true;
			try {
				channel.close();
				// this construct seems safer than that in NIOServerImpl, not sure why it is implemented as it is there
				// but it has not triggered any problem so far so leaving it as is
				synchronized(pipelines) {
					for (Pipeline pipeline : new ArrayList<Pipeline>(pipelines.values())) {
						try {
							pipeline.close();
						}
						catch (Exception e) {
							logger.error("Could not close pipeline", e);
						}
					}
					pipelines.clear();
					keys.clear();
				}
				channel = null;
			}
			catch (IOException e) {
				logger.error("Failed to close server", e);
			}
			finally {
				stopping = false;
			}
		}		
	}

	@Override
	public SSLContext getSSLContext() {
		return null;
	}

	@Override
	public SSLServerMode getSSLServerMode() {
		return null;
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
		this.metrics = metrics;
	}

	@Override
	public void close(SelectionKey key) {
		synchronized(pipelines) {
			Iterator<SocketAddress> iterator = keys.keySet().iterator();
			while (iterator.hasNext()) {
				SocketAddress address = iterator.next();
				if (keys.get(address).equals(key)) {
					pipelines.remove(address);
					iterator.remove();
					break;
				}
			}
		}
	}

	@Override
	public void setWriteInterest(SelectionKey key, boolean isInterested) {
		if (isInterested) {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public void upgrade(SelectionKey key, Pipeline pipeline) {
		throw new UnsupportedOperationException();		
	}

	@Override
	public PipelineFactory getPipelineFactory() {
		return pipelineFactory;
	}

	@Override
	public Collection<Pipeline> getPipelines() {
		return new ArrayList<Pipeline>();
	}

	@Override
	public ConnectionAcceptor getConnectionAcceptor() {
		return null;
	}

	@Override
	public void setConnectionAcceptor(ConnectionAcceptor connectionAcceptor) {
		throw new UnsupportedOperationException();
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

	@Override
	public boolean isStopping() {
		return stopping;
	}

	@Override
	public boolean isRunning() {
		return channel != null;
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
