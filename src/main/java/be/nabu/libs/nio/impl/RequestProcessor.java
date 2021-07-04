package be.nabu.libs.nio.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.metrics.api.MetricTimer;
import be.nabu.libs.nio.PipelineUtils;
import be.nabu.libs.nio.api.MessageProcessor;

public class RequestProcessor<T, R> implements Runnable {

	public static final String PROCESS_TIME = "processTime";
	
	private static ThreadLocal<Object> currentRequest = new ThreadLocal<Object>();
	
	private MessagePipelineImpl<T, R> pipeline;
	private Logger logger = LoggerFactory.getLogger(getClass());

	RequestProcessor(MessagePipelineImpl<T, R> pipeline) {
		this.pipeline = pipeline;
	}
	
	@Override
	public void run() {
		// GDPR
//		pipeline.putMDCContext();
		PipelineUtils.setPipelineForThread(pipeline);
		try {
			while(!Thread.interrupted()) {
				T request = pipeline.getRequestQueue().poll();
				if (request == null) {
					break;
				}
				R response;
				try {
					currentRequest.set(request);
					MessageProcessor<T, R> processor = pipeline.getMessageProcessorFactory().newProcessor(request);
					if (processor == null) {
						throw new IllegalArgumentException("There is no processor for the request");
					}
					MetricTimer timer = null;
					MetricInstance metrics = pipeline.getServer().getMetrics();
					if (metrics != null) {
						timer = metrics.start(PROCESS_TIME + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocketAddress()));
					}
					response = processor.process(pipeline.getSecurityContext(), pipeline.getSourceContext(), request);
					if (timer != null) {
						timer.stop();
					}
				}
				catch (Exception e) {
					try {
						response = pipeline.getExceptionFormatter().format(request, e);
					}
					catch (Exception f) {
						logger.error("Could not format exception", f);
						pipeline.close();
						throw new RuntimeException(f);
					}
				}
				finally {
					currentRequest.set(null);
				}
				if (response != null) {
					pipeline.getResponseQueue().add(response);
				}
			}
		}
		finally {
			PipelineUtils.setPipelineForThread(null);
		}
	}
	
	public static Object getCurrentRequest() {
		return currentRequest.get();
	}

}
