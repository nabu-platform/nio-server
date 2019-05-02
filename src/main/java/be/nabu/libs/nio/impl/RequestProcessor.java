package be.nabu.libs.nio.impl;

import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.metrics.api.MetricTimer;
import be.nabu.libs.nio.PipelineUtils;
import be.nabu.libs.nio.api.MessageProcessor;

public class RequestProcessor<T, R> implements Runnable {

	public static final String PROCESS_TIME = "processTime";
	
	private MessagePipelineImpl<T, R> pipeline;

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
					response = pipeline.getExceptionFormatter().format(request, e);
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

}
