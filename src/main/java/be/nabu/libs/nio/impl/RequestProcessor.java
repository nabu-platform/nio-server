package be.nabu.libs.nio.impl;

import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.metrics.api.MetricTimer;
import be.nabu.libs.nio.PipelineUtils;
import be.nabu.libs.nio.api.MessageProcessor;

public class RequestProcessor<T, R> implements Runnable {

	public static final String TOTAL_PROCESS_TIME = "totalProcessTime";
	public static final String USER_PROCESS_TIME = "userProcessTime";
	
	private MessagePipelineImpl<T, R> pipeline;

	RequestProcessor(MessagePipelineImpl<T, R> pipeline) {
		this.pipeline = pipeline;
	}
	
	@Override
	public void run() {
		PipelineUtils.setPipelineForThread(pipeline);
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
					timer = metrics.start(TOTAL_PROCESS_TIME);
				}
				response = processor.process(pipeline.getSecurityContext(), pipeline.getSourceContext(), request);
				if (timer != null) {
					metrics.log(USER_PROCESS_TIME + ":" + NIOServerImpl.getUserId(pipeline.getSourceContext().getSocket()), timer.stop());
				}
			}
			catch (Exception e) {
				response = pipeline.getExceptionFormatter().format(request, e);
			}
			if (response != null) {
				pipeline.getResponseQueue().add(response);
			}
		}
		PipelineUtils.setPipelineForThread(null);
	}

}
