package be.nabu.libs.nio.impl;

import be.nabu.libs.nio.api.MessageProcessor;


public class RequestProcessor<T, R> implements Runnable {

	private MessagePipelineImpl<T, R> pipeline;

	RequestProcessor(MessagePipelineImpl<T, R> pipeline) {
		this.pipeline = pipeline;
	}
	
	@Override
	public void run() {
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
				response = processor.process(pipeline.getSecurityContext(), pipeline.getSourceContext(), request);
			}
			catch (Exception e) {
				response = pipeline.getExceptionFormatter().format(request, e);
			}
			if (response != null) {
				pipeline.getResponseQueue().add(response);
			}
		}
	}

}
