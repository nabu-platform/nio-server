package be.nabu.libs.nio.impl;


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
				response = pipeline.getMessageProcessor().process(pipeline.getSecurityContext(), pipeline.getSourceContext(), request);
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
