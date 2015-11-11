package be.nabu.libs.nio.impl;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.events.api.ResponseHandler;
import be.nabu.libs.nio.api.ExceptionFormatter;
import be.nabu.libs.nio.api.MessageProcessor;
import be.nabu.libs.nio.api.SecurityContext;
import be.nabu.libs.nio.api.SourceContext;

public class EventDrivenMessageProcessor<T, R> implements MessageProcessor<T, R> {

	private EventDispatcher dispatcher;
	private ExceptionFormatter<T, R> exceptionFormatter;
	private Class<T> requestClass;
	private Class<R> responseClass;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private boolean dispatchResponse;

	public EventDrivenMessageProcessor(Class<T> requestClass, Class<R> responseClass, EventDispatcher dispatcher, ExceptionFormatter<T, R> exceptionFormatter, boolean dispatchResponse) {
		this.requestClass = requestClass;
		this.responseClass = responseClass;
		this.dispatcher = dispatcher;
		this.exceptionFormatter = exceptionFormatter;
		this.dispatchResponse = dispatchResponse;
	}
	
	@Override
	public R process(SecurityContext securityContext, SourceContext sourceContext, final T request) {
		Date timestamp = new Date();
		R response = dispatcher.fire(request, this, new ResponseHandler<T, R>() {
			@SuppressWarnings("unchecked")
			@Override
			public R handle(T request, Object response, boolean isLast) {
				if (response instanceof Exception) {
					return exceptionFormatter.format(request, (Exception) response);
				}
				else if (response != null && responseClass.isAssignableFrom(response.getClass())) {
					return (R) response;
				}
				return null;
			}
		}, new ResponseHandler<T, T>()  {
			@SuppressWarnings("unchecked")
			@Override
			public T handle(T request, Object rewritten, boolean isLast) {
				if (rewritten != null && requestClass.isAssignableFrom(rewritten.getClass())) {
					return (T) rewritten;
				}
				return null;
			}
		});
		logger.debug("Processed " + request.hashCode() + " in: " + (new Date().getTime() - timestamp.getTime()) + "ms");
		// if there is a response, send it up the event dispatcher again for potential rewriting
		if (response != null && dispatchResponse) {
			timestamp = new Date();
			// fire the response to allow others to alter it
			R alteredResponse = dispatcher.fire(response, this, new ResponseHandler<R, R>() {
				@SuppressWarnings("unchecked")
				@Override
				public R handle(R original, Object proposed, boolean isLast) {
					if (proposed instanceof Exception) {
						return exceptionFormatter.format(request, (Exception) proposed);
					}
					else if (proposed != null && responseClass.isAssignableFrom(proposed.getClass())) {
						return (R) proposed;
					}
					return null;
				}
			});
			if (alteredResponse != null) {
				logger.debug("Altered response to " + request.hashCode() + " in: " + (new Date().getTime() - timestamp.getTime()) + "ms");
				response = alteredResponse;
			}
		}
		return response;
	}

	@Override
	public Class<T> getRequestClass() {
		return requestClass;
	}

	@Override
	public Class<R> getResponseClass() {
		return responseClass;
	}

	public EventDispatcher getDispatcher() {
		return dispatcher;
	}

	public ExceptionFormatter<T, R> getExceptionFormatter() {
		return exceptionFormatter;
	}

}
