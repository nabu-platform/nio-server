package be.nabu.libs.nio.api;

public interface MessageProcessor<T, R> {
	public R process(SecurityContext securityContext, SourceContext sourceContext, T request);
	public Class<T> getRequestClass();
	public Class<R> getResponseClass();
}
