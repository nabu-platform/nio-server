package be.nabu.libs.nio.api;

public interface MessageProcessor<T, R> {
	public R process(SecurityContext context, SourceContext sourceContext, T request);
}
