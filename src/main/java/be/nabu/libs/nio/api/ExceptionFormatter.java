package be.nabu.libs.nio.api;

/**
 * An exception formatter allows you to generate a response in case of an exception
 * The original request may not be known at the time of formatting so it can be null
 */
public interface ExceptionFormatter<T, R> {
	public R format(T request, Exception e);
}
