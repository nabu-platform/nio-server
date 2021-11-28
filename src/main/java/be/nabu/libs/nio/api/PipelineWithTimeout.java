package be.nabu.libs.nio.api;

public interface PipelineWithTimeout extends Pipeline {
	public Long getMaxLifeTime();
	public Long getMaxIdleTime();
}
