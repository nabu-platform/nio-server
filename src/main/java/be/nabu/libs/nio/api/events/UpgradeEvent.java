package be.nabu.libs.nio.api.events;

import be.nabu.libs.nio.api.Pipeline;

public interface UpgradeEvent {
	public Pipeline getOriginalPipeline();
	public Pipeline getNewPipeline();
}
