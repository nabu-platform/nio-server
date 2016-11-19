package be.nabu.libs.nio.api;

import java.net.SocketAddress;
import java.util.Date;

public interface SourceContext {
	public SocketAddress getSocketAddress();
	public int getLocalPort();
	public Date getCreated();
}
