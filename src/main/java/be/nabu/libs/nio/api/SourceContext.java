package be.nabu.libs.nio.api;

import java.net.Socket;
import java.util.Date;

public interface SourceContext {
	public Socket getSocket();
	public Date getCreated();
}
