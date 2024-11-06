/*
* Copyright (C) 2015 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.libs.nio.api;

import java.io.IOException;

import javax.net.ssl.SSLContext;

import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.utils.io.SSLServerMode;

public interface Server {
	public void start() throws IOException;
	public void stop();
	public SSLContext getSSLContext();
	public SSLServerMode getSSLServerMode();
	public EventDispatcher getDispatcher();
	public MetricInstance getMetrics();
	public void setMetrics(MetricInstance metrics);
}
