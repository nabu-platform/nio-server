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

package be.nabu.libs.nio.api.events;

import be.nabu.libs.nio.api.NIOServer;
import be.nabu.libs.nio.api.Pipeline;

public interface ConnectionEvent {
	
	public enum ConnectionState {
		CONNECTED,
		REJECTED,
		BEFORE_CLOSE,
		CLOSED,
		// after the connection has been closed and no additional data in the input, we send out an empty event 
		EMPTY,
		UPGRADED
	}
	
	public NIOServer getServer();
	public ConnectionState getState();
	public Pipeline getPipeline();
}
