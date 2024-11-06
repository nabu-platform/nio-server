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

public enum PipelineState {
	/**
	 * The pipeline has requests pending or responses or is writing something out
	 */
	RUNNING,
	/**
	 * The pipeline has no pending requests or responses and any writing is done
	 * It is open to accepting new requests
	 */
	WAITING,
	/**
	 * No new data should be pushed to it but it may still process pending requests and write out response data 
	 */
	DRAINING,
	/**
	 * The pipeline is closed, it will not process new data or write out any responses, it may still process pending requests
	 */
	CLOSED
}
