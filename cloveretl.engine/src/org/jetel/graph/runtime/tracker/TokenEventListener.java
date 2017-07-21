/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.graph.runtime.tracker;

import java.util.Date;

import org.apache.log4j.Level;
import org.jetel.graph.JobType;
import org.jetel.graph.runtime.IAuthorityProxy.RunStatus;

/**
 * All token events gathered by {@link TokenTracker} can be listened by implementations of this interface.
 *  
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26 Apr 2012
 */
public interface TokenEventListener {

	/**
	 * New token was created in a component.
	 * @param date time stamp of the event
	 * @param componentId component id where the token was created
	 * @param token content of token
	 */
	public void initToken(Date date, String componentId, TokenContent token);

	/**
	 * Given token is release in a component. No other events for this token are expected.
	 * @param date time stamp of the event
	 * @param componentId component id where the token was released
	 * @param token content of token
	 */
	public void freeToken(Date date, String componentId, TokenContent token);

	/**
	 * Token was read by a component.
	 * @param date time stamp of the event
	 * @param componentId component id where the token was read
	 * @param inputPort input port index where the token was read
	 * @param token content of token
	 */
	public void readToken(Date date, String componentId, int inputPort, TokenContent token);

	/**
	 * Token was written by a component.
	 * @param date time stamp of the event
	 * @param componentId component id where the token was written
	 * @param outputPort output port index where the token was written
	 * @param token content of token
	 */
	public void writeToken(Date date, String componentId, int outputPort, TokenContent token);

	/**
	 * Two given token are considered in parent-child relationship.
	 * @param date time stamp of the event
	 * @param componentId component id where two tokens was linked
	 * @param sourceToken parent token
	 * @param targetToken child token
	 */
	public void linkTokens(Date date, String componentId, TokenContent sourceToken, TokenContent targetToken);
	
	/**
	 * Token executes a job
	 * @param date time stamp of the event
	 * @param componentId component id where the token executes a job
	 * @param token caused token
	 * @param jobType type of executed job
	 * @param runId run id of executed job
	 * @param settings custom settings of executed job
	 */
	public void executeJob(Date date, String componentId, TokenContent token, JobType jobType, RunStatus runStatus);

	/**
	 * Token reports a job is finished.
	 * @param date time stamp of the event
	 * @param componentId component id where the job was finished
	 * @param token reported token
	 * @param jobType type of finished job
	 * @param runId run id of finished job
	 * @param result custom information about finished job
	 */
	public void jobFinished(Date date, String componentId, TokenContent token, JobType jobType, RunStatus runStatus);
	
	/**
	 * Token reports a message
	 * @param date time stamp of the event
	 * @param componentId component id where the message is reported
	 * @param token reported token
	 * @param level logging level
	 * @param message reported message
	 * @param exception reported exception
	 */
	public void logMessage(Date date, String componentId, TokenContent token, Level level, String message, String exception);
	
}
