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

import org.apache.log4j.Level;
import org.jetel.data.DataRecord;
import org.jetel.graph.JobType;
import org.jetel.graph.JobflowEdge;
import org.jetel.graph.Node;
import org.jetel.graph.runtime.IAuthorityProxy.RunStatus;

/**
 * Component token tracker is used by components for token life-cycle tracking.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17 Apr 2012
 * @see TokenTracker
 * @see Node#getTokenTracker()
 */
public interface ComponentTokenTracker {

	/**
	 * New token is registered and unique token identifier is generated.
	 * This method should be called in case a new token is created.
	 */
	public void initToken(DataRecord token);
	
	/**
	 * Token is released and no other token tracking is expected.
	 */
	public void freeToken(DataRecord token);
	
	/**
	 * Given token was read by component from a port.
	 */
	public void readToken(int inputPort, DataRecord token);
	
	/**
	 * Given token was written by component to a port.
	 */
	public void writeToken(int outputPort, DataRecord token);
	
	/**
	 * This method informs token tracker about EOF on an input port.
	 * This method is invoked automatically by {@link JobflowEdge}.
	 */
	public void eofInputPort(int portNum);
	
	/**
	 * This method informs token tracker about EOF on an output port.
	 * This method is invoked automatically by {@link JobflowEdge}.
	 */
	public void eofOutputPort(int portNum);
	
	/**
	 * Two given tokens are considered with parent-child relationship.
	 */
	public void linkTokens(DataRecord sourceToken, DataRecord targetToken);
	
	/**
	 * A job was executed by a token.
	 * @param token caused token
	 * @param jobType type of executed job
	 * @param runId run identifier of executed job
	 * @param settings custom information about executed job
	 */
	public void executeJob(DataRecord token, JobType jobType, RunStatus runStatus);
	
	/**
	 * A job finished with a token.
	 * @param token reported token
	 * @param jobType type of finished job
	 * @param runId run identifier of finished job
	 * @param result custom information about finished job
	 */
	public void jobFinished(DataRecord token, JobType jobType, RunStatus runStatus);
	
	/**
	 * General tracking message for a token
	 * @param token reporting token
	 * @param level logging level
	 * @param message reported message
	 * @param exception caused exception
	 */
	public void logMessage(DataRecord token, Level level, String message, Throwable exception);
	
	/**
	 * Token identification of two given tokens is synchronized. Both tokens are from now considered equal.  
	 */
	public void unifyTokens(DataRecord sourceToken, DataRecord targetToken);
	
}
