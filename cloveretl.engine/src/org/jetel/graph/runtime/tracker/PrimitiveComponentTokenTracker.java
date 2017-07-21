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
import org.jetel.data.DataRecord;
import org.jetel.graph.JobType;
import org.jetel.graph.Node;
import org.jetel.graph.runtime.IAuthorityProxy.RunStatus;
import org.jetel.util.ExceptionUtils;

/**
 * Default component token tracker which is used by all components in case regular ETL graph execution.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 15.6.2012
 */
public class PrimitiveComponentTokenTracker implements ComponentTokenTracker {

	private Node component;
	
	private Log4jTokenTrackerSerializer log4jSerializer;
	
	private TokenContent tokenContent;
	
	public PrimitiveComponentTokenTracker(Node component) {
		this.component = component;
		log4jSerializer = new Log4jTokenTrackerSerializer(component.getGraph());
		log4jSerializer.setLogger(component.getLog());
		tokenContent = new TokenContent();
	}

	@Override
	public void executeJob(DataRecord token, JobType jobType, RunStatus runStatus) {
		tokenContent.setRecord(token);
		
		log4jSerializer.executeJob(new Date(), component.getId(), tokenContent, jobType, runStatus);
	}

	@Override
	public void jobFinished(DataRecord token, JobType jobType, RunStatus runStatus) {
		tokenContent.setRecord(token);
		
		log4jSerializer.jobFinished(new Date(), component.getId(), tokenContent, jobType, runStatus);
	}

	@Override
	public void logMessage(DataRecord token, Level level, String message, Throwable exception) {
		tokenContent.setRecord(token);
		
		log4jSerializer.logMessage(new Date(), component.getId(), tokenContent, level, message, ExceptionUtils.stackTraceToString(exception));
	}

	@Override
	public void initToken(DataRecord token) {
		//DO NOTHING
	}

	@Override
	public void freeToken(DataRecord token) {
		//DO NOTHING
	}

	@Override
	public void readToken(int inputPort, DataRecord token) {
		//DO NOTHING
	}

	@Override
	public void writeToken(int outputPort, DataRecord token) {
		//DO NOTHING
	}

	@Override
	public void eofInputPort(int portNum) {
		//DO NOTHING
	}

	@Override
	public void eofOutputPort(int portNum) {
		//DO NOTHING
	}

	@Override
	public void linkTokens(DataRecord sourceToken, DataRecord targetToken) {
		//DO NOTHING
	}

	@Override
	public void unifyTokens(DataRecord sourceToken, DataRecord targetToken) {
		//DO NOTHING
	}

}
