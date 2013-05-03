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

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jetel.graph.JobType;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.IAuthorityProxy.RunStatus;
import org.jetel.util.string.StringUtils;

/**
 * This token tracker serializer is dedicated to prints out all token events to log4j logger.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27 Apr 2012
 */
public class Log4jTokenTrackerSerializer implements TokenTrackerSerializer {

	private static final Logger DEFAULT_LOGGER = Logger.getLogger(Log4jTokenTrackerSerializer.class);
	
	private TransformationGraph graph;

	private Logger logger;
	
	public Log4jTokenTrackerSerializer(TransformationGraph graph) {
		this.graph = graph;
	}
	
	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	
	protected Logger getLogger() {
		return logger != null ? logger : DEFAULT_LOGGER;
	}
	
	@Override
	public void initToken(Date date, String nodeId, TokenContent token) {
		getLogger().info(String.format("Token [%s] created.", token.getLabel()));
	}

	@Override
	public void freeToken(Date date, String nodeId, TokenContent token) {
		getLogger().info(String.format("Token [%s] terminated.", token.getLabel()));
	}

	@Override
	public void readToken(Date date, String nodeId, int port, TokenContent token) {
		getLogger().info(String.format("Token [%s] received from input port %s.", token.getLabel(), port));
	}

	@Override
	public void writeToken(Date date, String nodeId, int port, TokenContent token) {
		getLogger().info(String.format("Token [%s] sent to output port %s.", token.getLabel(), port));
	}

	@Override
	public void linkTokens(Date date, String nodeId, TokenContent sourceToken, TokenContent targetToken) {
		getLogger().info(String.format("Token [%s] is linked to token [%s].", sourceToken.getLabel(), targetToken.getLabel()));
	}

	@Override
	public void executeJob(Date date, String nodeId, TokenContent token, JobType jobType, RunStatus runStatus) {
		StringBuilder result = new StringBuilder();
		if (token != null && token.getTokenId() >= 0) { 
			result.append(String.format("Token [%s] ", token.getLabel()));
		}
		result.append(String.format("started %s:%s:%s%s%s.",
				jobType,
				runStatus.runId,
				runStatus.jobUrl,
				StringUtils.isEmpty(runStatus.executionGroup) ? "" : " in execution group \"" + runStatus.executionGroup + "\"", 
				StringUtils.isEmpty(runStatus.clusterNodeId) ? "" : " on node " + runStatus.clusterNodeId));
		if (runStatus.graphParameters != null && !runStatus.graphParameters.isEmpty()) {
			result.append(String.format("\nGraph parameters:\n%s",
					formatProperties(runStatus.graphParameters)));
		}
		if (runStatus.dictionaryIn != null && !runStatus.dictionaryIn.isEmpty()) {
			result.append(String.format("\nInitial dictionary content:\n%s",
					formatProperties(runStatus.dictionaryIn.toProperties())));
		}
		getLogger().info(result.toString());
	}

	@Override
	public void jobFinished(Date date, String nodeId, TokenContent token, JobType jobType, RunStatus runStatus) {
		StringBuilder result = new StringBuilder();
		if (token != null && token.getTokenId() >= 0) { 
			result.append(String.format("Token [%s] ", token.getLabel()));
		}
		result.append(String.format("detected finish %s:%s:%s%s with status %s%s.",
				jobType,
				runStatus.runId == 0 ? "" : runStatus.runId,
				runStatus.jobUrl,
				StringUtils.isEmpty(runStatus.clusterNodeId) ? "" : " on node " + runStatus.clusterNodeId,
				runStatus.status,
				getErrorDescription(runStatus)));
		if (runStatus.dictionaryOut != null && !runStatus.dictionaryOut.isEmpty()) {
			result.append(String.format("\nFinal dictionary content:\n%s",
					formatProperties(runStatus.dictionaryOut.toProperties())));
		}
		getLogger().info(result.toString());
	}

	private String getErrorDescription(RunStatus runStatus) {
		if (runStatus.status == Result.ERROR) {
			if (!StringUtils.isEmpty(runStatus.errComponent)) {
				return String.format(" on component %s with message:\n%s", runStatus.errComponent, runStatus.errMessage);
			} else {
				return String.format(" with message:\n%s", runStatus.errMessage);
			}
		} else {
			return "";
		}
	}
	
	@Override
	public void logMessage(Date date, String nodeId, TokenContent token, Level level, String message, String exception) {
		StringBuilder result = new StringBuilder();
		if (token != null && token.getTokenId() >= 0) { 
			result.append(String.format("Token [%s] : ", token.getLabel()));
		}
		if (!StringUtils.isEmpty(message)) {
			result.append(message);
		}
		if (!StringUtils.isEmpty(exception)) {
			result.append("\n");
			result.append(exception);
		}
		getLogger().log(level, result.toString());
	}
	
	private static String formatProperties(Properties properties) {
		if (properties == null) {
			return null;
		}
		
		StringBuilder result = new StringBuilder();
		
		String[] propertyNames = properties.stringPropertyNames().toArray(new String[0]);
		Arrays.sort(propertyNames);
		
		for (String propertyName : propertyNames) {
			result.append('\t');
			result.append(propertyName);
			result.append('=');
			result.append(properties.getProperty(propertyName));
			result.append('\n');
		}
		
		if (result.length() > 0) {
			result.setLength(result.length() - 1);
		}
		
		return result.toString();
	}
	
}
