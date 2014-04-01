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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.jetel.data.Token;
import org.jetel.graph.JobType;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.IAuthorityProxy.RunStatus;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.util.ExceptionUtils;

/**
 * This is root class for whole token tracking toolkit.
 * Token tracker instance is managed by {@link WatchDog}, so each running graph has
 * associated a {@link TokenTracker}. All token events from all components and edges are gather by this tracker
 * and passed to all registered event serializers.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26 Apr 2012
 */
public class TokenTracker {

	private TransformationGraph graph;
	
	private List<TokenTrackerSerializer> serializers = new ArrayList<TokenTrackerSerializer>();
	
	private TokenContent tokenContent;
	private TokenContent sourceTokenContent;
	private TokenContent targetTokenContent;
	
	private long tokenSequence = 1;
	
	public TokenTracker(TransformationGraph graph) {
		this.graph = graph;
		
		serializers.add(new Log4jTokenTrackerSerializer(graph));
		
		tokenContent = new TokenContent();
		sourceTokenContent = new TokenContent();
		targetTokenContent = new TokenContent();
	}
	
	/**
	 * Token was created in a component.
	 */
	public synchronized void initToken(Node component, Token token) {
		token.setTokenId(nextTokenId());
		tokenContent.setToken(token);
		
		for (TokenTrackerSerializer serializer : serializers) {
			serializer.initToken(new Date(), component.getId(), tokenContent);
		}
	}
	
	/**
	 * Token was release in a component.
	 */
	public synchronized void freeToken(Node component, Token token) {
		tokenContent.setToken(token);
		
		for (TokenTrackerSerializer serializer : serializers) {
			serializer.freeToken(new Date(), component.getId(), tokenContent);
		}
	}
	
	/**
	 * Token was read by a component from a port.
	 */
	public synchronized void readToken(Node component, int inputPort, Token token) {
		tokenContent.setToken(token);
		
		for (TokenTrackerSerializer serializer : serializers) {
			serializer.readToken(new Date(), component.getId(), inputPort, tokenContent);
		}
	}

	/**
	 * Token was written by a component
	 */
	public synchronized void writeToken(Node component, int outputPort, Token token) {
		tokenContent.setToken(token);
		
		for (TokenTrackerSerializer serializer : serializers) {
			serializer.writeToken(new Date(), component.getId(), outputPort, tokenContent);
		}
	}

	/**
	 * Two tokens are linked with parent-child relationship.
	 */
	public synchronized void linkTokens(Node component, Token sourceToken, Token targetToken) {
		sourceTokenContent.setToken(sourceToken);
		targetTokenContent.setToken(targetToken);
		
		for (TokenTrackerSerializer serializer : serializers) {
			serializer.linkTokens(new Date(), component.getId(), sourceTokenContent, targetTokenContent);
		}
	}

	/**
	 * Token executes a job in a component.
	 * @param component component where the token executes a job
	 * @param token caused token
	 * @param jobType type of executed job
	 * @param runId run id of executed job
	 * @param settings custom settings of executed job
	 */
	public synchronized void executeJob(Node component, Token token, JobType jobType, RunStatus runStatus) {
		tokenContent.setToken(token);
		
		for (TokenTrackerSerializer serializer : serializers) {
			serializer.executeJob(new Date(), component.getId(), tokenContent, jobType, runStatus);
		}
	}

	/**
	 * Token reported a finished job in a component.
	 * @param component component where the token reports a job is finished
	 * @param token reporting token
	 * @param jobType type of finished job
	 * @param runId run id of finished job
	 * @param result custom information about finished job
	 */
	public synchronized void jobFinished(Node component, Token token, JobType jobType, RunStatus runStatus) {
		tokenContent.setToken(token);
		
		for (TokenTrackerSerializer serializer : serializers) {
			serializer.jobFinished(new Date(), component.getId(), tokenContent, jobType, runStatus);
		}
	}

	/**
	 * Token reported general message. 
	 * @param component component where the token reports the message
	 * @param token reporting token
	 * @param level log level of the message
	 * @param message reported message
	 * @param exception reported exception
	 */
	public synchronized void logMessage(Node component, Token token, Level level, String message, Throwable exception) {
		tokenContent.setToken(token);
		
		for (TokenTrackerSerializer serializer : serializers) {
			serializer.logMessage(new Date(), component.getId(), tokenContent, level, message, ExceptionUtils.stackTraceToString(exception));
		}
	}

	/**
	 * @return next token ID, sequence is shared with parent jobflow 
	 */
	public synchronized long nextTokenId() {
		if (graph.getRuntimeContext().isSubJob()) {
			return graph.getAuthorityProxy().getNextTokenIdFromParentJob();
		} else {
			return tokenSequence++; 
		}
	}
	
}
