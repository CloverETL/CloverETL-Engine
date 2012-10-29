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
import java.util.List;

import org.apache.log4j.Level;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.DataRecordNature;
import org.jetel.data.Token;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.graph.JobType;
import org.jetel.graph.JobflowEdge;
import org.jetel.graph.Node;
import org.jetel.graph.Phase;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IAuthorityProxy.RunStatus;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.junit.Before;

/**
 * @author tkramolis (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 31.5.2012
 */
public abstract class AbstractComponentTokenTrackerTestBase extends CloverTestCase {

	protected ComponentTokenTracker componentTokenTracker;
	protected TestTokenTracker tokenTracker;
	private TransformationGraph graphMock;
	protected Node testComponent;
	protected Node dummyComponent = new Node("DummyProducer") {

		@Override
		public String getType() {
			return "NONE";
		}

		@Override
		protected Result execute() throws Exception {
			return null;
		}
	};

	@Override
	@Before
	protected void setUp() throws Exception {
		initEngine();

		DataRecordMetadata metadata = new DataRecordMetadata("test");
		metadata.addField(new DataFieldMetadata("f1", "|"));
		metadata.setNature(DataRecordNature.TOKEN);

		graphMock = createGraphMock();
		tokenTracker = new TestTokenTracker(graphMock);
		testComponent = new ComponentMock(tokenTracker, graphMock);
		
		Phase phase = new Phase(0);
		graphMock.addPhase(phase);
		phase.addNode(dummyComponent);
		phase.addNode(testComponent);

		componentTokenTracker = createComponentTokenTracker(testComponent);
	}

	protected abstract ComponentTokenTracker createComponentTokenTracker(Node testComponent);

	private TransformationGraph createGraphMock() throws GraphConfigurationException {
		TransformationGraph dummyGraph = new TransformationGraph();
		WatchDog watchDog = new WatchDog(dummyGraph, new GraphRuntimeContext()) {
			@Override
			public TokenTracker getTokenTracker() {
				return tokenTracker;
			}
		};
		dummyGraph.setWatchDog(watchDog);
		return dummyGraph;
	}
	
	protected void readToken(int port, Token inToken) {
		componentTokenTracker.readToken(port, inToken);
	}

	protected void assertReadToken(Token lastInToken, Token currentInToken, int currentInPort, int actionsCountBefore) {
		if (lastInToken != null) {
			tokenTracker.assertLastActions(
					actionsCountBefore,
					new TokenAction(TokenActionType.READ, testComponent, currentInPort, currentInToken),
					new TokenAction(TokenActionType.FREE, testComponent, lastInToken)
			);
		} else {
			tokenTracker.assertLastActions(
					actionsCountBefore,
					new TokenAction(TokenActionType.READ, testComponent, currentInPort, currentInToken)
			);
		}
	}

	protected Token writeToken(int outPort) {
		Token outToken = DataRecordFactory.newToken("outToken");
		componentTokenTracker.writeToken(outPort, outToken);
		return outToken;
	}

	protected Token writeToken(int outPort, Token outToken) {
		componentTokenTracker.writeToken(outPort, outToken);
		return outToken;
	}
	
	protected void assertInitLinkWrite(Token inToken, int outPort, Token outToken) {
		tokenTracker.assertLastActions(new TokenAction(TokenActionType.INIT, testComponent, outToken), new TokenAction(TokenActionType.LINK, testComponent, inToken, outToken), new TokenAction(TokenActionType.WRITE, testComponent, outPort, outToken));
	}

	protected void inEdgesEof(Token lastReadToken) {
		int actionsCount = tokenTracker.getActionsCount();

		for (int i = 0; i < ComponentMock.PORTS_COUNT - 1; i++) {
			componentTokenTracker.eofInputPort(i);
		}
		assertEquals(actionsCount, tokenTracker.getActionsCount());

		componentTokenTracker.eofInputPort(ComponentMock.PORTS_COUNT);
		assertActionsAfterInEdgesEof(lastReadToken, actionsCount);
	}

	protected void assertActionsAfterInEdgesEof(Token lastReadToken, int actionsCountBeforeFinish) {
		tokenTracker.assertLastActions(new TokenAction(TokenActionType.FREE, testComponent, lastReadToken));
	}

	protected Token createAndReadRecord(Token lastReadToken, int inPort) {
		Token inToken = DataRecordFactory.newToken("in0");
		tokenTracker.initToken(dummyComponent, inToken);
		int actionsCountBefore = tokenTracker.getActionsCount();
		readToken(inPort, inToken);
		assertReadToken(lastReadToken, inToken, inPort, actionsCountBefore);
		return inToken;
	}

	protected static class TokenAction {
		private TokenActionType type;
		private Node component;
		private int port = -1;
		private Token sourceToken;
		private Token targetToken;

		public TokenAction(TokenActionType type, Node component, Token sourceToken) {
			this(type, component, -1, sourceToken);
		}

		public TokenAction(TokenActionType type, Node component, int port, Token sourceToken) {
			this.type = type;
			this.component = component;
			this.port = port;
			this.sourceToken = sourceToken;
		}

		public TokenAction(TokenActionType type, Node component, Token sourceToken, Token targetToken) {
			this.type = type;
			this.component = component;
			this.sourceToken = sourceToken;
			this.targetToken = targetToken;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof TokenAction)) {
				return false;
			}
			TokenAction action = (TokenAction) obj;
			return type == action.type && component == action.component && port == action.port && sourceToken.getTokenId() == action.sourceToken.getTokenId() && (targetToken == action.targetToken || (targetToken != null && action.targetToken != null && targetToken.getTokenId() == action.targetToken.getTokenId()));
		}

		@Override
		public String toString() {
			return "\n" + type.toString() + ", " + component.getId() + ", " + port + ", src: " + sourceToken.getTokenId() + ", tgt: " + (targetToken == null ? "none" : targetToken.getTokenId());
		}
	}

	protected static class ComponentMock extends Node {

		private static final String TYPE = "ComponentMock";

		public static final int PORTS_COUNT = 4;

		public ComponentMock(TokenTracker tokenTracker, TransformationGraph graph) throws GraphConfigurationException {
			super(TYPE, graph);

			for (int i = 0; i < PORTS_COUNT; i++) {
				addInputPort(i, new JobflowEdge("InEdge" + i, (DataRecordMetadata) null));
			}
			for (int i = 0; i < PORTS_COUNT; i++) {
				addOutputPort(i, new JobflowEdge("OutEdge" + i, (DataRecordMetadata) null));
			}
		}

		@Override
		public String getType() {
			return TYPE;
		}

		@Override
		protected Result execute() throws Exception {
			// don't do anything -- component behavior is simulated directly on ComponentTokenTracker
			return Result.FINISHED_OK;
		}

	}

	protected enum TokenActionType {
		INIT, FREE, READ, WRITE, LINK, EXECUTE_JOB, JOB_FINISHED
	}

	protected static class TestTokenTracker extends TokenTracker {

		public TestTokenTracker(TransformationGraph graph) {
			super(graph);
		}

		List<TokenAction> actions = new ArrayList<TokenAction>();

		@Override
		public synchronized void initToken(Node component, Token token) {
			super.initToken(component, token);
			actions.add(new TokenAction(TokenActionType.INIT, component, token));
		}

		@Override
		public synchronized void freeToken(Node component, Token token) {
			super.freeToken(component, token);
			actions.add(new TokenAction(TokenActionType.FREE, component, token));
		}

		@Override
		public synchronized void readToken(Node component, int inputPort, Token token) {
			super.readToken(component, inputPort, token);
			actions.add(new TokenAction(TokenActionType.READ, component, inputPort, token));
		}

		@Override
		public synchronized void writeToken(Node component, int outputPort, Token token) {
			super.writeToken(component, outputPort, token);
			actions.add(new TokenAction(TokenActionType.WRITE, component, outputPort, token));
		}

		@Override
		public synchronized void linkTokens(Node component, Token sourceToken, Token targetToken) {
			super.linkTokens(component, sourceToken, targetToken);
			actions.add(new TokenAction(TokenActionType.LINK, component, sourceToken, targetToken));
		}

		@Override
		public synchronized void executeJob(Node component, Token token, JobType jobType, RunStatus runStatus) {
			super.executeJob(component, token, jobType, runStatus);
			actions.add(new TokenAction(TokenActionType.EXECUTE_JOB, component, token));
		}

		@Override
		public synchronized void jobFinished(Node component, Token token, JobType jobType, RunStatus runStatus) {
			super.jobFinished(component, token, jobType, runStatus);
			actions.add(new TokenAction(TokenActionType.JOB_FINISHED, component, token));
		}

		@Override
		public synchronized void logMessage(Node component, Token token, Level level, String message,
				Throwable exception) {
			super.logMessage(component, token, level, message, exception);
		}

		public void assertLastActions(TokenAction... tokenActions) {
			assertLastActions(-1, tokenActions);
		}

		public void assertLastActions(int actionsCountBefore, TokenAction... tokenActions) {
			for (int i = tokenActions.length - 1; i >= 0; i--) {
				assertEquals(tokenActions[i], actions.get(actions.size() - tokenActions.length + i));
			}
			if (actionsCountBefore > -1) {
				assertEquals(actionsCountBefore + tokenActions.length, getActionsCount());
			}
		}

		public int getActionsCount() {
			return actions.size();
		}
	}

}