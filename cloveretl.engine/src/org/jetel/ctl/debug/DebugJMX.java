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
package org.jetel.ctl.debug;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import org.jetel.ctl.debug.DebugCommand.CommandType;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.JMXNotificationMessage;
import org.jetel.graph.runtime.jmx.CloverJMX;

/**
 * A JMX bean for CTL debugging. It manages debugging of CTL threads - passes debugging
 * commands to threads and sends suspend notifications to JMX listeners.
 * 
 * This mbean just distributes the operation invocations to {@link GraphDebugger}s, which
 * are dedicated for single graph debugging.
 * 
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 13. 12. 2017
 */
public class DebugJMX extends NotificationBroadcasterSupport implements DebugJMXMBean {

	public static final String MBEAN_NAME = "org.jetel.ctl:type=DebugJMX";

	private static Map<Long, GraphDebugger> graphDebuggerCache = new ConcurrentHashMap<>();

	/** The only instance of DebugJMX	 */
	private static volatile DebugJMX debugJMX;
	
	/**
	 * Creates the singleton DebugJMX and registers it as JMX mBean.
	 */
	public static synchronized void registerMBean() {
		if (debugJMX == null) {
			debugJMX = new DebugJMX();
			//register JMX mBean
	    	try {
				ObjectName debugJmxObjectName = new ObjectName(MBEAN_NAME);
				ManagementFactory.getPlatformMBeanServer().registerMBean(debugJMX, debugJmxObjectName);
	        } catch (Exception e) {
	        	throw new JetelRuntimeException("DebugJMX mBean cannot be published.", e);
	        }
		}
	}
	
	/**
	 * @return the only instance of DebugJMX
	 */
	public static DebugJMX getInstance() {
		if (debugJMX == null) {
			throw new IllegalStateException("DebugJMX mBean is not published yet. Use DebugJMX.registerMBean() first.");
		}
		return debugJMX;
	}

	public static synchronized GraphDebugger getGraphDebugger(TransformationGraph graph) {
		GraphRuntimeContext runtimeContext = graph.getRuntimeContext();
		long runId = runtimeContext.getRunId();
		GraphDebugger graphDebugger = graphDebuggerCache.get(runId);
		if (graphDebugger == null) {
			graphDebugger = new GraphDebugger(runtimeContext);
			graphDebuggerCache.put(runId, graphDebugger);
		}
		return graphDebugger;
	}

	public static synchronized void freeGraphDebugger(TransformationGraph graph) {
		GraphDebugger graphDebugger = graphDebuggerCache.remove(graph.getRuntimeContext().getRunId());
		if (graphDebugger != null) {
			graphDebugger.free();
		}
	}

	@Override
	public Thread[] listCtlThreads(long runId) {
		return graphDebuggerCache.get(runId).listCtlThreads();
	}

	@Override
	public StackFrame[] getStackFrames(long runId, long threadId) {
		return graphDebuggerCache.get(runId).getStackFrames(threadId);
	}

	@Override
	public void resume(long runId, long threadId) {
		graphDebuggerCache.get(runId).resume(threadId);
	}

	@Override
	public void resumeAll(long runId) {
		graphDebuggerCache.get(runId).resumeAll();
	}

	@Override
	public void suspend(long runId, long threadId) {
		graphDebuggerCache.get(runId).suspend(threadId);
	}

	@Override
	public void suspendAll(long runId) {
		graphDebuggerCache.get(runId).suspendAll();
	}

	@Override
	public void addBreakpoints(long runId, List<Breakpoint> breakpoints) {
		graphDebuggerCache.get(runId).addBreakpoints(breakpoints);
	}

	@Override
	public void removeBreakpoints(long runId, List<Breakpoint> breakpoints) {
		graphDebuggerCache.get(runId).removeBreakpoints(breakpoints);
	}

	@Override
	public void modifyBreakpoint(long runId, Breakpoint breakpoint) {
		graphDebuggerCache.get(runId).modifyBreakpoint(breakpoint);
	}

	@Override
	public void setCtlBreakingEnabled(long runId, boolean enabled) {
		graphDebuggerCache.get(runId).setCtlBreakingEnabled(enabled);
	}

	@Override
	public void stepThread(long runId, long threadId, CommandType stepType) {
		graphDebuggerCache.get(runId).stepThread(threadId, stepType);
	}

	@Override
	public void runToLine(long runId, long threadId, RunToMark mark) {
		graphDebuggerCache.get(runId).runToLine(threadId, mark);
	}

	@Override
	public ListVariableResult listVariables(long runId, long threadId, int frameIndex, boolean includeGlobal) {
		return graphDebuggerCache.get(runId).listVariables(threadId, frameIndex, includeGlobal);
	}

	@Override
	public Object evaluateExpression(long runId, String expression, long threadId, int callStackIndex) throws Exception {
		return graphDebuggerCache.get(runId).evaluateExpression(expression, threadId, callStackIndex);
	}

	public synchronized void sendNotification(GraphDebugger sender, String type, Object userData) {
		Notification suspendNotification = new Notification(type, this, CloverJMX.getNotificationSequence());
		suspendNotification.setUserData(new JMXNotificationMessage(sender.getRunId(), userData));
		sendNotification(suspendNotification);
	}

}
