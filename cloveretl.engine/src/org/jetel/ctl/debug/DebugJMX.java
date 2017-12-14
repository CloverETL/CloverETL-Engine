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

import java.io.Serializable;
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

	public static String MBEAN_NAME = "org.jetel.ctl:type=DebugJMX";

	private int notificationSequence;

	private static Map<Long, GraphDebugger> graphDebuggerCache = new ConcurrentHashMap<>();

	private static DebugJMX debugJMX;
	
	public static synchronized DebugJMX getInstance() {
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
		Notification suspendNotification = new Notification(type, this, ++notificationSequence);
		suspendNotification.setUserData(new NotificationMessage(sender.getRunId(), userData));
		sendNotification(suspendNotification);
	}

	public static class NotificationMessage implements Serializable {
		private static final long serialVersionUID = 2445808779831669767L;
		
		private long runId;
		private Object userData;
		
		public NotificationMessage(long runId, Object userData) {
			this.runId = runId;
			this.userData = userData;
		}
		
		public long getRunId() {
			return runId;
		}

		public Object getUserData() {
			return userData;
		}
	}
	
}
