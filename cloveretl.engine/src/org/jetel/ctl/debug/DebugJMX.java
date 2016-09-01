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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.apache.log4j.Logger;
import org.jetel.ctl.DebugTransformLangExecutor;
import org.jetel.ctl.debug.DebugCommand.CommandType;
import org.jetel.ctl.debug.condition.BooleanExpressionCondition;
import org.jetel.ctl.debug.condition.HitCountCondition;
import org.jetel.ctl.debug.condition.ValueChangeCondition;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.jmx.CloverJMXMBean;
import org.jetel.util.CompareUtils;
import org.jetel.util.string.UnicodeBlanks;

/**
 * A JMX bean for CTL debugger. It manages debugging of CTL threads - passes debug commands to threads and sends suspend
 * notifications to JMX listeners.
 * 
 * @author Magdalena Malysz (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11. 3. 2016
 */
public class DebugJMX extends NotificationBroadcasterSupport implements DebugJMXMBean, IdSequence {
	
	private static final long EXPRESSION_TIMEOUT_MS = 20000;
	
	private int notificationSequence;
    static Logger logger = Logger.getLogger(DebugJMX.class);
	
    private GraphRuntimeContext runtimeContext;
	private Map<Long, ExecutorThread> activeThreads;
	private Set<DebugTransformLangExecutor> executors;
	private Map<Long, NodeThread> nodeThreads;
	
	private ObjectName jmxBeanName;
	private volatile NotificationListener finishListener;
	private final Object finishListenerLock = new Object();
	
	private long idSeq;
	
	public DebugJMX(GraphRuntimeContext runtimeContext) {
		this.runtimeContext = runtimeContext;
		nodeThreads = new ConcurrentHashMap<>();
		activeThreads = new ConcurrentHashMap<>();
		executors = new CopyOnWriteArraySet<>();
		/*
		 * set initial breakpoints conditions
		 */
		for (Breakpoint breakpoint : runtimeContext.getCtlBreakpoints()) {
			updateBreakpointCondition(breakpoint);
		}
	}
	
	public void registerTransformLangExecutor(DebugTransformLangExecutor executor) {
		executor.setIdSequence(this);
		executors.add(executor);
	}
	
	public void unregisterTransformLangExecutor(DebugTransformLangExecutor executor) {
		executors.remove(executor);
	}
	
	/**
	 * Registers new thread with CTL execution.
	 * @param thread
	 * @param executor
	 * @return <code>true</code> if the registered thread should be suspended at once
	 */
	public boolean registerCTLThread(Thread thread, DebugTransformLangExecutor executor) {
		
		Long threadId = Long.valueOf(thread.getId());
		NodeThread nodeThread = nodeThreads.get(threadId);
		boolean suspendIt = false;
		if (nodeThread == null) {
			Node node = ContextProvider.getNode();
			if (node != null && Result.RUNNING.equals(node.getResultCode())) {
				ensureJmxListenerPresent(node);
				nodeThread = new NodeThread(node, thread.getJavaThread());
				nodeThreads.put(threadId, nodeThread);
				notifyThreadStart(threadId);
			}
		} else {
			if (nodeThread.isSuspend()) {
				suspendIt = true;
				nodeThread.setSuspend(false);
			}
		}
		activeThreads.put(threadId, new ExecutorThread(thread, executor));
		return suspendIt;
	}
	
	public void unregisterCTLThread(Thread thread) {
		activeThreads.remove(Long.valueOf(thread.getId()));
	}
	
	public void notifySuspend(DebugStatus status) {
		Notification suspendNotification = new Notification(THREAD_SUSPENDED, this, ++notificationSequence);
		suspendNotification.setUserData(status);
		sendNotification(suspendNotification);
	}
	
	public void notifyResume(DebugStatus status) {
		Notification notification = new Notification(THREAD_RESUMED, this, ++notificationSequence);
		notification.setUserData(status);
		sendNotification(notification);
	}
	
	public void notifyInit() {
		Notification notification = new Notification(JOB_INIT, this, ++notificationSequence);
		sendNotification(notification);
	}
	
	public void notifyThreadStart(Long threadId) {
		Notification notification = new Notification(THREAD_STARTED, this, ++notificationSequence);
		notification.setUserData(threadId);
		sendNotification(notification);
	}
	
	public void notifyThreadStop(Long threadId) {
		Notification notification = new Notification(THREAD_STOPPED, this, ++notificationSequence);
		notification.setUserData(threadId);
		sendNotification(notification);
	}
	
	public void notifyConditionError(Breakpoint breakpoint, String errorMessage) {
		Notification notification = new Notification(BP_CONDITION_ERROR, this, ++notificationSequence);
		notification.setUserData(new BreakpointConditionError(breakpoint, errorMessage));
		sendNotification(notification);
	}
	
	@Override
	public synchronized long nextId() {
		return ++idSeq;
	}
	
	@Override
	public void resume(long threadId) {
		for (ExecutorThread thread : activeThreads.values()) {
			if (thread.getCTLThread().getId() == threadId) {
				thread.getExecutor().resume();
			}
		}
		for (NodeThread thread : nodeThreads.values()) {
			if (thread.getJavaThread().getId() == threadId) {
				thread.setSuspend(false);
			}
		}
		try {
			processCommand(threadId, new DebugCommand(CommandType.RESUME));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void suspend(long threadId) {
		
		ExecutorThread active = activeThreads.get(Long.valueOf(threadId));
		if (active != null && !active.getCTLThread().isSuspended()) {
			active.getExecutor().suspend();
			return;
		}
		NodeThread node = nodeThreads.get(Long.valueOf(threadId));
		if (node != null && !node.isSuspend()) {
			node.setSuspend(true);
		}
	}
	
	@Override
	public Thread[] listCtlThreads() {
		
		Map<Long, ExecutorThread> active = new HashMap<>(activeThreads);
		Map<Long, NodeThread> nodes = new HashMap<>(nodeThreads);
		
		List<Thread> threads = new ArrayList<>(nodes.size());
		
		for (ExecutorThread thread : active.values()) {
			threads.add(thread.getCTLThread());
		}
		/*
		 * add threads that do not execute CTL currently
		 */
		for (Entry<Long, NodeThread> e : nodes.entrySet()) {
			if (!active.containsKey(e.getKey())) {
				Thread ctlThread = new Thread();
				ctlThread.setJavaThread(e.getValue().getJavaThread());
				threads.add(ctlThread);
			}
		}
		Thread result[] = threads.toArray(new Thread[threads.size()]);
		
		Arrays.sort(result, new Comparator<Thread>() {
			@Override
			public int compare(Thread t1, Thread t2) {
				return CompareUtils.compare(t1.getName(), t2.getName());
			}
		});
		return result;
	}

	@Override
	public StackFrame[] getStackFrames(long threadId) {
		try {
			DebugStatus status = processCommand(threadId, new DebugCommand(CommandType.GET_CALLSTACK));
			return status != null ? (StackFrame[])status.getValue() : new StackFrame[0];
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


	@Override
	public void resumeAll() {
		runtimeContext.setSuspendThreads(false);
		for (ExecutorThread thread : activeThreads.values()) {
			if (thread.getCTLThread().isSuspended()) {
				try {
					thread.getExecutor().executeCommand(new DebugCommand(CommandType.RESUME));
				} catch (InterruptedException e) {
					logger.warn("Interrupted while resuming thread.", e);
				}
			}
		}
	}
	
	@Override
	public void suspendAll() {
		runtimeContext.setSuspendThreads(true);
		for (DebugTransformLangExecutor executor : executors) {
			executor.suspend();
		}
	}
	
	@Override
	public void addBreakpoints(List<Breakpoint> breakpoints) {
		runtimeContext.getCtlBreakpoints().addAll(breakpoints);
	}
	
	@Override
	public void removeBreakpoints(List<Breakpoint> breakpoints) {
		runtimeContext.getCtlBreakpoints().removeAll(breakpoints);
	}
	
	@Override
	public void modifyBreakpoint(Breakpoint breakpoint) {
		for (Breakpoint bp : runtimeContext.getCtlBreakpoints()) {
			if (bp.equals(breakpoint)) {
				bp.setEnabled(breakpoint.isEnabled());
				bp.setHitCount(breakpoint.getHitCount());
				bp.setExpression(breakpoint.getExpression());
				bp.setValueChange(breakpoint.isValueChange());
				updateBreakpointCondition(bp);
			}
		}
	}

	@Override
	public void setCtlBreakingEnabled(boolean enabled) {
		runtimeContext.setCtlBreakingEnabled(enabled);
	}

	@Override
	public void stepThread(long threadId, CommandType stepType) {
		try {
			if (stepType != CommandType.STEP_IN && stepType != CommandType.STEP_OUT && stepType != CommandType.STEP_OVER) {
				throw new IllegalArgumentException("Invalid step type " + stepType);
			}
			processCommand(threadId, new DebugCommand(stepType));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void runToLine(long threadId, RunToMark mark) {
		DebugCommand command = new DebugCommand(CommandType.RUN_TO_LINE);
		command.setValue(mark);
		try {
			processCommand(threadId, command);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public ListVariableResult listVariables(long threadId, int frameIndex, boolean includeGlobal) {
		DebugCommand command = new DebugCommand(CommandType.LIST_VARS);
		ListVariableOptions options = new ListVariableOptions();
		options.setFrameIndex(frameIndex);
		options.setIncludeGlobal(includeGlobal);
		command.setValue(options);
		try {
			DebugStatus status = processCommand(threadId, command);
			if (status != null && status.getValue() instanceof ListVariableResult) {
				return (ListVariableResult) status.getValue();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return new ListVariableResult(new ArrayList<Variable>(), new ArrayList<Variable>());
	}
	
	@Override
	public Object evaluateExpression(String expression, long threadId, int callStackIndex) throws Exception {
		DebugCommand command = new DebugCommand(CommandType.EVALUATE_EXPRESSION);
		CTLExpression exp = new CTLExpression(expression, callStackIndex);
		exp.setTimeout(EXPRESSION_TIMEOUT_MS); // CLO-9391
		command.setValue(exp);
		return processCommand(threadId, command).getValue();
	}
	
	public void free() {
		/*
		 * When a job is killed while being executed, the threads are interrupted and debug
		 * executors throw an exception, so their threads are removed from active threads.
		 * But should the thread executing CTL be not interrupted (this happens with threads in the initialization phase),
		 * it would hang in executor forever - so let's interrupt those threads while freeing.
		 */
		for (ExecutorThread thread : activeThreads.values()) {
			logger.info("Detected active debugged thread " + thread.getCTLThread() + ", interrupting...");
			thread.getCTLThread().getJavaThread().interrupt();
		}
		synchronized (finishListenerLock) {
			if (finishListener != null) {
				try {
					MBeanServer server = ManagementFactory.getPlatformMBeanServer();
					if (server.isRegistered(jmxBeanName)) {
						server.removeNotificationListener(jmxBeanName, finishListener);
					}
					finishListener = null;
				} catch (ListenerNotFoundException | InstanceNotFoundException e) {
					logger.warn("Could not remove node finish listener.", e);
				}
			}
		}
		activeThreads.clear();
		nodeThreads.clear();
		executors.clear();
	}
	
	public static String createMBeanName(String graphId, long runId) {
        return "org.jetel.ctl:type=DebugJMX_" + graphId + "_" + runId;
	}
	
	private DebugStatus processCommand(long threadId, DebugCommand command) throws Exception {
		ExecutorThread thread = activeThreads.get(threadId);
		if (thread != null) { 
			DebugStatus debugStatus = thread.getExecutor().executeCommand(command);
			if (debugStatus.getException() != null) {
				throw debugStatus.getException();
			}
			return debugStatus;
		} else {
			logger.warn(String.format("CTL debug: Thread with id '%d' is not running.", threadId));
			return null;
		}
	}
	
	private void ensureJmxListenerPresent(Node node) {
		synchronized (finishListenerLock) {
			if (finishListener == null) {
				try {
					FinishNotificationListener listener = new FinishNotificationListener();
					jmxBeanName = node.getGraph().getWatchDog().getCloverJmxName();
					ManagementFactory.getPlatformMBeanServer().addNotificationListener(
						jmxBeanName, listener, new FinishNotificationFilter(), Long.valueOf(runtimeContext.getRunId()));
					finishListener = listener;
				} catch (InstanceNotFoundException e) {
					throw new JetelRuntimeException(e);
				}
			}
		}
	}
	
	private void updateBreakpointCondition(Breakpoint breakpoint) {
		breakpoint.setCondition(null);
		if (breakpoint.getHitCount() > 0) {
			breakpoint.setCondition(new HitCountCondition(breakpoint.getHitCount()));
		} else if (!UnicodeBlanks.isBlank(breakpoint.getExpression())) {
			if (breakpoint.isValueChange()) {
				breakpoint.setCondition(new ValueChangeCondition(breakpoint.getExpression()));
			} else {
				breakpoint.setCondition(new BooleanExpressionCondition(breakpoint.getExpression()));
			}
		}
	}
	
	private void handleNodeFinished(String nodeId) {
		for (Iterator<Entry<Long, NodeThread>> it = nodeThreads.entrySet().iterator(); it.hasNext();) {
			Entry<Long, NodeThread> entry = it.next();
			if (entry.getValue().getNode().getId().equals(nodeId)) {
				it.remove();
				notifyThreadStop(entry.getKey());
			}
		}
	}
	
	protected static class FinishNotificationFilter implements NotificationFilter {
		
		private static final long serialVersionUID = 1L;
		
		private static final List<String> RELEVANT_EVENTS = Collections.unmodifiableList(Arrays.asList(
			CloverJMXMBean.NODE_FINISHED, CloverJMXMBean.PHASE_FINISHED));
		
		@Override
		public boolean isNotificationEnabled(Notification notification) {
			return RELEVANT_EVENTS.contains(notification.getType());
		}
	}
	
	protected class FinishNotificationListener implements NotificationListener {
		
		@Override
		public void handleNotification(Notification notification, Object handback) {
			Long runId = null;
			if (handback instanceof Long) {
				runId = (Long)handback;
			}
			if (runId == null || runId.longValue() != runtimeContext.getRunId()) {
				return;
			}
			if (CloverJMXMBean.NODE_FINISHED.equals(notification.getType())) {
				handleNodeFinished(notification.getMessage());
			}
		}
	}

}
