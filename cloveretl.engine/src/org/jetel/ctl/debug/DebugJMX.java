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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.apache.log4j.Logger;
import org.jetel.ctl.DebugTransformLangExecutor;
import org.jetel.ctl.debug.DebugCommand.CommandType;
import org.jetel.graph.runtime.GraphRuntimeContext;

/**
 * A JMX bean for CTL debugger. It manages debugging of CTL threads - passes debug commands to threads and sends suspend
 * notifications to JMX listeners.
 * 
 * @author Magdalena Malysz (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11. 3. 2016
 */
public class DebugJMX extends NotificationBroadcasterSupport implements DebugJMXMBean {
	
	private int notificationSequence;
    static Logger logger = Logger.getLogger(DebugJMX.class);
	
    private GraphRuntimeContext runtimeContext;
	private Map<Long, ExecutorThread> activeThreads;
	private Set<DebugTransformLangExecutor> executors;
	
	public DebugJMX(GraphRuntimeContext runtimeContext) {
		this.runtimeContext = runtimeContext;
		activeThreads = new ConcurrentHashMap<Long, ExecutorThread>();
		executors = new CopyOnWriteArraySet<>();
	}
	
	public void registerTransformLangExecutor(DebugTransformLangExecutor executor) {
		executors.add(executor);
	}
	
	public void unregisterTransformLangExecutor(DebugTransformLangExecutor executor) {
		executors.remove(executor);
	}
	
	public void registerCTLThread(Thread thread, DebugTransformLangExecutor executor) {
		activeThreads.put(Long.valueOf(thread.getId()), new ExecutorThread(thread, executor));
	}
	
	public void unregisterCTLDebugThread(Thread thread) {
		activeThreads.remove(Long.valueOf(thread.getId()));
	}
	
	public void notifySuspend(DebugStatus status) {
		Notification suspendNotification = new Notification(DebugJMX.THREAD_SUSPENDED, this, notificationSequence++);
		suspendNotification.setUserData(status);
		sendNotification(suspendNotification);
	}
	
	public void notifyResumed(DebugStatus status) {
		Notification notification = new Notification(DebugJMX.THREAD_RESUMED, this, notificationSequence++);
		notification.setUserData(status);
		sendNotification(notification);
	}
	
	public void notifyInit() {
		Notification notification = new Notification(DebugJMX.JOB_INIT, this, notificationSequence++);
		sendNotification(notification);
	}
	
	@Override
	public void resume(long threadId) {
		for (ExecutorThread thread : activeThreads.values()) {
			if (thread.getCTLThread().getId() == threadId) {
				thread.getExecutor().resume();
			}
		}
		try {
			processCommand(threadId, new DebugCommand(CommandType.RESUME));
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void suspend(long threadId) {
		for (ExecutorThread thread : activeThreads.values()) {
			if (thread.getCTLThread().getId() == threadId && !thread.getCTLThread().isSuspended()) {
				thread.getExecutor().suspend();
			}
		}
	}
	
	@Override
	public void info(long threadId) {
		try {
			processCommand(threadId, new DebugCommand(CommandType.INFO));
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public Thread[] listCtlThreads() {
		
		List<ExecutorThread> threads = new ArrayList<>(activeThreads.values());
		Thread[] result = new Thread[threads.size()];
		for (int i = 0; i < result.length; ++i) {
			result[i] = threads.get(i).getCTLThread();
		}
		return result;
	}

	@Override
	public StackFrame[] getStackFrames(long threadId) {
		try {
			DebugStatus status = processCommand(threadId, new DebugCommand(CommandType.GET_CALLSTACK));
			return status != null ? (StackFrame[])status.getValue() : new StackFrame[0];
		} catch (InterruptedException e) {
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
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void runToLine(long threadId, RunToMark mark) {
		DebugCommand command = new DebugCommand(CommandType.RUN_TO_LINE);
		command.setValue(mark);
		try {
			processCommand(threadId, command);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
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
		activeThreads.clear();
		executors.clear();
	}
	
	public static String createMBeanName(String graphId, long runId) {
        return "org.jetel.ctl:type=DebugJMX_" + graphId + "_" + runId;
	}
	
	private DebugStatus processCommand(long threadId, DebugCommand command) throws InterruptedException {
		ExecutorThread thread = activeThreads.get(threadId);
		if (thread != null) { 
			return thread.getExecutor().executeCommand(command);
		} else {
			logger.warn(String.format("CTL debug: Thread with id '%d' is not running.", threadId));
			return null;
		}
	}
}
