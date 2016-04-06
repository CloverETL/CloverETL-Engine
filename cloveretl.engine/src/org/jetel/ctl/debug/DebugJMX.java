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
	
	private Map<Long, CTLDebugThread> activeThreads;
	private Set<DebugTransformLangExecutor> executors;
	
	public DebugJMX() {
		activeThreads = new ConcurrentHashMap<Long, CTLDebugThread>();
		executors = new CopyOnWriteArraySet<>();
	}
	
	public void registerTransformLangExecutor(DebugTransformLangExecutor executor) {
		executors.add(executor);
	}
	
	public void registerCTLThread(Thread thread, DebugTransformLangExecutor executor) {
		activeThreads.put(Long.valueOf(thread.getId()), new CTLDebugThread(thread, executor));
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
		try {
			processCommand(threadId, new DebugCommand(CommandType.RESUME));
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
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
		
		List<CTLDebugThread> threads = new ArrayList<>(activeThreads.values());
		Thread[] result = new Thread[threads.size()];
		for (int i = 0; i < result.length; ++i) {
			result[i] = threads.get(i).getThread();
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
		for (CTLDebugThread thread : activeThreads.values()) {
			if (thread.getThread().isSuspended()) {
				try {
					thread.getExecutor().executeCommand(new DebugCommand(CommandType.RESUME));
				} catch (InterruptedException e) {
					logger.warn("Interrupted while resuming thread.", e);
				}
			}
		}
	}
	
	@Override
	public void addBreakpoints(Breakpoint[] breakpoints) {
		for (DebugTransformLangExecutor executor : executors) {
			DebugCommand cmd = new DebugCommand(CommandType.SET_BREAKPOINTS);
			cmd.setValue(breakpoints);
			try {
				executor.putCommand(cmd);
			} catch (InterruptedException e) {
				logger.warn("Interrupted while adding breakpoints.", e);
			}
		}
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
	
	public static String createMBeanName(String graphId, long runId) {
        return "org.jetel.ctl:type=DebugJMX_" + graphId + "_" + runId;
	}
	
	private DebugStatus processCommand(long threadId, DebugCommand command) throws InterruptedException {
		CTLDebugThread thread = activeThreads.get(threadId);
		if (thread != null) { 
			return thread.getExecutor().executeCommand(command);
		} else {
			logger.warn(String.format("CTL debug: Thread with id '%d' is not running.", threadId));
			return null;
		}
	}
}
