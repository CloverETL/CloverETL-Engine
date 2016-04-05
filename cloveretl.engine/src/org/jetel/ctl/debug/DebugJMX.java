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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
    static Log logger = LogFactory.getLog(DebugJMX.class);
	
	private Map<Long, CTLDebugThread> activeThreads;
	private Set<DebugTransformLangExecutor> executors;
	
	public DebugJMX() {
		activeThreads = new ConcurrentHashMap<Long, CTLDebugThread>();
		executors = new CopyOnWriteArraySet<>();
	}
	
	public void registerTransformLangExecutor(DebugTransformLangExecutor executor) {
		executors.add(executor);
	}
	
	public void registerCTLThread(Thread thread, ArrayBlockingQueue<DebugCommand> commandQueue, ArrayBlockingQueue<DebugStatus> statusQueue) {
		activeThreads.put(thread.getId(), new CTLDebugThread(thread, commandQueue, statusQueue));
	}
	
	public void unregisterCTLDebugThread(Thread thread) {
		activeThreads.remove(thread.getId());
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
	
	private DebugStatus processCommand(long threadId, DebugCommand command) {
		CTLDebugThread thread = activeThreads.get(threadId);
		if (thread != null) { 
			thread.putCommand(command);
			DebugStatus status = thread.takeCommand();
			return status;
		} else {
			logger.warn(String.format("CTL debug: Thread with id '%d' is not running.", threadId));
			return null;
		}
	}
	
	private DebugStatus processCommand(DebugTransformLangExecutor executor, DebugCommand command) throws InterruptedException {
		
		executor.getCommandQueue().put(command);
		return executor.getStatusQueue().take();
	}
	
	@Override
	public synchronized void resume(long threadId) {
		DebugCommand dcommand = new DebugCommand(CommandType.RESUME);
		processCommand(threadId, dcommand);
	}
	
	@Override
	public synchronized void info(long threadId) {
		processCommand(threadId, new DebugCommand(CommandType.INFO));
	}
	
	@Override
	public Thread[] listCtlThreads() {
		Thread[] threads = new Thread[activeThreads.size()];
		int i = 0;
		for (CTLDebugThread threadInfo : activeThreads.values()) {
			threads[i++] = threadInfo.getThread();
		}
		return threads;
	}

	@Override
	public synchronized StackFrame[] getStackFrames(long threadId) {
		DebugCommand dcommand = new DebugCommand(CommandType.GET_CALLSTACK);
		DebugStatus status = processCommand(threadId, dcommand);
		if (status != null) {
			return (StackFrame[]) status.getValue();
		}
		return new StackFrame[0];
	}


	@Override
	public synchronized void resumeAll() {
		for (Long threadId : activeThreads.keySet()) {
			DebugCommand dcommand = new DebugCommand(CommandType.RESUME);
			processCommand(threadId, dcommand);
		}
	}
	
	@Override
	public void addBreakpoints(Breakpoint[] breakpoints) {
		for (DebugTransformLangExecutor executor : executors) {
			DebugCommand cmd = new DebugCommand(CommandType.SET_BREAKPOINTS);
			cmd.setValue(breakpoints);
			try {
				processCommand(executor, cmd);
			} catch (InterruptedException e) {
				logger.warn("Debug command interrupted", e);
			}
		}
	}
	
	public static String createMBeanName(String graphId, long runId) {
        return "org.jetel.ctl:type=DebugJMX_" + graphId + "_" + runId;
	}
}
