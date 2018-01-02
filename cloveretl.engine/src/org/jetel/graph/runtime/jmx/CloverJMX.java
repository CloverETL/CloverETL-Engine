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
package org.jetel.graph.runtime.jmx;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.JMXNotificationMessage;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.util.LogUtils;

/**
 * JMX managed bean implementation.
 *  
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 13, 2008
 */
public class CloverJMX extends NotificationBroadcasterSupport implements CloverJMXMBean, Serializable {

	private static final long serialVersionUID = 7993293097835091585L;
	
	public static final String MBEAN_NAME = "org.jetel.graph.runtime:type=CLOVERJMX";
	
	private static final Logger log = Logger.getLogger(CloverJMX.class);

	static final transient MemoryMXBean MEMORY_MXBEAN = ManagementFactory.getMemoryMXBean();
    static final transient ThreadMXBean THREAD_MXBEAN = ManagementFactory.getThreadMXBean();
    
    private static transient boolean isThreadCpuTimeSupported = THREAD_MXBEAN.isThreadCpuTimeSupported();

    /**
     * Cache for all currently running WatchDogs.
     */
    private static Map<Long, WatchDog> watchDogCache = new ConcurrentHashMap<>();

    private int notificationSequence;
    
    /**
     * The only instance of CloverJMX mBean.
     */
    private static volatile CloverJMX cloverJMX;
    
	/**
	 * Registers CloverJMX as JMX mBean.
	 */
	public static synchronized void registerMBean() {
		if (cloverJMX == null) {
			cloverJMX = new CloverJMX();
	    	try {
				ObjectName objectName = new ObjectName(MBEAN_NAME);
				ManagementFactory.getPlatformMBeanServer().registerMBean(cloverJMX, objectName);
	        } catch (Exception e) {
	        	throw new JetelRuntimeException("CloverJMX mBean cannot be published.", e);
	        }
		}
	}

	/**
	 * @return the singleton
	 */
	public static CloverJMX getInstance() {
		if (cloverJMX == null) {
			throw new IllegalStateException("CloverJMX mBean is not published yet. Use CloverJMX.registerMBean() first.");
		}
		return cloverJMX;
	}
    
	/**
	 * Registers a running watchdog. Each watchdog should register yourself to allow be monitored using this mBean.
	 */
	public void registerWatchDog(WatchDog watchDog) {
		GraphRuntimeContext runtimeContext = watchDog.getGraphRuntimeContext();
		long runId = runtimeContext.getRunId();
		if (!watchDogCache.containsKey(runId)) {
			log.debug("New running job registered in CloverJMX: " + runId);
			watchDogCache.put(runId, watchDog);
		} else {
			throw new IllegalStateException("WatchDog with runId=" + runId + " is already registered.");
		}
	}
	
	@Override
	public GraphTracking getGraphTracking(long runId) {
		MDC.put(LogUtils.MDC_RUNID_KEY, runId);
		try {
			return getWatchDog(runId).getGraphTracking();
		} finally {
			MDC.remove(LogUtils.MDC_RUNID_KEY);
		}
	}

	@Override
	public void abortGraphExecution(long runId) {
		MDC.put(LogUtils.MDC_RUNID_KEY, runId);
		try {
			getWatchDog(runId).abort();
		} finally {
			MDC.remove(LogUtils.MDC_RUNID_KEY);
		}
	}

	@Override
	public void abortGraphExecution(long runId, boolean waitForAbort) {
		MDC.put(LogUtils.MDC_RUNID_KEY, runId);
		try {
			getWatchDog(runId).abort(waitForAbort);
		} finally {
			MDC.remove(LogUtils.MDC_RUNID_KEY);
		}
	}

	@Override
	public void relaseJob(long runId) {
		MDC.put(LogUtils.MDC_RUNID_KEY, runId);
		try {
			WatchDog watchDog = watchDogCache.remove(runId);
			if (watchDog == null) {
				log.error("Unregister WatchDog failed for runId=" + runId);
			} else {
				log.debug("Finished job unregistered from CloverJMX: " + runId);
			}
		} finally {
			MDC.remove(LogUtils.MDC_RUNID_KEY);
		}
	}

	private WatchDog getWatchDog(long runId) {
		WatchDog watchDog = watchDogCache.get(runId);
		if (watchDog != null) {
			return watchDog;
		} else {
			throw new IllegalStateException("WatchDog does not found for runId=" + runId);
		}
	}
	
	//TODO should move to a utility class
	public static boolean isThreadCpuTimeSupported() {
		return isThreadCpuTimeSupported;
	}

	@Override
	public synchronized void setApprovedPhaseNumber(long runId, int approvedPhaseNumber) {
		MDC.put(LogUtils.MDC_RUNID_KEY, runId);
		try {
			getWatchDog(runId).setApprovedPhaseNumber(approvedPhaseNumber);
			notifyAll();
		} finally {
			MDC.remove(LogUtils.MDC_RUNID_KEY);
		}
	}

	public void sendNotification(long runId, String type) {
		sendNotification(runId, type, null);
	}

	public void sendNotification(long runId, String type, String message) {
		sendNotification(runId, type, message, null);
	}
	
	public void sendNotification(long runId, String type, String message, Object userData) {
		Notification notification = new Notification(type, this, notificationSequence++);
		notification.setUserData(new JMXNotificationMessage(runId, userData));
		sendNotification(notification);
	}
	
}
