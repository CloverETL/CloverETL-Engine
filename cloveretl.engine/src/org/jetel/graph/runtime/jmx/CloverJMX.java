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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.dictionary.DictionaryValuesContainer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.JMXNotificationMessage;
import org.jetel.graph.runtime.JobListener;
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
	
	/**
	 * Default time after which a finished job is automatically removed from cache of running jobs. 
	 */
	private static final long DEFAULT_OBSOLETE_JOB_TIMEOUT = 10 * 1000; // 10s
	
	private static final Logger log = Logger.getLogger(CloverJMX.class);

	static final transient MemoryMXBean MEMORY_MXBEAN = ManagementFactory.getMemoryMXBean();
    static final transient ThreadMXBean THREAD_MXBEAN = ManagementFactory.getThreadMXBean();
    
    private static transient boolean isThreadCpuTimeSupported = THREAD_MXBEAN.isThreadCpuTimeSupported();

    /**
     * Cache for all currently running WatchDogs.
     */
    private transient Map<Long, WatchDog> watchDogCache = new ConcurrentHashMap<>();

    private long notificationSequence;
    
    /**
     * Obsolete timeout can be changed due junit tests.
     */
    private long obsoleteJobTimeout = DEFAULT_OBSOLETE_JOB_TIMEOUT;
    
    /**
     * The only instance of CloverJMX mBean.
     */
    private static volatile CloverJMX cloverJMX;
    
    /**
     * List of listeners, which are interested about the Watchdog is release by an authority.
     */
    private transient List<JobListener> jobListeners = new ArrayList<>();
    
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
		//first clean old watchdogs, which seems to be forgot
		cleanObsoleteWatchDogs();
		
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
		Object oldRunId = MDC.get(LogUtils.MDC_RUNID_KEY);
		MDC.put(LogUtils.MDC_RUNID_KEY, runId);
		try {
			return getWatchDog(runId).getGraphTracking();
		} finally {
			if (oldRunId == null) {
				MDC.remove(LogUtils.MDC_RUNID_KEY);
			} else {
				MDC.put(LogUtils.MDC_RUNID_KEY, oldRunId);
			}
		}
	}

	@Override
	public boolean abortGraphExecution(long runId) {
		return abortGraphExecution(runId, false);
	}

	@Override
	public boolean abortGraphExecution(long runId, boolean waitForAbort) {
		Object oldRunId = MDC.get(LogUtils.MDC_RUNID_KEY);
		MDC.put(LogUtils.MDC_RUNID_KEY, runId);
		try {
			WatchDog watchDog = watchDogCache.get(runId);
			if (watchDog != null) {
				watchDog.abort(waitForAbort);
				return true;
			}
			return false;
		} finally {
			if (oldRunId == null) {
				MDC.remove(LogUtils.MDC_RUNID_KEY);
			} else {
				MDC.put(LogUtils.MDC_RUNID_KEY, oldRunId);
			}
		}
	}

	@Override
	public void relaseJob(long runId) {
		Object oldRunId = MDC.get(LogUtils.MDC_RUNID_KEY);
		MDC.put(LogUtils.MDC_RUNID_KEY, runId);
		try {
			WatchDog watchDog = watchDogCache.remove(runId);
			if (watchDog == null) {
				log.error("Released WatchDog does not found for runId #" + runId);
			} else {
				sendReleaseWatchdogNotification(watchDog);
				log.debug("WatchDog unregistered from CloverJMX #" + runId);
			}
		} finally {
			if (oldRunId == null) {
				MDC.remove(LogUtils.MDC_RUNID_KEY);
			} else {
				MDC.put(LogUtils.MDC_RUNID_KEY, oldRunId);
			}
		}
	}

	private void sendReleaseWatchdogNotification(WatchDog watchDog) {
		synchronized (jobListeners) {
			for (JobListener jobListener : jobListeners) {
				jobListener.jobFinished(watchDog);
			}
		}
	}
	
	/**
	 * @return {@link WatchDog} for the given runId or null if no {@link WatchDog} is registered
	 */
	public WatchDog getRunningGraph(long runId) {
		return watchDogCache.get(runId);
	}

	private WatchDog getWatchDog(long runId) {
		WatchDog watchDog = watchDogCache.get(runId);
		if (watchDog != null) {
			return watchDog;
		} else {
			throw new JetelRuntimeException("WatchDog not found for runId=" + runId);
		}
	}
	
	//TODO should move to a utility class
	public static boolean isThreadCpuTimeSupported() {
		return isThreadCpuTimeSupported;
	}

	@Override
	public synchronized void setApprovedPhaseNumber(long runId, int approvedPhaseNumber, DictionaryValuesContainer mergedDictionary) {
		Object oldRunId = MDC.get(LogUtils.MDC_RUNID_KEY);
		MDC.put(LogUtils.MDC_RUNID_KEY, runId);
		try {
			
			WatchDog watchDog = getWatchDog(runId);
			setDictionary(watchDog, mergedDictionary);
			watchDog.setApprovedPhaseNumber(approvedPhaseNumber);
			
			notifyAll();
		} finally {
			if (oldRunId == null) {
				MDC.remove(LogUtils.MDC_RUNID_KEY);
			} else {
				MDC.put(LogUtils.MDC_RUNID_KEY, oldRunId);
			}
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
	
	/**
	 * Removes all finished jobs older than 10s from watchDogCache.
	 */
	private synchronized void cleanObsoleteWatchDogs() {
		List<Long> watchdogsToRelease = new ArrayList<>();
		long currentTime = System.currentTimeMillis();
		for (Iterator<Map.Entry<Long, WatchDog>> iterator = watchDogCache.entrySet().iterator(); iterator.hasNext(); ) {
			Entry<Long, WatchDog> entry = iterator.next();
			WatchDog watchDog = entry.getValue();
			GraphTrackingDetail tracking = watchDog.getGraphTracking();
			if (tracking.getResult().isStop()
					&& tracking.getEndTime() + getObsoleteJobTimeout() < currentTime) {
				watchdogsToRelease.add(entry.getKey());
			}
		}
		
		for (Long runId : watchdogsToRelease) {
			WatchDog watchDog = watchDogCache.remove(runId);
			if (watchDog != null) {
				log.warn("Obsolete WatchDog has been removed from CloverJMX cache of running jobs with runId=" + runId);
				sendReleaseWatchdogNotification(watchDog);
			}
		}
	}
	
	public long getObsoleteJobTimeout() {
		return obsoleteJobTimeout;
	}
	
	public void setObsoleteJobTimeout(long obsoleteJobTimeout) {
		this.obsoleteJobTimeout = obsoleteJobTimeout;
	}
	
	private void setDictionary(WatchDog watchDog, DictionaryValuesContainer mergedDictionary) {
		try {
			DictionaryValuesContainer.setModifiedValues(watchDog.getGraph().getDictionary(), mergedDictionary);
		} catch (Exception e) {
			log.error("Can't merge dictionary " + watchDog.getGraphRuntimeContext().getRunId() + " "+mergedDictionary, e);
		}
	}
	
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        watchDogCache = new ConcurrentHashMap<>();
        jobListeners = new ArrayList<>();
    }

	/**
	 * @return number of just now running jobs
	 */
	public int getNumOfRunningJobs() {
		return watchDogCache.size();
	}
	
	public void addJobListener(JobListener jobListener) {
		synchronized (jobListener) {
			jobListeners.add(jobListener);
		}
	}
	
}
