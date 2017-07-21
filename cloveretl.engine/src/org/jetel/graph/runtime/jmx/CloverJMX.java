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

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.jetel.graph.Phase;
import org.jetel.graph.runtime.WatchDog;

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

	transient static final MemoryMXBean MEMORY_MXBEAN = ManagementFactory.getMemoryMXBean();
    transient static final ThreadMXBean THREAD_MXBEAN = ManagementFactory.getThreadMXBean();
    
    transient private static boolean isThreadCpuTimeSupported = THREAD_MXBEAN.isThreadCpuTimeSupported();

	private final transient WatchDog watchDog;

	private final GraphTrackingDetail graphDetail;

    private boolean canClose = false;

    private int notificationSequence;
    
    private boolean graphFinished = false;
    
    private volatile int approvedPhaseNumber = Integer.MIN_VALUE;
    
    /**
	 * Constructor.
     * @param watchDog 
	 */
	public CloverJMX(WatchDog watchDog) {
		this.watchDog = watchDog;
		this.graphDetail = new GraphTrackingDetail(watchDog.getGraph());
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.CloverJMXMBean#getCloverVersion()
	 */
	@Override
	public String getCloverVersion() {
		// TODO Auto-generated method stub
		return "<unknown clover engine version>";
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.CloverJMXMBean#getGraphTracking()
	 */
	@Override
	public GraphTracking getGraphTracking() {
		return graphDetail;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.CloverJMXMBean#abortGraphExecution()
	 */
	@Override
	public void abortGraphExecution() {
		watchDog.abort();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.CloverJMXMBean#closeServer()
	 */
	@Override
	synchronized public void closeServer() {
    	canClose = true;
    	this.notifyAll();
	}

	public static boolean isThreadCpuTimeSupported() {
		return isThreadCpuTimeSupported;
	}

	WatchDog getWatchDog() {
		return watchDog;
	}

	synchronized public boolean canCloseServer() {
		return canClose;
	}
	
	public synchronized int getApprovedPhaseNumber() {
		return approvedPhaseNumber;
	}
	
	public synchronized void setApprovedPhaseNumber(int approvedPhaseNumber) {
		this.approvedPhaseNumber = approvedPhaseNumber;
		notifyAll();
	}
	
	//******************* EVENTS ********************/
	
	synchronized public void graphStarted() {
		graphDetail.graphStarted();

		sendNotification(new Notification(GRAPH_STARTED, this/*getGraphDetail()*/, notificationSequence++));
	}

	synchronized public void phaseStarted(Phase phase) {
		graphDetail.phaseStarted(phase);
		
		sendNotification(new Notification(PHASE_STARTED, this/*getGraphDetail().getRunningPhaseDetail()*/, notificationSequence++));
	}

	synchronized public void gatherTrackingDetails() {
		graphDetail.gatherTrackingDetails();
		
		sendNotification(new Notification(TRACKING_UPDATED, this/*getGraphDetail().getRunningPhaseDetail()*/, notificationSequence++));
	}

	synchronized public void phaseFinished() {
		graphDetail.phaseFinished();
		
		sendNotification(new Notification(PHASE_FINISHED, this/*getGraphDetail().getRunningPhaseDetail()*/, notificationSequence++));
	}

	synchronized public void phaseAborted() {
		graphDetail.phaseFinished();
		
		sendNotification(new Notification(PHASE_ABORTED, this/*getGraphDetail().getRunningPhaseDetail()*/, notificationSequence++));
	}

	synchronized public void phaseError(String message) {
		graphDetail.phaseFinished();
		
		sendNotification(new Notification(PHASE_ERROR, this/*getGraphDetail().getRunningPhaseDetail()*/, notificationSequence++));
	}

	synchronized public void graphFinished() {
		if(!graphFinished) { // if graph was already finished, we'll send only a notification
			graphDetail.graphFinished();
			graphFinished = true;
		}

		sendNotification(new Notification(GRAPH_FINISHED, this/*getGraphDetail()*/, notificationSequence++));
	}

	/**
	 * Graph was aborted. Only send a notification.
	 */
	synchronized public void graphAborted() {
		if(!graphFinished) { // if graph was already finished, we'll send only a notification
			graphDetail.gatherTrackingDetails();
			graphDetail.graphFinished();
			graphFinished = true;
		}

		sendNotification(new Notification(GRAPH_ABORTED , this/*getGraphDetail()*/, notificationSequence++));
	}

	/**
	 * Graph ends with an error. Only send a notification.
	 */
	synchronized public void graphError(String message) {
		if(!graphFinished) { // if graph was already finished, we'll send only a notification
			graphDetail.gatherTrackingDetails();
			graphDetail.graphFinished();
			graphFinished = true;
		}

		sendNotification(new Notification(GRAPH_ERROR, this/*getGraphDetail()*/, notificationSequence++, message));
	}

}
