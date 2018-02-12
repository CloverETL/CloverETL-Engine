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
package org.jetel.graph.runtime;

import java.util.concurrent.TimeUnit;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.MDC;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.runtime.jmx.CloverJMX;
import org.jetel.graph.runtime.jmx.GraphTrackingDetail;
import org.jetel.graph.runtime.jmx.PhaseTracking;
import org.jetel.graph.runtime.jmx.RunIdNotificationFilter;
import org.jetel.graph.runtime.jmx.TrackingUtils;
import org.jetel.util.LogUtils;
import org.jetel.util.string.StringUtils;


/**
 * Console logger of all tracking information. This class is JMX notification listener.
 * ETL graphs and jobflows have different tracking logger, see {@link GraphTrackingLogger}
 * and {@link JobflowTrackingLogger}.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *         
 * @author David Pavlis (david.pavlis@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 13, 2008
 */
public abstract class TrackingLogger implements NotificationListener {
	
    public static final String TRACKING_LOGGER_NAME = "Tracking";
    
    protected static final Log logger = LogFactory.getLog(TRACKING_LOGGER_NAME);

    private WatchDog watchDog;
    
    public static void track(WatchDog watchDog) {
    	TrackingLogger trackingLogger = null;
    	if (ContextProvider.getRuntimeJobType().isGraph()) {
    		trackingLogger = new GraphTrackingLogger(watchDog);
    	} else {
    		trackingLogger = new JobflowTrackingLogger(watchDog);
    	}
		CloverJMX.getInstance().addNotificationListener(trackingLogger, new RunIdNotificationFilter(watchDog.getGraphRuntimeContext().getRunId()), null);
    }
    
    protected TrackingLogger(WatchDog watchDog) {
    	this.watchDog = watchDog;
    }

	@Override
	public void handleNotification(Notification notification, Object handback) {
		JMXNotificationMessage message = (JMXNotificationMessage) notification.getUserData();

		Object oldRunId = MDC.get(LogUtils.MDC_RUNID_KEY);
		MDC.put(LogUtils.MDC_RUNID_KEY, Long.valueOf(message.getRunId()));

		try {
			if(notification.getType().equals(CloverJMX.GRAPH_STARTED)) {
				graphStarted();
			} else if(notification.getType().equals(CloverJMX.TRACKING_UPDATED)) {
				trackingUpdated();
			} else if(notification.getType().equals(CloverJMX.PHASE_FINISHED)) {
				phaseFinished();
			} else if(notification.getType().equals(CloverJMX.PHASE_ABORTED)) {
				phaseAborted();
			} else if(notification.getType().equals(CloverJMX.PHASE_ERROR)) {
				phaseError();
			} else if(notification.getType().equals(CloverJMX.GRAPH_FINISHED)
					|| notification.getType().equals(CloverJMX.GRAPH_ABORTED)
					|| notification.getType().equals(CloverJMX.GRAPH_ERROR)) {
				graphFinished();
				try {
					CloverJMX.getInstance().removeNotificationListener(this);
				} catch (ListenerNotFoundException e) {
					logger.warn("Unexpected error while graph logging will be ignored.");
				}
			}
		} finally {
			if (oldRunId == null) {
				MDC.remove(LogUtils.MDC_RUNID_KEY);
			} else {
				MDC.put(LogUtils.MDC_RUNID_KEY, oldRunId);
			}
		}
	}

    protected void graphStarted() {
		//printProcessingStatus(false);
    }

    protected void trackingUpdated() {
		printProcessingStatus(false);
    }

	protected void phaseFinished() {
		printProcessingStatus(true);
		logger.info("Execution of phase [" + getGraphTracking().getRunningPhaseTracking().getPhaseLabel()
				+ "] successfully finished - elapsed time(sec): "
				+ TrackingUtils.convertTime(getGraphTracking().getExecutionTime(), TimeUnit.SECONDS));
	}

	protected void phaseAborted() {
		logger.info("Execution of phase [" + getGraphTracking().getRunningPhaseTracking().getPhaseLabel()
				+ "] was aborted - elapsed time(sec): "
				+ TrackingUtils.convertTime(getGraphTracking().getExecutionTime(), TimeUnit.SECONDS));
	}

	protected void phaseError() {
		logger.info("Execution of phase [" + getGraphTracking().getRunningPhaseTracking().getPhaseLabel()
				+ "] finished with error - elapsed time(sec): "
				+ TrackingUtils.convertTime(getGraphTracking().getExecutionTime(), TimeUnit.SECONDS));
	}
	
	protected void graphFinished() {
		if (getGraphTracking().getPhaseTracking().length > 0) {
			logger.info("-----------------------** Summary of Phases execution **---------------------");
			logger.info("Phase#            Finished Status         RunTime(sec)    MemoryAllocation(KB)");
			for (PhaseTracking phaseDetail : getGraphTracking().getPhaseTracking()) {
				if(phaseDetail != null) {
	    			Object nodeInfo[] = { phaseDetail.getPhaseLabel(), 
	    					phaseDetail.getResult().message(),
	    					TrackingUtils.convertTime(phaseDetail.getExecutionTime(), TimeUnit.SECONDS),
	                        phaseDetail.getMemoryUtilization() >> 10};
	    			int nodeSizes[] = {-18, -24, 12, 18};
	    			logger.info(StringUtils.formatString(nodeInfo, nodeSizes));
				}
			}
			logger.info("------------------------------** End of Summary **---------------------------");
		}
	}

	protected abstract void printProcessingStatus(boolean finalTracking);

	protected GraphTrackingDetail getGraphTracking() {
		return watchDog.getGraphTracking();
	}
	
}
