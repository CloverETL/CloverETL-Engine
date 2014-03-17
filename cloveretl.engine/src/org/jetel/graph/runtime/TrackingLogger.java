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
import org.jetel.graph.ContextProvider;
import org.jetel.graph.runtime.jmx.CloverJMX;
import org.jetel.graph.runtime.jmx.PhaseTracking;
import org.jetel.graph.runtime.jmx.TrackingUtils;
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
	
    public final static String TRACKING_LOGGER_NAME = "Tracking";
    protected static final Log logger = LogFactory.getLog(TRACKING_LOGGER_NAME);

    protected final CloverJMX cloverJMX;
    
    public static void track(CloverJMX cloverJMX) {
    	TrackingLogger trackingLogger = null;
    	if (ContextProvider.getJobType().isGraph()) {
    		trackingLogger = new GraphTrackingLogger(cloverJMX);
    	} else {
    		trackingLogger = new JobflowTrackingLogger(cloverJMX);
    	}
		cloverJMX.addNotificationListener(trackingLogger, null, null);
    }

    TrackingLogger(CloverJMX cloverJMX) {
    	this.cloverJMX = cloverJMX;
    }

	@Override
	public void handleNotification(Notification notification, Object handback) {
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
				cloverJMX.removeNotificationListener(this);
			} catch (ListenerNotFoundException e) {
				logger.warn("Unexpected error while graph logging will be ignored.");
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
		logger.info("Execution of phase [" + cloverJMX.getGraphTracking().getRunningPhaseTracking().getPhaseNum()
				+ "] successfully finished - elapsed time(sec): "
				+ TrackingUtils.convertTime(cloverJMX.getGraphTracking().getExecutionTime(), TimeUnit.SECONDS));
	}

	protected void phaseAborted() {
		logger.info("Execution of phase [" + cloverJMX.getGraphTracking().getRunningPhaseTracking().getPhaseNum()
				+ "] was aborted - elapsed time(sec): "
				+ TrackingUtils.convertTime(cloverJMX.getGraphTracking().getExecutionTime(), TimeUnit.SECONDS));
	}

	protected void phaseError() {
		logger.info("Execution of phase [" + cloverJMX.getGraphTracking().getRunningPhaseTracking().getPhaseNum()
				+ "] finished with error - elapsed time(sec): "
				+ TrackingUtils.convertTime(cloverJMX.getGraphTracking().getExecutionTime(), TimeUnit.SECONDS));
	}
	
	protected void graphFinished() {
		if (cloverJMX.getGraphTracking().getPhaseTracking().length > 0) {
			logger.info("-----------------------** Summary of Phases execution **---------------------");
			logger.info("Phase#            Finished Status         RunTime(sec)    MemoryAllocation(KB)");
			for (PhaseTracking phaseDetail : cloverJMX.getGraphTracking().getPhaseTracking()) {
				if(phaseDetail != null) {
	    			Object nodeInfo[] = { Integer.valueOf(phaseDetail.getPhaseNum()), 
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

}
