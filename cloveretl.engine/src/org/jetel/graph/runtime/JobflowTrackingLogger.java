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

import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;

import org.jetel.graph.runtime.jmx.CloverJMX;
import org.jetel.graph.runtime.jmx.NodeTracking;
import org.jetel.graph.runtime.jmx.PhaseTracking;
import org.jetel.graph.runtime.jmx.PortTracking;
import org.jetel.graph.runtime.jmx.TrackingUtils;
import org.jetel.util.string.StringUtils;

/**
 * Console logger of all tracking information. This class is JMX notification listener.
 * This tracking logger implementation is dedicated for jobflows.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.5.2012
 */
public class JobflowTrackingLogger extends TrackingLogger {

    private static final int[] ARG_SIZES_WITHOUT_CPU = { 38, -5, 9, 12, 7, 8 };

    JobflowTrackingLogger(CloverJMX cloverJMX) {
    	super(cloverJMX);
    }

    /**
     *  Outputs basic LOG information about graph processing
     *
     * @param  iterator  Description of Parameter
     * @param  phaseNo   Description of the Parameter
     * @since            July 30, 2002
     */
    private void printProcessingStatus(boolean finalTracking) {
        //StringBuilder strBuf=new StringBuilder(120);
        if (finalTracking)
            logger.info("----------------------** Final tracking Log for phase [" + cloverJMX.getGraphTracking().getRunningPhaseTracking().getPhaseNum() + "] **---------------------");
        else 
            logger.info("---------------------** Start of tracking Log for phase [" + cloverJMX.getGraphTracking().getRunningPhaseTracking().getPhaseNum() + "] **-------------------");
        // France is here just to get 24hour time format
        logger.info("Time: "
            + DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, Locale.FRANCE).
                format(new Date()));
        if (finalTracking) {
        	logger.info("Node                   ID         Port      #Records");
        } else {
        	logger.info("Node                   ID         Port      #Records");
        }
        logger.info("---------------------------------------------------------------------------------");
        for (NodeTracking nodeDetail : cloverJMX.getGraphTracking().getRunningPhaseTracking().getNodeTracking()) {
            Object nodeInfo[] = {nodeDetail.getNodeName(), nodeDetail.getNodeID(), nodeDetail.getResult().message()};
            int nodeSizes[] = {-23, -41, 15};
            logger.info(StringUtils.formatString(nodeInfo, nodeSizes));
            //in ports
            Object portInfo[];
            for (PortTracking inputPortDetail : nodeDetail.getInputPortTracking()) {
                portInfo = new Object[] {"In:", Integer.toString(inputPortDetail.getIndex()), 
	                Long.toString(inputPortDetail.getTotalRecords())};
                logger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITHOUT_CPU));
            }
            //out ports
            for (PortTracking outputPortDetail : nodeDetail.getOutputPortTracking()) {
            	portInfo = new Object[] {"Out:", Integer.toString(outputPortDetail.getIndex()), 
                	Long.toString(outputPortDetail.getTotalRecords())};
                logger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITHOUT_CPU));
            }
        }
        logger.info("---------------------------------** End of Log **--------------------------------");
    }

	/**  Outputs summary info about executed phases */
	private void printPhasesSummary() {
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

	@Override
	public void handleNotification(Notification notification, Object handback) {
		if (notification.getType().equals(CloverJMX.PHASE_FINISHED)) {
			printProcessingStatus(true);
			logger.info("Execution of phase [" + cloverJMX.getGraphTracking().getRunningPhaseTracking().getPhaseNum()
					+ "] successfully finished - elapsed time(sec): "
					+ TrackingUtils.convertTime(cloverJMX.getGraphTracking().getExecutionTime(), TimeUnit.SECONDS));
		} else if(notification.getType().equals(CloverJMX.PHASE_ABORTED)) {
			logger.info("Execution of phase [" + cloverJMX.getGraphTracking().getRunningPhaseTracking().getPhaseNum()
					+ "] was aborted - elapsed time(sec): "
					+ TrackingUtils.convertTime(cloverJMX.getGraphTracking().getExecutionTime(), TimeUnit.SECONDS));
		} else if(notification.getType().equals(CloverJMX.PHASE_ERROR)) {
				logger.info("Execution of phase [" + cloverJMX.getGraphTracking().getRunningPhaseTracking().getPhaseNum()
						+ "] finished with error - elapsed time(sec): "
						+ TrackingUtils.convertTime(cloverJMX.getGraphTracking().getExecutionTime(), TimeUnit.SECONDS));
		} else if(notification.getType().equals(CloverJMX.GRAPH_FINISHED)
				|| notification.getType().equals(CloverJMX.GRAPH_ABORTED)
				|| notification.getType().equals(CloverJMX.GRAPH_ERROR)) {
			printPhasesSummary();
			try {
				cloverJMX.removeNotificationListener(this);
			} catch (ListenerNotFoundException e) {
				logger.warn("Unexpected error while graph logging will be ignored.");
			}
		}
	}

}
