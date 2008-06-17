/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.graph.runtime;

import java.text.DateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.runtime.jmx.CloverJMX;
import org.jetel.graph.runtime.jmx.InputPortTrackingDetail;
import org.jetel.graph.runtime.jmx.NodeTrackingDetail;
import org.jetel.graph.runtime.jmx.OutputPortTrackingDetail;
import org.jetel.graph.runtime.jmx.PhaseTrackingDetail;
import org.jetel.util.string.StringUtils;


/**
 * Console logger of all tracking information. This class is JMX notification listener.
 * Each clover graph tracking update is sent to the engine log.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *         
 * @author David Pavlis (david.pavlis@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 13, 2008
 */
public class TrackingLogger implements NotificationListener {
	
    private final static String TRACKING_LOGGER_NAME = "Tracking";
    private static final Log logger = LogFactory.getLog(TRACKING_LOGGER_NAME);

    private static final int[] ARG_SIZES_WITH_CPU = { -6, -4, 28, -5, 9, 12, 7, 8 };
    private static final int[] ARG_SIZES_WITHOUT_CPU = { 38, -5, 9, 12, 7, 8 };
    volatile boolean runIt = true;
    private final CloverJMX cloverJMX;
    
    public static void track(CloverJMX cloverJMX) {
    	new TrackingLogger(cloverJMX);
    }
    
    private TrackingLogger(CloverJMX cloverJMX) {
    	this.cloverJMX = cloverJMX;

    	cloverJMX.addNotificationListener(this, null, null);
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
            logger.info("----------------------** Final tracking Log for phase [" + cloverJMX.getGraphDetail().getRunningPhaseDetail().getPhaseNum() + "] **---------------------");
        else 
            logger.info("---------------------** Start of tracking Log for phase [" + cloverJMX.getGraphDetail().getRunningPhaseDetail().getPhaseNum() + "] **-------------------");
        // France is here just to get 24hour time format
        logger.info("Time: "
            + DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, Locale.FRANCE).
                format(Calendar.getInstance().getTime()));
        logger.info("Node                   Status     Port      #Records         #KB  Rec/s    KB/s");
        logger.info("----------------------------------------------------------------------------------");
        for (NodeTrackingDetail nodeDetail : cloverJMX.getGraphDetail().getRunningPhaseDetail().getNodesDetails()) {
            Object nodeInfo[] = {nodeDetail.getNodeId(), nodeDetail.getResult().message()};
            int nodeSizes[] = {-23, -15};
            logger.info(StringUtils.formatString(nodeInfo, nodeSizes));
            //in ports
            Object portInfo[];
            boolean cpuPrinted = false;
            int i = 0;
            for (InputPortTrackingDetail inputPortDetail : nodeDetail.getInputPortsDetails()) {
                if (i == 0) {
                    cpuPrinted = true;
                    final float cpuUsage = (finalTracking ? nodeDetail.getPeakUsageCPU()  : nodeDetail.getUsageCPU());
                    portInfo = new Object[] {" %cpu:", cpuUsage >= 0.01f ? Float.toString(cpuUsage) : "..",
                            "In:", Integer.toString(i), 
                            Integer.toString(inputPortDetail.getTotalRows()),
                            Long.toString(inputPortDetail.getTotalBytes() >> 10),
                            Integer.toString(inputPortDetail.getAverageRows()),
                            Integer.toString(inputPortDetail.getAverageBytes() >> 10)};
                    logger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITH_CPU)); 
                } else {
                        portInfo = new Object[] {"In:", Integer.toString(i), 
                        Integer.toString(inputPortDetail.getTotalRows()),
                        Long.toString(inputPortDetail.getTotalBytes() >> 10),
                        Integer.toString(inputPortDetail.getAverageRows()),
                        Integer.toString(inputPortDetail.getAverageBytes())};
                    logger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITHOUT_CPU));
                }
                i++;
            }
            //out ports
            i = 0;
            for (OutputPortTrackingDetail outputPortDetail : nodeDetail.getOutputPortsDetails()) {
                if (i == 0 && !cpuPrinted) {
                    final float cpuUsage = (finalTracking ? nodeDetail.getPeakUsageCPU() : nodeDetail.getUsageCPU());
                    portInfo = new Object[] {" %cpu:", cpuUsage > 0.01f ? Float.toString(cpuUsage) : "..",
                            "Out:", Integer.toString(i), 
                            Integer.toString(outputPortDetail.getTotalRows()),
                            Long.toString(outputPortDetail.getTotalBytes() >> 10),
                            Integer.toString(outputPortDetail.getAverageRows()),
                            Integer.toString(outputPortDetail.getAverageBytes() >> 10)};
                    logger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITH_CPU));
                }else{
                    portInfo = new Object[] {"Out:", Integer.toString(i), 
                        Integer.toString(outputPortDetail.getTotalRows()),
                        Long.toString(outputPortDetail.getTotalBytes() >> 10),
                        Integer.toString(outputPortDetail.getAverageRows()),
                        Integer.toString(outputPortDetail.getAverageBytes() >> 10)};
                    logger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITHOUT_CPU));
                }
                i++;
            }               
        }
        logger.info("---------------------------------** End of Log **--------------------------------");
    }

	/**  Outputs summary info about executed phases */
	private void printPhasesSummary() {
		logger.info("-----------------------** Summary of Phases execution **---------------------");
		logger.info("Phase#            Finished Status         RunTime(sec)    MemoryAllocation(KB)");
		for (PhaseTrackingDetail phaseDetail : cloverJMX.getGraphDetail().getPhasesDetails()) {
			if(phaseDetail != null) {
    			Object nodeInfo[] = { Integer.valueOf(phaseDetail.getPhaseNum()), 
    					phaseDetail.getResult().message(),
                        phaseDetail.getExecutionTime(TimeUnit.SECONDS),
                        phaseDetail.getMemoryUtilization() >> 10};
    			int nodeSizes[] = {-18, -24, 12, 18};
    			logger.info(StringUtils.formatString(nodeInfo, nodeSizes));
			}
		}
		logger.info("------------------------------** End of Summary **---------------------------");
	}

	public void handleNotification(Notification notification, Object handback) {
		if(notification.getType().equals(CloverJMX.GRAPH_STARTED)) {
			//printProcessingStatus(false);
		} else if(notification.getType().equals(CloverJMX.TRACKING_UPDATED)) {
			printProcessingStatus(false);
		} else if(notification.getType().equals(CloverJMX.PHASE_FINISHED)) {
			printProcessingStatus(true);
		} else if(notification.getType().equals(CloverJMX.GRAPH_FINISHED)) {
			printPhasesSummary();
			try {
				cloverJMX.removeNotificationListener(this);
			} catch (ListenerNotFoundException e) {
				logger.warn("Unexpected error while graph logging will be ignored.");
			}
		}
	}

}
