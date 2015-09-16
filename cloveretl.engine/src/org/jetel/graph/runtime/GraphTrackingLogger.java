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

import org.jetel.graph.runtime.jmx.CloverJMX;
import org.jetel.graph.runtime.jmx.NodeTracking;
import org.jetel.graph.runtime.jmx.PortTracking;
import org.jetel.util.string.StringUtils;

/**
 * Console logger of all tracking information. This class is JMX notification listener.
 * This tracking logger implementation is dedicated for ETL graphs.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.5.2012
 */
public class GraphTrackingLogger extends TrackingLogger {

    private static final int[] ARG_SIZES_WITH_CPU = { -6, -4, 27, 15, 11, 8, 8 };
    private static final int[] ARG_SIZES_WITHOUT_CPU = { 37, 15, 11, 8, 8 };

    GraphTrackingLogger(CloverJMX cloverJMX) {
    	super(cloverJMX);
    }

    /**
     *  Outputs basic LOG information about graph processing
     *
     * @param  iterator  Description of Parameter
     * @param  phaseNo   Description of the Parameter
     * @since            July 30, 2002
     */
	@Override
	protected void printProcessingStatus(boolean finalTracking) {
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
        	logger.info("Node                   ID        Port       #Records        #KB  aRec/s   aKB/s");
        } else {
        	logger.info("Node                   ID        Port       #Records        #KB   Rec/s    KB/s");
        }
        logger.info("---------------------------------------------------------------------------------");
        for (NodeTracking nodeDetail : cloverJMX.getGraphTracking().getRunningPhaseTracking().getNodeTracking()) {
            Object nodeInfo[] = {nodeDetail.getNodeName(), nodeDetail.getNodeID(), nodeDetail.getResult().message()};
            int nodeSizes[] = {-23, -41, 15};
            logger.info(StringUtils.formatString(nodeInfo, nodeSizes));
            //in ports
            Object portInfo[];
            boolean cpuPrinted = false;
            int i = 0;
            for (PortTracking inputPortDetail : nodeDetail.getInputPortTracking()) {
                if (i == 0) {
                    cpuPrinted = true;
                    final float cpuUsage = (finalTracking ? nodeDetail.getPeakUsageCPU()  : nodeDetail.getUsageCPU());
                    portInfo = new Object[] {" %cpu:", Integer.toString((int) (cpuUsage * 100)),
                            "In:" + Integer.toString(i), 
                            Long.toString(inputPortDetail.getTotalRecords()),
                            Long.toString(inputPortDetail.getTotalBytes() >> 10),
                            Integer.toString(inputPortDetail.getRecordFlow()),
                            Integer.toString(inputPortDetail.getByteFlow() >> 10)};
                    logger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITH_CPU)); 
                } else {
                        portInfo = new Object[] {"In:" + Integer.toString(i), 
                        Long.toString(inputPortDetail.getTotalRecords()),
                        Long.toString(inputPortDetail.getTotalBytes() >> 10),
                        Integer.toString(inputPortDetail.getRecordFlow()),
                        Integer.toString(inputPortDetail.getByteFlow() >> 10)};
                    logger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITHOUT_CPU));
                }
                i++;
            }
            //out ports
            i = 0;
            for (PortTracking outputPortDetail : nodeDetail.getOutputPortTracking()) {
                if (i == 0 && !cpuPrinted) {
                    cpuPrinted = true;
                    final float cpuUsage = (finalTracking ? nodeDetail.getPeakUsageCPU() : nodeDetail.getUsageCPU());
                    portInfo = new Object[] {" %cpu:", Integer.toString((int) (cpuUsage * 100)),
                            "Out:" + Integer.toString(i), 
                            Long.toString(outputPortDetail.getTotalRecords()),
                            Long.toString(outputPortDetail.getTotalBytes() >> 10),
                            Integer.toString(outputPortDetail.getRecordFlow()),
                            Integer.toString(outputPortDetail.getByteFlow() >> 10)};
                    logger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITH_CPU));
                }else{
                    portInfo = new Object[] {"Out:" + Integer.toString(i), 
                    	Long.toString(outputPortDetail.getTotalRecords()),
                        Long.toString(outputPortDetail.getTotalBytes() >> 10),
                        Integer.toString(outputPortDetail.getRecordFlow()),
                        Integer.toString(outputPortDetail.getByteFlow() >> 10)};
                    logger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITHOUT_CPU));
                }
                i++;
            }
            //CPU usage has to be printed also for components without ports
            if (!cpuPrinted) {
                final float cpuUsage = (finalTracking ? nodeDetail.getPeakUsageCPU() : nodeDetail.getUsageCPU());
                portInfo = new Object[] {" %cpu:", Integer.toString((int) (cpuUsage * 100))};
                logger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITH_CPU));
            }
        }
        logger.info("---------------------------------** End of Log **--------------------------------");
    }

}
