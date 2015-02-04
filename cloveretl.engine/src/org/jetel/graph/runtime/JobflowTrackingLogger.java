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
    
    @Override
    protected void trackingUpdated() {
    	//DO NOTHING
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

}
