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

import javax.management.NotificationListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.JobType;
import org.jetel.graph.runtime.jmx.CloverJMX;


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


}
