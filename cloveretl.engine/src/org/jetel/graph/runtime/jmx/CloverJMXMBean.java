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



/**
 * JMX managed bean which is dedicated to provide tracking information about running graph.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 6, 2008
 */
public interface CloverJMXMBean {

    /**
     * Notification identifier - graph was started.
     */
    public static final String GRAPH_STARTED = "clover.graph.started";

    /**
     * Notification identifier - phase was started.
     */
    public static final String PHASE_STARTED = "clover.phase.started";
    
    /**
     * Notification identifier - tracking information was updated.
     */
    public static final String TRACKING_UPDATED = "clover.tracking.updated";

    /**
     * Notification identifier - phase was finished.
     */
    public static final String PHASE_FINISHED = "clover.phase.finished";

    /**
     * Notification identifier - phase was aborted.
     */
    public static final String PHASE_ABORTED = "clover.phase.aborted";

    /**
     * Notification identifier - phase ends with an error.
     */
    public static final String PHASE_ERROR = "clover.phase.error";

    /**
     * Notification identifier - graph was finished.
     */
    public static final String GRAPH_FINISHED = "clover.graph.finished";

    /**
     * Notification identifier - graph was aborted.
     */
    public static final String GRAPH_ABORTED = "clover.graph.aborted";

    /**
     * Notification identifier - graph ends with an error.
     */
    public static final String GRAPH_ERROR = "clover.graph.error";

    
    /**
     * (Not implemented)
     * @return clover engine version in text form
     */
    public String getCloverVersion();
    
    /**
     * @return comprehensive graph tracking information
     */
    public GraphTracking getGraphTracking();

    /**
     * Event for clients to stop graph processing.
     */
    public void abortGraphExecution();

    /**
     * Client should call this method immediately after all tracking information have been received.
     */
    public void closeServer();

}
