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

import org.jetel.graph.dictionary.DictionaryValuesContainer;

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
     * Notification identifier - initialization ends with an error.
     */
    public static final String INITIALIZE_ERROR = "clover.init.error";
	
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
     * Notification identifier - node finished work (both normally and with error or abortion).
     */
    public static final String NODE_FINISHED = "clover.node.finished";

    /**
     * @return comprehensive graph tracking information
     */
    public GraphTracking getGraphTracking(long runId);

    /**
     * Event for clients to stop graph processing.
     * This operation is blocking until the graph is really aborted.
     * @return true if watchdog was aborted, false if there was no watchdog to abort 
     */
    public boolean abortGraphExecution(long runId);

    /**
     * Event for clients to stop graph processing.
     * This abort operation can be synchronous or asynchronous,
     * based on the only parameter @param waitForAbort.
     * Method execution with waitForAbort=true is identical with
     * {@link #abortGraphExecution()}.
     * Method execution with waitForAbort=false just send a signal,
     * which tries to abort the graph and the current thread is 
     * immediately returned.
     * @return true if watchdog was aborted, false if there was no watchdog to abort
     */
    public boolean abortGraphExecution(long runId, boolean waitForAbort);

    /**
     * Client should call this method immediately after all tracking information have been received.
     */
    public void releaseJob(long runId);
    
    public void setApprovedPhaseNumber(long runId, int approvedPhaseNumber, DictionaryValuesContainer mergedDictionary);
}
