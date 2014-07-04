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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jetel.data.Defaults;
import org.jetel.util.string.StringUtils;

/**
 * This class enumerates all types of graph tracking JMX notifications.
 *  
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2 Mar 2012
 */
public enum TrackingEvent {
	
	// currently only 2 possible values
	GRAPH_FINISHED("graphFinished", "GRAPH_FINISHED", "GRAPH_ABORTED", "GRAPH_ERROR", "GRAPH_TIMEOUT"),
	JOBFLOW_FINISHED("jobflowFinished", "JOBFLOW_FINISHED", "JOBFLOW_ABORTED", "JOBFLOW_ERROR", "JOBFLOW_TIMEOUT"),
	PROFILER_JOB_FINISHED("profilerJobFinished", "PROFILER_JOB_FINISHED", "PROFILER_JOB_ABORTED", "PROFILER_JOB_ERROR", "PROFILER_JOB_TIMEOUT");
	
	private String id;
	
	private List<String> associatedNotificationTypes = new ArrayList<String>();
	
	private TrackingEvent(String id, String... associatedNotificationTypes) {
		this.id = id;
		this.associatedNotificationTypes = Arrays.asList(associatedNotificationTypes);
	}
	
	/**
	 * @return identifier of this tracking event
	 */
	public String getId() {
		return id;
	}
	
	/**
	 * @param notificationType identifier of requested {@link TrackingEvent}
	 * @return a TrackingEvent based on notification type
	 * @throws IllegalArgumentException when the TrackingEvent isn't found. 
	 */
	public static TrackingEvent fromNotificationType(String notificationType) {
		for (TrackingEvent trackingEvent : values()) {
			if (trackingEvent.associatedNotificationTypes.contains(notificationType)) {
				return trackingEvent;
			}
		}
		throw new IllegalArgumentException("unknown tracking notification type '" + notificationType + "'");
	}
	
	/**
	 * Works in the same way as {@link #fromNotificationType(String)}, but
	 * return null if the TrackingEvent isn't found. 
	 * @param notificationType
	 * @return
	 */
	public static TrackingEvent fromNotificationTypeSafely(String notificationType) {
		for (TrackingEvent trackingEvent : values()) {
			if (trackingEvent.associatedNotificationTypes.contains(notificationType)) {
				return trackingEvent;
			}
		}// for
		return null;
	}
	
	/**
	 * Converts given string into list of {@link TrackingEvent}.
	 * String is considered as semicolon separated list of tracking event identifiers, for example:
	 * 
	 * "detailsGathered;phaseFinished"
	 */
	public static List<TrackingEvent> fromTrackingEventId(String trackingEventIds) {
		List<TrackingEvent> result = new ArrayList<TrackingEvent>();
		if (StringUtils.isEmpty(trackingEventIds)) {
			return result;
		}
		
		for (String trackingEventId : trackingEventIds.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX)) {
			boolean found = false;
			for (TrackingEvent trackingEvent : values()) {
				if (trackingEvent.id.equals(trackingEventId)) {
					result.add(trackingEvent);
					found = true;
					break;
				}
			}
			if (!found) {
				throw new IllegalArgumentException("unknown tracking event id '" + trackingEventId + "'");
			}
		}
		
		return result;
	}
	
    /**
     * Static definition of default tracking event list for graph monitoring - list of "JMX" events
     * of executed graph which are listened. For now this list is statically defined since only "graphFinish"
     * is supported by clover server. It is possible to extend this functionality in the future.
     * Other possible tracking events could be "phaseFinished", "nodeFinished" or "trackingUpdate".
     * @see MonitorGraph
     */
    public static final List<TrackingEvent> DEFAULT_TRACKING_EVENT_LIST;
	static {
		DEFAULT_TRACKING_EVENT_LIST = new ArrayList<TrackingEvent>();
		DEFAULT_TRACKING_EVENT_LIST.add(TrackingEvent.GRAPH_FINISHED);
		DEFAULT_TRACKING_EVENT_LIST.add(TrackingEvent.JOBFLOW_FINISHED);
		DEFAULT_TRACKING_EVENT_LIST.add(TrackingEvent.PROFILER_JOB_FINISHED);
	}

}
	
