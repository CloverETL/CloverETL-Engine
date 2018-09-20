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
package org.jetel.graph;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Enum of different runtime results with numeric codes and
 * string indetifications of results.<br>
 * 
 * The order of results is:
 * <ol>
 * <li>N_A (no result available; component just created)
 * <li>READY (component is ready to be executed; was initialized)
 * <li>RUNNING (component is being executed)
 * <li><ul>
 * <li>FINISHED_OK (component finished execution succesfully)
 * <li>ERROR (componet finished execution with error)
 * <li>ABORTED (component was aborted/interrupted)
 * </ul></li>
 * </ol>
 * 
 * @author david.pavlis
 * @since  11.1.2007
 *
 */
public enum Result {

	N_A(3, "N/A", "Not available", false),
	READY(2, "READY", "Ready", false),
	RUNNING(1, "RUNNING", "Running", false),
	WAITING(4, "WAITING", "Waiting", false),
	FINISHED_OK(0, "FINISHED_OK", "Finished OK", true),
	ERROR(-1, "ERROR", "Error", true),
	ABORTED(-2, "ABORTED", "Aborted", true),
	TIMEOUT(-3, "TIMEOUT", "Timeout", true), 
	UNKNOWN(-4, "UNKNOWN", "Unknown", true); 
	
	private final int code;
	private final String message;
	private final String label;
	private boolean stop;
	private static final List<Result> sortedStatusList;

	private static final Comparator<Result> LABEL_COMPARATOR = new Comparator<Result>() {
		@Override
		public int compare(Result s1, Result s2) {
			String label1 = s1.getLabel();
			String label2 = s2.getLabel();
			return label1.compareTo(label2);
		}
	};
	
	static {
		sortedStatusList = Arrays.asList(Result.values());
		Collections.sort(sortedStatusList, Result.LABEL_COMPARATOR);
	}

	Result(int code, String message,String label, boolean stop) {
		this.code = code;
		this.label = label;
		this.message = message;
		this.stop = stop;
	}

    public int code(){return code;}
    
    public String message(){return message;}
    
    public boolean isStop(){return stop;}

	public String getLabel() {
		return label;
	}
	
	public String getMessage() {
		return message;
	}

	public static List<Result> getResultsSorted() {
		return sortedStatusList;
	}

    /**
     * Converts string representation to the enum.
     * @param result
     * @return the enum.
     */
    public static Result fromString(String result) {
    	if (result.equals(N_A.message())) {
    		return N_A;
    	}
    	return Result.valueOf(result);
    }
    
}