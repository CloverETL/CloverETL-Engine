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
    
    N_A(3,"N/A", false),
    READY(2,"READY", false),
    RUNNING(1,"RUNNING", false),
    WAITING(4,"WAITING", false),
    FINISHED_OK(0,"FINISHED_OK", true),
    ERROR(-1,"ERROR", true),
    ABORTED(-2,"ABORTED", true),
    TIMEOUT(-3, "TIMEOUT", true), 
    UNKNOWN(-4, "UNKNOWN", true); 
    
    private final int code;
    private final String message;
    private boolean stop;
    
    Result(int code, String message, boolean stop) {
		this.code = code;
		this.message = message;
		this.stop = stop;
	}
    
    public int code(){return code;}
    
    public String message(){return message;}
    
    public boolean isStop(){return stop;}
    
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