/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-07  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 26.2.2007
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.graph.runtime;

import java.io.Serializable;

public class PhaseTrackingDetail implements Serializable {
    
    private static final long serialVersionUID = -6140687882226805507L;
    
    private int execTime;
    private long memUtilization;

    
    public PhaseTrackingDetail(int execTime,long memUtilization) {
        this.execTime=execTime;
        this.memUtilization=memUtilization;
    }

    /**
     * @return the phaseExecTime in milliseconds
     * @since 27.2.2007
     */
    public int getExecTime() {
        return execTime;
    }
    
    /**
     * @return the phaseExecTime in seconds
     * @since 27.2.2007
     */
    public int getExecTimeSec() {
        return execTime/1000;
    }
    
    /**
     * @param phaseExecTime the phaseExecTime to set
     * @since 27.2.2007
     */
    public void setExecTime(int phaseExecTime) {
        this.execTime = phaseExecTime;
    }
    /**
     * @return the phaseMemUtilization in kilobytes (KB)
     * @since 27.2.2007
     */
    public long getMemUtilization() {
        return memUtilization;
    }
    
    public int getMemUtilizationKB() {
        return (int)memUtilization/1024;
    }
    
    /**
     * @param phaseMemUtilization the phaseMemUtilization to set
     * @since 27.2.2007
     */
    public void setMemUtilization(long phaseMemUtilization) {
        this.memUtilization = phaseMemUtilization;
    }
}
