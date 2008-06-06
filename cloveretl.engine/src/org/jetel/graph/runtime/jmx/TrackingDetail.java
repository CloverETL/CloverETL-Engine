/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-06  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 4.1.2007
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.graph.runtime.jmx;

import org.jetel.graph.Result;

public interface TrackingDetail {

    public enum PortType {
        IN_PORT,
        OUT_PORT;
    }
    
    public int getAvgBytes(PortType portType,int portNum);
    public int getAvgRows(PortType portType,int portNum);
    public int getPeakRows(PortType portType,int portNum);
    public int getAvgWaitingRows(int portNum);
    public int getAvgWaitingTime();
    public long getTotalBytes(PortType portType,int portNum);
    public long getTotalCPUTime();
    public long getTotalUserTime();
    public int getTotalRows(PortType portType,int portNum);
    public String getNodeId();
    public String getNodeName();
    public int getPhase();
    public long getTimestamp();
    public int getNumInputPorts();
    public int getNumOutputPorts();
    public Result getResult();
    public float getUsageCPU();
    public float getPeakUsageCPU();
    public float getUsageUser();
    public float getPeakUsageUser();
    
}
