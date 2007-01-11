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
package org.jetel.graph.runtime;

import java.util.Arrays;

import org.jetel.graph.Node;
import org.jetel.graph.Result;

public interface TrackingDetail {

    public static final int IN_PORT=0;
    public static final int OUT_PORT=1;
    
    public int getAvgBytes(int portType,int portNum);
    public int getAvgRows(int portType,int portNum);
    public int getAvgWaitingRows(int portNum);
    public int getAvgWaitingTime();
    public long getTotalBytes(int portType,int portNum);
    public long getTotalCPUTime();
    public long getTotalUserTime();
    public int getTotalRows(int portType,int portNum);
    public String getNodeId();
    public long getTimestamp();
    public int getNumInputPorts();
    public int getNumOutputPorts();
    public Result getResult();
    public float getUsageCPU();
    public float getUsageUser();
    
}
