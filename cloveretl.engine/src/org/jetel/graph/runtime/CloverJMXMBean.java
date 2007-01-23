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
 * Created on 9.1.2007
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.graph.runtime;

import java.util.List;

public interface CloverJMXMBean {
  
    public String getCloverVersion();
    
    public int getUpdateInterval();
    public void setUpdateInterval(int updateInterval);
    
    public int getRunningPhase();
    public String getRunningGraphName();
    public long getRunningGraphTime();
    
    public int getRunningNodesCount();
    public String[] getNodesList();
    
    public TrackingDetail getTrackingDetail(String nodeID);
    public String getTrackingDetailString(String nodeID);
    
    public void stopGraphExecution();
    
}
