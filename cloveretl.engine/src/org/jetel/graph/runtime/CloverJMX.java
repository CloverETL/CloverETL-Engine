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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.management.*; 

public class CloverJMX extends NotificationBroadcasterSupport  implements CloverJMXMBean {

    private long sequenceNumber = 1;
    private String runningGraphName;
    private String cloverVersion;
    private int runingPhase;
    private int runningNodes;
    private long runTimeMS;
    
    
    private Map<String,TrackingDetail> trackingMap;
    
    public CloverJMX(){
        StringBuilder str=new StringBuilder(30);
      //  str.append(org.jetel.util.JetelVersion.getMajorVersion()).append('.');
      //  str.append(org.jetel.util.JetelVersion.getMinorVersion()).append(" build# ");
      //  str.append(org.jetel.util.JetelVersion.getBuildNumber()).append(" compiled ");
      //  str.append(org.jetel.util.JetelVersion.getBuildDatetime());
        
        cloverVersion=str.toString();
    }
    
   
    public String getCloverVersion() {
       return cloverVersion;
    }

    public String getRunningGraphName() {
        return runningGraphName;
    }

    public long getRunningGraphTime() {
        return runTimeMS;
    }

    public int getRunningPhase() {
        return runingPhase;
    }

    public int getRunningNodesCount() {
        return runningNodes;
    }

    public TrackingDetail getTrackingDetail(String nodeID){
        return trackingMap.get(nodeID);
        
    }
    
    public String getTrackingDetailString(String nodeID){
        TrackingDetail detail=trackingMap.get(nodeID);
        if (detail!=null){
            StringBuilder str=new StringBuilder(60);
            str.append(detail.getNodeId()).append(';');
            str.append(detail.getNumInputPorts()).append(';');
            str.append(detail.getNumOutputPorts()).append(';');
            for(int i=0;i<detail.getNumInputPorts();i++){
                str.append(detail.getTotalRows(TrackingDetail.IN_PORT, i)).append(';');
            }
            for(int i=0;i<detail.getNumOutputPorts();i++){
                str.append(detail.getTotalRows(TrackingDetail.OUT_PORT, i)).append(';');
            }
            return str.toString();
        }
        return "";
    }

    public List getNodesList(){
       return new LinkedList(trackingMap.keySet());
        // return (String[])trackingMap.keySet().toArray(new String[trackingMap.size()]);
    }
    
    public int getUpdateInterval() {
        // TODO Auto-generated method stub
        return 0;
    }

    public void setUpdateInterval(int updateInterval) {
        // TODO Auto-generated method stub

    }

    public void stopGraphExecution() {
        // TODO Auto-generated method stub

    }
    
    public synchronized void updated() { 
      
        System.out.println("MBEAN was updetd"); 
 
        Notification n = 
            new AttributeChangeNotification(this, 
                        sequenceNumber++, 
                        System.currentTimeMillis(), 
                        "Tracking updated", 
                        "Tracking", 
                        "int", 
                        -1/*oldSize*/, 
                        1/*this.cacheSize*/); 
 
    sendNotification(n); 
    } 
 
    @Override 
    public MBeanNotificationInfo[] getNotificationInfo() { 
        String[] types = new String[] { 
            AttributeChangeNotification.ATTRIBUTE_CHANGE 
        }; 
        String name = AttributeChangeNotification.class.getName(); 
        String description = "An attribute of this MBean has changed"; 
        MBeanNotificationInfo info = 
            new MBeanNotificationInfo(types, name, description); 
        return new MBeanNotificationInfo[] {info}; 
    }

    /**
     * @param trackingMap the trackingMap to set
     * @since 17.1.2007
     */
    public void setTrackingMap(Map<String, TrackingDetail> trackingMap) {
        this.trackingMap = trackingMap;
    }

    /**
     * @param runingPhase the runingPhase to set
     * @since 17.1.2007
     */
    public void setRuningPhase(int runingPhase) {
        this.runingPhase = runingPhase;
    }

    /**
     * @param runningGraphName the runningGraphName to set
     * @since 17.1.2007
     */
    public void setRunningGraphName(String runningGraphName) {
        this.runningGraphName = runningGraphName;
    }

    /**
     * @param runTimeSec the runTimeSec to set
     * @since 17.1.2007
     */
    public void setRunTime(long runTimeSec) {
        this.runTimeMS = runTimeSec;
    }


    /**
     * @param runningNodes the runningNodes to set
     * @since 17.1.2007
     */
    public void setRunningNodes(int runningNodes) {
        this.runningNodes = runningNodes;
    } 

}
