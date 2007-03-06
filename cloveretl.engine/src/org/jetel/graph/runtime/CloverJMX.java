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

import java.util.Map;

import javax.management.AttributeChangeNotification;
import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.jetel.data.Defaults;
import org.jetel.graph.Phase;
import org.jetel.graph.Result;

public class CloverJMX extends NotificationBroadcasterSupport  implements CloverJMXMBean {

    public static final String UPDATE_NOTIFICATION_ID = "Tracking updated";
    public static final String PHASE_UPDATE_NOTIFICATION_ID = "Phase updated";
    public static final String GRAPH_FINISHED_NOTIFICATION_ID = "Graph finished";
    public static final String GRAPH_STARTED_NOTIFICATION_ID = "Graph started";
    
    private long sequenceNumber = 1;
    private String runningGraphName;
    private String cloverVersion;
    private int runingPhase;
    private int runningNodes;
    private long runTimeMS;
    private WatchDog watchDog;
    
    
    private Map<String,TrackingDetail> trackingMap;
    
    public CloverJMX(WatchDog watchDog){
        StringBuilder str=new StringBuilder(30);
      //  str.append(org.jetel.util.JetelVersion.getMajorVersion()).append('.');
      //  str.append(org.jetel.util.JetelVersion.getMinorVersion()).append(" build# ");
      //  str.append(org.jetel.util.JetelVersion.getBuildNumber()).append(" compiled ");
      //  str.append(org.jetel.util.JetelVersion.getBuildDatetime());
        
        cloverVersion=str.toString();
        this.watchDog=watchDog;
        runningGraphName=watchDog.getTransformationGraph().getName();
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

    
    public int[] getPhaseList() {
        Phase[] phases=watchDog.getTransformationGraph().getPhases();
        int[] phaseNums=new int[phases.length];
        for (int i=0;i<phaseNums.length;i++) {
            phaseNums[i]=phases[i].getPhaseNum();
        }
        return phaseNums;
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
    
    public TrackingDetail getTrackingDetail(int phase,String nodeID) {
        return watchDog.getTransformationGraph().getPhase(phase).getTracking().get(nodeID);
    }
    
    public PhaseTrackingDetail getPhaseTracking() {
        return watchDog.getTransformationGraph().getPhase(getRunningPhase()).getPhaseTracking();
    }
    
    public PhaseTrackingDetail getPhaseTracking(int phase) {
        return watchDog.getTransformationGraph().getPhase(phase).getPhaseTracking();
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

    public String[] getNodesList(){
         return trackingMap.keySet().toArray(new String[trackingMap.size()]);
    }
    
    public int getUpdateInterval() {
        // TODO Auto-generated method stub
        return 0;
    }

    public void setUpdateInterval(int updateInterval) {
        // TODO Auto-generated method stub

    }

    public void stopGraphExecution() {
        watchDog.stopRun();

    }
    
    public synchronized void updated() { 
        Notification n = new Notification(UPDATE_NOTIFICATION_ID,
                                this,
                                sequenceNumber++, 
                                System.currentTimeMillis(), 
                                UPDATE_NOTIFICATION_ID); 
 
        sendNotification(n);
    } 
    
    public synchronized void phaseUpdated() { 
        Notification n = new Notification(PHASE_UPDATE_NOTIFICATION_ID, 
                                this, 
                                sequenceNumber++, 
                                System.currentTimeMillis(), 
                                PHASE_UPDATE_NOTIFICATION_ID); 
 
        sendNotification(n); 
    } 
    
    public synchronized void graphFinished(Result result) { 
        Notification n = new Notification(GRAPH_FINISHED_NOTIFICATION_ID, 
                                this, 
                                sequenceNumber++, 
                                System.currentTimeMillis(), 
                                runningGraphName+Defaults.Component.KEY_FIELDS_DELIMITER+result); 
 
        n.setUserData(result);
        sendNotification(n); 
    } 

    public synchronized void graphStarted() { 
        Notification n = new Notification(GRAPH_STARTED_NOTIFICATION_ID, 
                                this, 
                                sequenceNumber++, 
                                System.currentTimeMillis(), 
                                runningGraphName); 
 
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
        phaseUpdated();
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
