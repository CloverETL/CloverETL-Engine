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

import java.io.Serializable;

import org.jetel.graph.Result;

public class NodeTrackingDetail implements TrackingDetail, Serializable {

    
    private static final long serialVersionUID = 9164050461393378702L;
    
    private String nodeId;
    private String nodeName;
    private long timestamp;
    private int timespan;
    private Result result;
    
    private int numInputPorts;
    private int numOutputPorts;
    private int totalRows[][];
    private long totalBytes[][];
    private int avgRows[][];
    private int avgBytes[][];
    private long totalCPUTime;
    private long totalUserTime;
    private float usageCPU;
    private float peakUsageCPU;
    private float peakUsageUser;
    private float usageUser;
    private int avgWaitingTime;
    private int waitingRows[];
    private int avgWaitingRows[];
    private int phase;
    
    public NodeTrackingDetail(String id,String name,int phase,int inputPorts,int outputPorts){
        this.nodeId=id;
        this.nodeName=name;
        this.phase=phase;
        this.numInputPorts=inputPorts;
        this.numOutputPorts=outputPorts;
        final int ports=Math.max(inputPorts, outputPorts);
        totalRows=new int[2][ports];
        totalBytes=new long[2][ports];
        avgRows=new int[2][ports];
        avgBytes=new int[2][ports];
        waitingRows=new int[outputPorts];
        avgWaitingRows=new int[outputPorts];
    }
    
    public void clear(){
        timestamp=timespan=0;
        totalUserTime=totalCPUTime=0;
        peakUsageCPU=peakUsageUser=0;
        /*Arrays.fill(totalRows,0);
        Arrays.fill(totalBytes,0);
        Arrays.fill(avgRows,0);
        Arrays.fill(avgBytes, 0);
        Arrays.fill(waitingRows, 0);
        Arrays.fill(avgWaitingRows, 0);*/
    }
    
    @Override public boolean equals(Object obj){
        if (obj instanceof NodeTrackingDetail){
            return ((NodeTrackingDetail)obj).nodeId.equals(nodeId);
        }
        return false;
    }
    
    @Override public int hashCode(){
        return nodeId.hashCode();
    }
    
    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getAvgBytes(int, int)
     */
    public int getAvgBytes(int portType,int portNum) {
        return avgBytes[portType][portNum];
    }
    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getAvgRows(int, int)
     */
    public int getAvgRows(int portType,int portNum) {
        return avgRows[portType][portNum];
    }
    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getAvgWaitingRows(int)
     */
    public int getAvgWaitingRows(int portNum) {
        return avgWaitingRows[portNum];
    }
    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getAvgWaitingTime()
     */
    public int getAvgWaitingTime() {
        return avgWaitingTime;
    }
    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getTotalBytes(int, int)
     */
    public long getTotalBytes(int portType,int portNum) {
        return totalBytes[portType][portNum];
    }
    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getTotalCPUTime()
     */
    public long getTotalCPUTime() {
        return totalCPUTime;
    }
    
    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getTotalUserTime()
     */
    public long getTotalUserTime() {
        return totalUserTime;
    }
    
    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getTotalRows(int, int)
     */
    public int getTotalRows(int portType,int portNum) {
        return totalRows[portType][portNum];
    }
    
    public void timestamp(){
        long newtime=System.currentTimeMillis();
        timespan=(int)(newtime-timestamp);
        timestamp=newtime;
    }
    
    public void updateRows(int portType,int portNum,int rows){
        avgRows[portType][portNum]=(int)((rows-totalRows[portType][portNum])*1000/timespan);
        totalRows[portType][portNum]=rows;
    }

    public void updateBytes(int portType,int portNum,long bytes){
        avgBytes[portType][portNum]=(int)((bytes-totalBytes[portType][portNum])*1000/timespan);
        totalBytes[portType][portNum]=bytes;
    }
    
    public void updateRunTime(long cpuTime,long userTime,long systemTime){
        double time=cpuTime;
        usageCPU=(float)time/systemTime;
        if (usageCPU>peakUsageCPU) peakUsageCPU=usageCPU;
        time=userTime;
        usageUser=(float)time/systemTime;
        if (usageUser>peakUsageUser) peakUsageUser=usageUser;
        if (cpuTime<0) return;
        totalCPUTime=cpuTime;
        totalUserTime=userTime;
    }
    
    public void updateWaitingRows(int portNum,int rows){
        avgWaitingRows[portNum]=Math.abs(rows-avgWaitingRows[portNum])/2;
        waitingRows[portNum]=rows;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getNodeId()
     */
    public String getNodeId() {
        return nodeId;
    }

    public String getNodeName() {
        return nodeName;
    }
    
    public int getPhase() {
        return phase;
    }
    
    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getTimestamp()
     */
    public long getTimestamp() {
        return timestamp;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getNumInputPorts()
     */
    public int getNumInputPorts() {
        return numInputPorts;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getNumOutputPorts()
     */
    public int getNumOutputPorts() {
        return numOutputPorts;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getResult()
     */
    public Result getResult() {
        return result;
    }

    /**
     * @param result the result to set
     * @since 4.1.2007
     */
    public void setResult(Result result) {
        this.result = result;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getUsageCPU()
     */
    public float getUsageCPU() {
        return usageCPU;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.GraphTrackingDetail#getUsageUser()
     */
    public float getUsageUser() {
        return usageUser;
    }

    /**
     * @return the peakUsageCPU
     * @since 23.1.2007
     */
    public float getPeakUsageCPU() {
        return peakUsageCPU;
    }

    /**
     * @return the peakUsageUser
     * @since 23.1.2007
     */
    public float getPeakUsageUser() {
        return peakUsageUser;
    }
    
}
