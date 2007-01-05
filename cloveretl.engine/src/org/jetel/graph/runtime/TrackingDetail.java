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

public class TrackingDetail {

    public static final int IN_PORT=0;
    public static final int OUT_PORT=1;
    
    private String nodeId;
    private long timestamp;
    private int timespan;
    private Node.Result result;
    
    private int numInputPorts;
    private int numOutputPorts;
    private int totalRows[][];
    private long totalBytes[][];
    private int avgRows[][];
    private int avgBytes[][];
    private long totalCPUTime;
    private long totalUserTime;
    private float usageCPU;
    private float usageUser;
    private int avgWaitingTime;
    private int waitingRows[];
    private int avgWaitingRows[];
    
    public TrackingDetail(String id,int inputPorts,int outputPorts){
        this.nodeId=id;
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
        /*Arrays.fill(totalRows,0);
        Arrays.fill(totalBytes,0);
        Arrays.fill(avgRows,0);
        Arrays.fill(avgBytes, 0);
        Arrays.fill(waitingRows, 0);
        Arrays.fill(avgWaitingRows, 0);*/
    }
    
    @Override public boolean equals(Object obj){
        if (obj instanceof TrackingDetail){
            return ((TrackingDetail)obj).nodeId.equals(nodeId);
        }
        return false;
    }
    
    @Override public int hashCode(){
        return nodeId.hashCode();
    }
    
    /**
     * @return the avgBytes
     * @since 4.1.2007
     */
    public int getAvgBytes(int portType,int portNum) {
        return avgBytes[portType][portNum];
    }
    /**
     * @return the avgRows
     * @since 4.1.2007
     */
    public int getAvgRows(int portType,int portNum) {
        return avgRows[portType][portNum];
    }
    /**
     * @return the avgWaitingRows
     * @since 4.1.2007
     */
    public int getAvgWaitingRows(int portNum) {
        return avgWaitingRows[portNum];
    }
    /**
     * @return the avgWaitingTime
     * @since 4.1.2007
     */
    public int getAvgWaitingTime() {
        return avgWaitingTime;
    }
    /**
     * @return the totalBytes
     * @since 4.1.2007
     */
    public long getTotalBytes(int portType,int portNum) {
        return totalBytes[portType][portNum];
    }
    /**
     * @return the totalCPUTime
     * @since 4.1.2007
     */
    public long getTotalCPUTime() {
        return totalCPUTime;
    }
    
    /**
     * @return the totalUserTime
     * @since 4.1.2007
     */
    public long getTotalUserTime() {
        return totalUserTime;
    }
    
    /**
     * @return the totalRows
     * @since 4.1.2007
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
        time=userTime;
        usageUser=(float)time/systemTime;
        if (cpuTime<0) return;
        totalCPUTime=cpuTime;
        totalUserTime=userTime;
    }
    
    public void updateWaitingRows(int portNum,int rows){
        avgWaitingRows[portNum]=Math.abs(rows-avgWaitingRows[portNum])/2;
        waitingRows[portNum]=rows;
    }

    /**
     * @return the id
     * @since 4.1.2007
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * @return the timestamp
     * @since 4.1.2007
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @return the inputPorts
     * @since 4.1.2007
     */
    public int getNumInputPorts() {
        return numInputPorts;
    }

    /**
     * @return the outputPorts
     * @since 4.1.2007
     */
    public int getNumOutputPorts() {
        return numOutputPorts;
    }

    /**
     * @return the result
     * @since 4.1.2007
     */
    public Node.Result getResult() {
        return result;
    }

    /**
     * @param result the result to set
     * @since 4.1.2007
     */
    public void setResult(Node.Result result) {
        this.result = result;
    }

    /**
     * @return the usageCPU
     * @since 4.1.2007
     */
    public float getUsageCPU() {
        return usageCPU;
    }

    /**
     * @return the usageUser
     * @since 5.1.2007
     */
    public float getUsageUser() {
        return usageUser;
    }
    
}
