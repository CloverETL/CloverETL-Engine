/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
*/
package org.jetel.component;

import java.io.IOException;
import java.util.Arrays;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Partition Component</h3> <!-- Partitions input data into
 * set of partitions (each connected output port becomes one partition.
 * Data is partitioned using different algorithms. Three (RoundRobin,Hash,Rage) are
 * implemented -->
 *
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Partition</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Partitions data into distinct flows. Each connected port becomes
 * one flow. For each record read, the partition function chosen denotes which output
 * flow (port) will be the record sent to.<br>
 * Which partition algorithm becomes active depends on following:
 * <ul>
 * <li>If <code>partitionKey</code> is <b>NOT</b> specified/defined, RoundRobin algorithm is used.
 * <li>If <code>partitionKey</code> <b>IS</b> specified and <b>NO</b> <code>ranges</code> is specified, then
 * partition by calculated hash value is used. The formula used is: <code>hashValue / MAX_HASH_VALUE * #connected_output_ports) MOD #connected_output_ports</code>
 * <li>If <b>BOTH</b> <code>partitionKey</code> and <code>ranges</code> are specified, then partition by
 * range is used - <i>partitionKey's</i> value is sequentially compared with defined range boundaris. If
 * the value is less or equal to specified boundary, then the record is sent out through the port corresponding
 * to that boundary.<br><i>Note: when boundaries are used, only 1 field can be specified as partitionKey.</i>
 * </ul>
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0..n] - one or more output ports connected</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"PARTITION"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>partitionKey</b><br><i>optional</i></td><td>key which specifies which fields (one or more) will be used for calculating hash valu
 * used for determinig output port. If ranges attribute is not defined, then partition method partition by hash will be used.</td>
 * <tr><td><b>ranges</b><br><i>optional</i></td><td>definition of intervals against which
 * partitionKey will be checked. If key's value belongs to interval then interval's order number is converted
 * to output port number. <br>If this option is used, the partitioning is range partitioning and partition key can
 * be composed of ONE field only (actually 2nd and additional fields will be ignored).<br>
 * <i>Note:Boundaries are first sorted in ascending order, then checked.When checking interval boundaries, the
 * &lt;= operator is used.</i></td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="PARTITION_BY_KEY" type="PARTITION" partitionKey="age"/&gt;</pre>
 *  <pre>&lt;Node id="PARTITION_BY_RANGE" type="PARTITION" partitionKey="age" ranges="18;30;60;80"/&gt;</pre>
 *
 * @author      dpavlis
 * @since       February 28, 2005
 * @revision    $Revision$
 */
public class Partition extends Node {

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "PARTITION";

	private final static int READ_FROM_PORT=0;
	
	private String[] partitionKeyNames;
	private String[] partitionRanges;

	private RecordKey partitionKey;
	private HashKey hashKey;

	//	 instantiate proper partitioning function
	private PartitionFunction partitionFce;

	private static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";

	private static final String XML_RANGES_ATTRIBUTE = "ranges";

	/**
	 *  Constructor for the Partition object
	 *
	 * @param  id         Description of the Parameter
	 */
	public Partition(String id) {
		super(id);
		partitionFce=null;
	}


    /**
     * @param partitionKeyNames The partitionKeyNames to set.
     */
    public void setPartitionKeyNames(String[] partitionKeyNames) {
        this.partitionKeyNames = partitionKeyNames;
    }
	
	public void setPartitionRanges(String[] partitionRanges){
	    this.partitionRanges=partitionRanges;
	}
    
	/**
	 * Method which can be used to set custom partitioning function
	 * 
	 * @param fce class implementing PartitionFunction interface
	 */
	public void setPartitionFunction(PartitionFunction fce){
	    this.partitionFce=fce;
	}

	/**
	 *  Main processing method for the SimpleCopy object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		InputPort inPort;
		OutputPort[] outPorts;

		inPort=getInputPort(READ_FROM_PORT);
		//get array of all output ports defined/connected - use collection Collection - getOutPorts();
		outPorts = (OutputPort[]) getOutPorts().toArray(new OutputPort[0]);
		//create array holding incoming records
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		inRecord.init();

		while (inRecord!=null && runIt ) {
		    try{
		        inRecord=inPort.readRecord(inRecord);
		        if (inRecord!=null){
		            outPorts[partitionFce.getOutputPort(inRecord)].writeRecord(inRecord);
		        }
		    } catch (IOException ex) {
		        resultMsg = ex.getMessage();
		        resultCode = Node.RESULT_ERROR;
		        closeAllOutputPorts();
		        return;
		    } catch (Exception ex) {
		        resultMsg = ex.getClass().getName()+" : "+ex.getMessage();
		        resultCode = Node.RESULT_FATAL_ERROR;
		        return;
		    }
		    SynchronizeUtils.cloverYield();
		}
		broadcastEOF();
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		resultCode = Node.RESULT_OK;
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port and one output
		if (inPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one input port has to be defined!");
		} else if (outPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		// initialize partition key - if defined
		if (partitionKeyNames!=null){
		    partitionKey = new RecordKey(partitionKeyNames, getInputPort(0).getMetadata());
		    try {
		        partitionKey.init();
		    } catch (Exception e) {
		        throw new ComponentNotReadyException(e.getMessage());
		    }
		}
		if (partitionFce==null){
		    if (partitionKey!=null){
			    if (partitionRanges!=null){
			        partitionFce=new RangePartition(partitionRanges);
			    }else{
			        partitionFce=new HashPartition();
			    }
			}else{
			    partitionFce=new RoundRobinPartition(); 
			}
		}
		partitionFce.init(outPorts.size(),partitionKey);
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);

		if (partitionKeyNames != null) {
			StringBuffer buf = new StringBuffer(partitionKeyNames[0]);
			for (int i=1; i<partitionKeyNames.length; i++) {
			buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + partitionKeyNames[i]);
			}
			
			xmlElement.setAttribute(XML_PARTITIONKEY_ATTRIBUTE,buf.toString());
		}
		
		if (partitionRanges != null) {
			StringBuffer buf = new StringBuffer(partitionRanges[0]);
			for (int i=1; i<partitionRanges.length; i++) {
				buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + partitionRanges[i]);
			}
			
			xmlElement.setAttribute(XML_RANGES_ATTRIBUTE,buf.toString());
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		Partition partition;
		
		try {
		    partition = new Partition(xattribs.getString(Node.XML_ID_ATTRIBUTE));
		    if (xattribs.exists(XML_PARTITIONKEY_ATTRIBUTE)){
					partition.setPartitionKeyNames(xattribs.getString(XML_PARTITIONKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
		    }
		    if (xattribs.exists(XML_RANGES_ATTRIBUTE)) {
		        partition.setPartitionRanges(xattribs.getString(XML_RANGES_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
		    }
		    return partition;
		} catch (Exception ex) {
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
	public boolean checkConfig() {
		return true;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}

	
	
	
	
	/**
	 * RoundRobin partition algorithm.
	 * 
	 * @author david
	 * @since  1.3.2005
	 *
	 */
	private static class RoundRobinPartition implements PartitionFunction{
	    int last;
	    int numPorts;
	    
	    RoundRobinPartition(){
	    }
	    
	    public void init(int numPartitions,RecordKey partitionKey){
	        this.numPorts=numPartitions;
	        this.last=-1;
	    }
	    
	    public int getOutputPort(DataRecord record){
	        last=(last+1)%numPorts;
	        return last;
	    }
	    
	}
	
	/**
	 * Partition algorithm based on calculating hash value of
	 * specified key. The hash is then split to intervals. Number of
	 * intervals is based specified number
	 *  
	 * 
	 * @author david
	 * @since  1.3.2005
	 */
	private static class HashPartition implements PartitionFunction{
	    int numPorts;
	    HashKey hashKey;
	    
	    HashPartition(){
	    }
	    
	    public void init(int numPartitions, RecordKey partitionKey){
	        this.numPorts=numPartitions;
	        hashKey=new HashKey(partitionKey,null);
	    }
	    
	    public int getOutputPort(DataRecord record){
	        hashKey.setDataRecord(record);
	        //int hash=hashKey.hashCode(); 
	        //int value=(hash)&0x0FF;//// take only last 8 bits
	        return hashKey.hashCode()%numPorts;
	    }
	}
	
	/**
	 * Partition algorithm which compares current key value with set of
	 * intervals (defined as a set of interval upper limits - inclusive).
	 * 
	 * @author david
	 * @since  5.3.2005
	 *
	 */
	private static class RangePartition implements PartitionFunction{
	    int numPorts;
	    DataField[] boundaries;
	    int[] keyFields;
	    String[] boundariesStr;
	    
	    /**
	     * @param boundariesStr array of strings containing definitions of ranges boundaries
	     * @param record	DataRecord with the same structure as the one which will be used
	     * for determining output port.
	     */
	    RangePartition(String[] boundariesStr){
	        this.boundariesStr=boundariesStr;
	    }
	    
	    public void init(int numPartitions, RecordKey partitionKey){
	        this.numPorts=numPartitions;
	        keyFields=partitionKey.getKeyFields();

	    }
	    
	    public int getOutputPort(DataRecord record){
	        // create boundaries the first time this method is called
	        if (boundaries==null){
	            boundaries = new DataField[boundariesStr.length];
		        for (int i=0;i<boundaries.length;i++){
		            boundaries[i]=record.getField(keyFields[0]).duplicate();
		            boundaries[i].fromString(boundariesStr[i]);
		        }
		        Arrays.sort(boundaries);
	        }
	        
	        // use sequential search if number of boundries is small
	        if (boundaries.length <= 6) {
                for (int i = 0; i < numPorts && i < boundaries.length; i++) {
                    // current boundary is upper limit inclusive
                    if (record.getField(keyFields[0]).compareTo(boundaries[i]) <= 0) {
                        return i;
                    }
                }
                return numPorts - 1;
            } else {
                int index = Arrays.binarySearch(boundaries, record
                        .getField(keyFields[0]));
                // DEBUG
                // System.out.println("Index partition: "+index+" value "+record
                //         .getField(keyFields[0]).toString());
                // DEBUG END
                if (index>=0){
                    return index;
                }else{
                    return (index*-1)-1;
                }
            }
	    }
	}
	
}

