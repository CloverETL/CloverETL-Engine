/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002  David Pavlis
*
*    This program is free software; you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation; either version 2 of the License, or
*    (at your option) any later version.
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.jetel.component;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import org.w3c.dom.NamedNodeMap;
import org.jetel.graph.*;
import org.jetel.data.DataRecord;
import org.jetel.data.FileRecordBuffer;
import org.jetel.data.RecordKey;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.ComponentXMLAttributes;

/**
 *  <h3>Sorted Join Component</h3>
 *
 * <!-- Changes / reformats the data between pair of INPUT/OUTPUT ports
 *  This component is only a wrapper around transformation class implementing
 *  org.jetel.component.RecordTransform interface. The method transform
 *  is called for every record passing through this component -->
 *
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>SortedJoin</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Changes / reformats the data between pair of INPUT/OUTPUT ports.<br>
 *  This component is only a wrapper around transformation class implementing
 *  <i>org.jetel.component.RecordTransform</i> interface. The method <i>transform</i>
 *  is called for every record passing through this component.<br></td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0..n]- input records (min 2 ports active)</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] - one output port</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>  
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"SORTED_JOIN"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>transformClass</b></td><td>name of the class to be used for transforming joined data</td>
 *  </tr>
 *  </table>  
 *
 *  <h4>Example:</h4> 
 *  <pre>&lt;Node id="JOIN" type="SORTED_JOIN" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 * @author     dpavlis
 * @since    April 4, 2002
 */
public class SortedJoin extends Node {

	public static final String COMPONENT_TYPE="SORTED_JOIN";
	
	private static final int WRITE_TO_PORT = 0;
	private static final int DRIVER_ON_PORT = 0;
	private static final int SLAVE_ON_PORT = 1;
	
	private String transformClassName;
	
	private RecordTransform transformation=null;
	
	private DataRecord[] joinedRecords;
	
	private boolean[] inputDataRequired;
	
	private String[] joinKeys;
	
	private RecordKey recordKeys[];
	
	private ByteBuffer dataBuffer;
	private FileRecordBuffer recordBuffer;
	
	public SortedJoin(String id,String[] joinKeys, String transformClass /*boolean[] inputDataRequired*/){
		super(id);
		this.joinKeys=joinKeys;
		this.transformClassName=transformClass;
		//this.inputDataRequired=inputDataRequired;
		//throw new RuntimeException("SortedJoin component not finished");
	}
	
	public SortedJoin(String id,String[] joinKeys, RecordTransform transformClass, boolean[] inputDataRequired){
		super(id);
		this.joinKeys=joinKeys;
		this.transformation=transformClass;
		this.inputDataRequired=inputDataRequired;
		//throw new RuntimeException("SortedJoin component not finished");
	}
	

	/**
	 *  Gets the Type attribute of the SimpleCopy object
	 *
	 * @return    The Type value
	 * @since     April 4, 2002
	 */
	public String getType() {
		return COMPONENT_TYPE;
	}

	
	private void fillRecordBuffer(InputPort port,DataRecord curRecord,DataRecord nextRecord,RecordKey key)
		throws IOException,InterruptedException{
			
			recordBuffer.clear();
			if(curRecord!=null){
				dataBuffer.clear();
				curRecord.serialize(dataBuffer);
				dataBuffer.flip();
				recordBuffer.push(dataBuffer);
				while(nextRecord!=null){
					nextRecord=port.readRecord(nextRecord);
					if (nextRecord!=null){
						if(key.compare(curRecord,nextRecord)==0){
							dataBuffer.clear();
							nextRecord.serialize(dataBuffer);
							dataBuffer.flip();
							recordBuffer.push(dataBuffer);
						}else{
							return;
						}
					}
				}
			}
	}
	
	
	private int getCorrespondingRecord(DataRecord driver,DataRecord slave,InputPort slavePort,RecordKey key[])
		throws IOException,InterruptedException{
		
		while(slave!=null){
			switch(key[DRIVER_ON_PORT].compare(key[SLAVE_ON_PORT],driver,slave)){
				case 1: slave=slavePort.readRecord(slave);
					break;
				case 0:	return 0;
				case -1: return -1;
			}
		}
		return -1; // no more records on slave port
	}
	
	private boolean flushCombinations(DataRecord driver,DataRecord slave,DataRecord out,OutputPort port)
		throws IOException,InterruptedException{
		DataRecord inRecords[]={driver,slave};
		recordBuffer.rewind();
		dataBuffer.clear();
		
		while(recordBuffer.shift(dataBuffer)!=null){
			dataBuffer.flip();
			slave.deserialize(dataBuffer);
			// call transform function here
			if (!transformation.transform(inRecords,out))
			{
				resultMsg=transformation.getMessage();
				return false;
			}
			// TODO::::
			//port.writeRecord(out);
			dataBuffer.clear();
		}
		return true;
	}
	
	
	

	/**
	 *  Main processing method for the SimpleCopy object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		ByteBuffer data = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		boolean isDifferent;
		// get all ports involved
		InputPort driverPort=getInputPort(DRIVER_ON_PORT);
		InputPort slavePort=getInputPort(SLAVE_ON_PORT);
		OutputPort outPort=getOutputPort(WRITE_TO_PORT);
		
		//initialize input records & output record
		DataRecord driverRecord=new DataRecord(driverPort.getMetadata());
		DataRecord driverNextRecord=new DataRecord(driverPort.getMetadata());
		DataRecord slaveRecord=new DataRecord(slavePort.getMetadata());
		DataRecord slaveNextRecord=new DataRecord(slavePort.getMetadata());
		//DataRecord outRecord=new DataRecord(outPort.getMetadata());
		DataRecord outRecord=driverRecord; // only work around
		driverRecord.init();
		slaveRecord.init();
		slaveNextRecord.init();
		driverNextRecord.init();
		outRecord.init();
		
		// create buffer for slave records
		recordBuffer=new FileRecordBuffer(null); // systen TEMP path
		//for the first time, we expect that records are different
		isDifferent=true;
		
		try{
			// first initial load of records
			driverRecord=driverPort.readRecord(driverRecord);
			slaveRecord=slavePort.readRecord(slaveRecord);
			while(runIt && driverRecord!=null){
				
				if (isDifferent){
					if (getCorrespondingRecord(driverRecord,slaveRecord,slavePort,recordKeys)!=0){
						driverRecord=driverPort.readRecord(driverRecord);
						isDifferent=true;
						continue;
					}else{
						fillRecordBuffer(slavePort,slaveRecord,slaveNextRecord,recordKeys[SLAVE_ON_PORT]);
						isDifferent=false;
					}
				}
				flushCombinations(driverRecord,slaveRecord,outRecord,outPort);
				driverNextRecord=driverPort.readRecord(driverNextRecord);
				// different driver record ??
				if(recordKeys[DRIVER_ON_PORT].compare(driverRecord,driverNextRecord)!=0){
					// detected change;
					isDifferent=true;
				}
				DataRecord tmpRec=driverRecord;
				driverRecord=driverNextRecord;
				driverNextRecord=tmpRec;
			
			}
		}catch(IOException ex){
			resultMsg=ex.getMessage();
			resultCode=Node.RESULT_ERROR;
			closeAllOutputPorts();
			return;
		}catch(Exception ex){
			resultMsg=ex.getMessage();
			resultCode=Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
			return;
		}
		
		broadcastEOF();
		if (runIt) resultMsg="OK"; else resultMsg="STOPPED";
		resultCode=Node.RESULT_OK;
	}	


	/**
	 *  Description of the Method
	 *
	 * @since    April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		Class tClass;
		// test that we have at least one input port and one output
		if (inPorts.size()<2){
			throw new ComponentNotReadyException("At least two input ports have to be defined!");
		}else if (outPorts.size()<0){ // TODO:
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		// allocate array for joined records (this array will be passed to reformat function)
		joinedRecords=new DataRecord[inPorts.size()];
		recordKeys=new RecordKey[2];
		recordKeys[0]=new RecordKey(joinKeys,getInputPort(0).getMetadata());
		recordKeys[1]=new RecordKey(joinKeys,getInputPort(1).getMetadata());
		recordKeys[0].init();
		recordKeys[1].init();
		
		if(transformation==null){
			// try to load in transformation class & instantiate
			try{
				tClass = Class.forName(transformClassName);
			}catch(ClassNotFoundException ex){
				throw new ComponentNotReadyException("Can't find specified transformation class: "+transformClassName);
			}catch(Exception ex){
				throw new ComponentNotReadyException(ex.getMessage());
			}
			try{
				transformation=(RecordTransform)tClass.newInstance();
			}catch(Exception ex){
				throw new ComponentNotReadyException(ex.getMessage());
			}
                   
		}
		// init transformation
		Collection col=getInPorts();
		DataRecordMetadata[] inMetadata=new DataRecordMetadata[col.size()];
		int counter=0;
		Iterator i=col.iterator();
		while(i.hasNext()){
			inMetadata[counter++]=((InputPort)i.next()).getMetadata();
		}
		// put aside: getOutputPort(WRITE_TO_PORT).getMetadata()
		if (!transformation.init(inMetadata,null))
		{
			throw new ComponentNotReadyException("Error when initializing reformat function !");
		}
		dataBuffer=ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public org.w3c.dom.Node toXML() {
		// TODO
		return null;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	public static Node fromXML(org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML);

		try{
			return new SortedJoin(xattribs.getString("id"),
				xattribs.getString("joinKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
				xattribs.getString("transformClass"));
		}catch(Exception ex){
			System.err.println(ex.getMessage());
			return null;
		}
	}
		
	
}

