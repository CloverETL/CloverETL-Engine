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
	
	private String transformClassName;
	
	private RecordTransform transformation=null;
	
	private DataRecord[] joinedRecords;
	
	private boolean[] inputDataRequired;
	
	private String[] joinKeys;
	
	private RecordKey recordKeys[];
	
	public SortedJoin(String id,String[] joinKeys, String transformClass, boolean[] inputDataRequired){
		super(id);
		this.joinKeys=joinKeys;
		this.transformClassName=transformClass;
		this.inputDataRequired=inputDataRequired;
		throw new RuntimeException("SortedJoin component not finished");
	}
	
	public SortedJoin(String id,String[] joinKeys, RecordTransform transformClass, boolean[] inputDataRequired){
		super(id);
		this.joinKeys=joinKeys;
		this.transformation=transformClass;
		this.inputDataRequired=inputDataRequired;
		throw new RuntimeException("SortedJoin component not finished");
	}
	/**
	 *  Gets the Status attribute of the SimpleCopy object
	 *
	 * @return    The Status value
	 * @since     April 4, 2002
	 */
	public String getStatus() {
		return "OK";
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

	
	private void fillRBuffer(FileRecordBuffer buffer,ByteBuffer data,DataRecord record,RecordKey key,InputPort port)
		throws IOException,InterruptedException{
			DataRecord next=new DataRecord(record.getMetadata());
			next.init();
			
			while(next!=null){
				next=port.readRecord(next);
				if (next!=null){
					if(key.compare(record,next)==0){
						next.serialize(data);
						buffer.push(data);
					}else{
					}
				}
			}
			
		
	}
	
	
	private boolean matchAB(DataRecord a,DataRecord b,InputPort portA,InputPort portB,RecordKey rkA,RecordKey rkB)
		throws IOException,InterruptedException{
		// these two lines probably not neccessary
		//a=portA.readRecord(a);
		//b=portB.readRecord(b);
		while(a!=null && b!=null){
			switch(rkA.compare(rkB,a,b)){
				case -1: a=portA.readRecord(a);
					break;
				case 0:	return true;
				case 1: b=portB.readRecord(b);
					break;
			}
		}
		return false;
	}
	
	private boolean flushCombinations(DataRecord a,DataRecord b,DataRecord out,FileRecordBuffer buffer,OutputPort port)
		throws IOException,InterruptedException{
		DataRecord[] inRecords={a,b};
		ByteBuffer data=null;
		buffer.rewind();
		
		while(buffer.shift(data)!=null){
			b.deserialize(data);
			// call transform function here
			if (!transformation.transform(inRecords,out))
			{
				resultMsg=transformation.getMessage();
				return false;
			}
			port.writeRecord(out);
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
		InputPort driverPort=getInputPort(DRIVER_ON_PORT);
		InputPort dataPort=getInputPort(1);
		OutputPort outPort=getOutputPort(WRITE_TO_PORT);
		DataRecord driverRecord=new DataRecord(driverPort.getMetadata());
		DataRecord dataRecord=new DataRecord(dataPort.getMetadata());
		DataRecord outRecord=new DataRecord(outPort.getMetadata());
		driverRecord.init();
		dataRecord.init();
		outRecord.init();
		DataRecord[] inRecords={driverRecord,dataRecord};
		FileRecordBuffer fbuffer=new FileRecordBuffer(""); // curr path

		
		try{
			// first initial load of records
			driverRecord=driverPort.readRecord(driverRecord);
			dataRecord=dataPort.readRecord(dataRecord);
			while(runIt){
				if (driverRecord!=null&&dataRecord!=null){
					// compare records
					switch(recordKeys[0].compare(recordKeys[1],driverRecord,dataRecord)){
						case 0: /* match */
							// fill in temp buffer with the same records from data (port[1])
							
						
							if (!transformation.transform(inRecords,outRecord))
							{
								resultMsg=transformation.getMessage();
								break;
							}
							outPort.writeRecord(outRecord);
							driverRecord=driverPort.readRecord(driverRecord);
							dataRecord=dataPort.readRecord(dataRecord);
						break;
						case 1: /* driver greater */
							dataRecord=dataPort.readRecord(dataRecord);
							break;
						case -1: /* driver lover */
							driverRecord=driverPort.readRecord(driverRecord);
							break;
					}
				}else{
					break;
				}
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
		}else if (outPorts.size()<1){
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
		if (!transformation.init(inMetadata,getOutputPort(WRITE_TO_PORT).getMetadata()))
		{
			throw new ComponentNotReadyException("Error when initializing reformat function !");
		}
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
		NamedNodeMap attribs=nodeXML.getAttributes();
		
		if (attribs!=null){
			String id=attribs.getNamedItem("id").getNodeValue();
			String className=attribs.getNamedItem("transformClass").getNodeValue();
			String keyStr=attribs.getNamedItem("joinKey").getNodeValue();
			if (id!=null){
				return new SortedJoin(id,keyStr.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),className,null);
			}
		}
		return null;
	}
	
}

