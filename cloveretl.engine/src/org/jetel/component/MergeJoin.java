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
import org.jetel.exception.JetelException;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.DynamicJavaCode;

/**
 *  <h3>Sorted Join Component</h3> <!-- Changes / reformats the data between
 *  pair of INPUT/OUTPUT ports This component is only a wrapper around
 *  transformation class implementing org.jetel.component.RecordTransform
 *  interface. The method transform is called for every record passing through
 *  this component -->
 *  <table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>MergeJoin</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *      <td>
 *        Joins sorted records on input ports. It expects that on port [0], there is a
 *	driver and on port [1] is slave<br>
 *	For each driver record, all slave records with corresponding key are found and
 *	sent to transformation class.<br>
 *	The method <i>transform</i> is called for every pair of driver&amps;slave.<br>
 *	It skips driver records for which there is no corresponding slave - unless outer
 *	join is specified, when only driver record is passed to <i>transform<i> method.
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0] - driver records<br>
 *	  [1] - slave records<br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - one output port
 *      </td></tr>
 *    <tr><td><h4><i>Comment:</i> </h4>
 *      </td>
 *      <td></td>
 *    </tr>
 *  </table>
 *  <br>
 *  <table border="1">
 *    <th>XML attributes:</th>
 *    <tr><td><b>type</b></td><td>"MERGE_JOIN"</td></tr>
 *    <tr><td><b>id</b></td><td>component identification</td></tr>
 *    <tr><td><b>joinKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *    <tr><td><b>slaveOverrideKey</b><br><i>optional</i></td><td>field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *    <tr><td><b>transformClass</b></td><td>name of the class to be used for transforming joined data</td></tr>
 *    <tr><td><b>leftOuterJoin</b><br><i>optional</i></td><td>true/false</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="JOIN" type="SORTED_JOIN" joinKey="CustomerID" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *
 * @author      dpavlis
 * @since       April 4, 2002
 * @revision    $Revision$
 * @created     4. June 2003
 */
public class MergeJoin extends Node {

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "MERGE_JOIN";

	private final static int WRITE_TO_PORT = 0;
	private final static int DRIVER_ON_PORT = 0;
	private final static int SLAVE_ON_PORT = 1;

	private final static int CURRENT = 0;
	private final static int PREVIOUS = 1;
	private final static int TEMPORARY = 1;

	private String transformClassName;

	private RecordTransform transformation = null;
	private DynamicJavaCode dynamicTransformation = null;

	private DataRecord[] joinedRecords;

	private boolean leftOuterJoin = false;

	private String[] joinKeys;
	private String[] slaveOverrideKeys = null;

	private RecordKey recordKeys[];

	private ByteBuffer dataBuffer;
	private FileRecordBuffer recordBuffer;

	// for passing data records into transform function
	private final static DataRecord[] inRecords = new DataRecord[2];


	/**
	 *  Constructor for the SortedJoin object
	 *
	 * @param  id              id of component
	 * @param  joinKeys        field names composing key
	 * @param  transformClass  class (name) to be used for transforming data
	 */
	public MergeJoin(String id, String[] joinKeys, String transformClass) {
		super(id);
		this.joinKeys = joinKeys;
		this.transformClassName = transformClass;
		this.leftOuterJoin = false;
		// no outer join
	}


	/**
	 *  Constructor for the SortedJoin object
	 *
	 * @param  id              id of component
	 * @param  joinKeys        field names composing key
	 * @param  transformClass  class (name) to be used for transforming data
	 * @param  leftOuterJoin   indicates, whether to perform left outer join
	 */
	public MergeJoin(String id, String[] joinKeys, RecordTransform transformClass, boolean leftOuterJoin) {
		super(id);
		this.joinKeys = joinKeys;
		this.transformation = transformClass;
		this.leftOuterJoin = leftOuterJoin;
	}

	public MergeJoin(String id, String[] joinKeys, DynamicJavaCode dynaTransCode) {
		super(id);
		this.joinKeys = joinKeys;
		this.dynamicTransformation=dynaTransCode;
		this.leftOuterJoin = false;
		// no outer join
	}
	
	/**
	 *  Sets on/off leftOuterJoin indicator
	 *
	 * @param  outerJoin  The new leftOuterJoin value
	 */
	public void setLeftOuterJoin(boolean outerJoin) {
		leftOuterJoin = outerJoin;
	}


	/**
	 *  Sets specific key (string) for slave records<br>
	 *  Can be used if slave record has different names
	 *  for fields composing the key
	 *
	 * @param  slaveKeys  The new slaveOverrideKey value
	 */
	public void setSlaveOverrideKey(String[] slaveKeys) {
		this.slaveOverrideKeys = slaveKeys;
	}


	/**
	 *  Populates record buffer with all slave records having the same key
	 *
	 * @param  port                      Description of the Parameter
	 * @param  nextRecord                Description of the Parameter
	 * @param  key                       Description of the Parameter
	 * @param  currRecord                Description of the Parameter
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 * @exception  JetelException        Description of the Exception
	 */
	private void fillRecordBuffer(InputPort port, DataRecord currRecord, DataRecord nextRecord, RecordKey key)
			 throws IOException, InterruptedException, JetelException {

		recordBuffer.clear();
		if (currRecord != null) {
			dataBuffer.clear();
			currRecord.serialize(dataBuffer);
			dataBuffer.flip();
			recordBuffer.push(dataBuffer);
			while (nextRecord != null) {
				nextRecord = port.readRecord(nextRecord);
				if (nextRecord != null) {
					switch (key.compare(currRecord, nextRecord)) {
						case 0:
							dataBuffer.clear();
							nextRecord.serialize(dataBuffer);
							dataBuffer.flip();
							recordBuffer.push(dataBuffer);
							break;
						case -1:
							return;
						case 1:
							throw new JetelException("Slave record out of order!");
					}
				}
			}
		}
	}


	/**
	 *  Finds corresponding slave record for current driver (if there is some)
	 *
	 * @param  driver                    Description of the Parameter
	 * @param  slave                     Description of the Parameter
	 * @param  slavePort                 Description of the Parameter
	 * @param  key                       Description of the Parameter
	 * @return                           The correspondingRecord value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 */
	private int getCorrespondingRecord(DataRecord driver, DataRecord slave, InputPort slavePort, RecordKey key[])
			 throws IOException, InterruptedException {

		while (slave != null) {
			switch (key[DRIVER_ON_PORT].compare(key[SLAVE_ON_PORT], driver, slave)) {
				case 1:
					slave = slavePort.readRecord(slave);
					break;
				case 0:
					return 0;
				case -1:
					return -1;
			}
		}
		return -1;
		// no more records on slave port
	}


	/**
	 *  Outputs all combinations of current driver record and all slaves with the
	 *  same key
	 *
	 * @param  driver                    Description of the Parameter
	 * @param  slave                     Description of the Parameter
	 * @param  out                       Description of the Parameter
	 * @param  port                      Description of the Parameter
	 * @return                           Description of the Return Value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 */
	private boolean flushCombinations(DataRecord driver, DataRecord slave, DataRecord out, OutputPort port)
			 throws IOException, InterruptedException {
		recordBuffer.rewind();
		dataBuffer.clear();
		inRecords[0] = driver;
		inRecords[1] = slave;

		while (recordBuffer.shift(dataBuffer) != null) {
			dataBuffer.flip();
			slave.deserialize(dataBuffer);
			// **** call transform function here ****
			if (!transformation.transform(inRecords, out)) {
				resultMsg = transformation.getMessage();
				return false;
			}
			port.writeRecord(out);
			dataBuffer.clear();
		}
		return true;
	}


	/**
	 *  If there is no corresponding slave record and is defined outer join, then
	 *  output driver only.
	 *
	 * @param  driver                    Description of the Parameter
	 * @param  out                       Description of the Parameter
	 * @param  port                      Description of the Parameter
	 * @return                           Description of the Return Value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 */
	private boolean flushDriverOnly(DataRecord driver, DataRecord out, OutputPort port)
			 throws IOException, InterruptedException {
		inRecords[0] = driver;
		inRecords[1] = null;

		if (!transformation.transform(inRecords, out)) {
			resultMsg = transformation.getMessage();
			return false;
		}
		port.writeRecord(out);
		return true;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  metadata  Description of the Parameter
	 * @param  count     Description of the Parameter
	 * @return           Description of the Return Value
	 */
	private DataRecord[] allocateRecords(DataRecordMetadata metadata, int count) {
		DataRecord[] data = new DataRecord[count];

		for (int i = 0; i < count; i++) {
			data[i] = new DataRecord(metadata);
			data[i].init();
		}
		return data;
	}



	/**
	 *  Main processing method for the SimpleCopy object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		boolean isDriverDifferent;
		ByteBuffer data = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);

		// get all ports involved
		InputPort driverPort = getInputPort(DRIVER_ON_PORT);
		InputPort slavePort = getInputPort(SLAVE_ON_PORT);
		OutputPort outPort = getOutputPort(WRITE_TO_PORT);

		//initialize input records driver & slave
		DataRecord[] driverRecords = allocateRecords(driverPort.getMetadata(), 2);
		DataRecord[] slaveRecords = allocateRecords(slavePort.getMetadata(), 2);

		// initialize output record
		DataRecord outRecord = new DataRecord(outPort.getMetadata());
		outRecord.init();

		// tmp record for switching contents
		DataRecord tmpRec;
		// create file buffer for slave records - system TEMP path
		recordBuffer = new FileRecordBuffer(null);

		//for the first time (as initialization), we expect that records are different
		isDriverDifferent = true;

		try {
			// first initial load of records
			driverRecords[CURRENT] = driverPort.readRecord(driverRecords[CURRENT]);
			slaveRecords[CURRENT] = slavePort.readRecord(slaveRecords[CURRENT]);
			while (runIt && driverRecords[CURRENT] != null) {
				if (isDriverDifferent) {
					switch (getCorrespondingRecord(driverRecords[CURRENT], slaveRecords[CURRENT], slavePort, recordKeys)) {
						case -1:
							// driver lower
							// no corresponding slave
							if (leftOuterJoin) {
								flushDriverOnly(driverRecords[CURRENT], outRecord, outPort);
							}
							driverRecords[CURRENT] = driverPort.readRecord(driverRecords[CURRENT]);
							isDriverDifferent = true;
							continue;
						case 0:
							// match
							fillRecordBuffer(slavePort, slaveRecords[CURRENT], slaveRecords[TEMPORARY], recordKeys[SLAVE_ON_PORT]);
							// switch temporary --> current
							tmpRec = slaveRecords[CURRENT];
							slaveRecords[CURRENT] = slaveRecords[TEMPORARY];
							slaveRecords[TEMPORARY] = tmpRec;
							isDriverDifferent = false;
							break;
						case 1:
							// should not happen !!
							throw new RuntimeException(getType() + " - program internal error !");
					}
				}
				flushCombinations(driverRecords[CURRENT], slaveRecords[TEMPORARY], outRecord, outPort);
				// get next driver

				driverRecords[TEMPORARY] = driverPort.readRecord(driverRecords[TEMPORARY]);
				if (driverRecords[TEMPORARY] != null) {
					// different driver record ??
					switch (recordKeys[DRIVER_ON_PORT].compare(driverRecords[CURRENT], driverRecords[TEMPORARY])) {
						case 0:
							break;
						case -1:
							// detected change;
							isDriverDifferent = true;
							break;
						case 1:
							throw new JetelException("Driver record out of order!");
					}
				}
				// switch temporary --> current
				tmpRec = driverRecords[CURRENT];
				driverRecords[CURRENT] = driverRecords[TEMPORARY];
				driverRecords[TEMPORARY] = tmpRec;
			}
		} catch (IOException ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
			return;
		} catch (Exception ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
			return;
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
		Class tClass;
		// test that we have at least one input port and one output
		if (inPorts.size() < 2) {
			throw new ComponentNotReadyException("At least two input ports have to be defined!");
		} else if (outPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		// allocate array for joined records (this array will be passed to reformat function)
		joinedRecords = new DataRecord[inPorts.size()];
		if (slaveOverrideKeys == null) {
			slaveOverrideKeys = joinKeys;
		}
		recordKeys = new RecordKey[2];
		recordKeys[0] = new RecordKey(joinKeys, getInputPort(DRIVER_ON_PORT).getMetadata());
		recordKeys[1] = new RecordKey(slaveOverrideKeys, getInputPort(SLAVE_ON_PORT).getMetadata());
		recordKeys[0].init();
		recordKeys[1].init();

		if (transformation == null) {
			if (transformClassName != null) {
				// try to load in transformation class & instantiate
				try {
					tClass = Class.forName(transformClassName);
				} catch (ClassNotFoundException ex) {
					throw new ComponentNotReadyException("Can't find specified transformation class: " + transformClassName);
				} catch (Exception ex) {
					throw new ComponentNotReadyException(ex.getMessage());
				}
				try {
					transformation = (RecordTransform) tClass.newInstance();
				} catch (Exception ex) {
					throw new ComponentNotReadyException(ex.getMessage());
				}
			} else {
				System.out.print(" (compiling dynamic source) ");
				// use DynamicJavaCode to instantiate transformation class
				Object transObject = dynamicTransformation.instantiate();
				if (transObject instanceof RecordTransform) {
					transformation = (RecordTransform) transObject;
				} else {
					throw new ComponentNotReadyException("Provided transformation class doesn't implement RecordTransform.");
				}

			}
		}
		// init transformation
		Collection col = getInPorts();
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[col.size()];
		int counter = 0;
		Iterator i = col.iterator();
		while (i.hasNext()) {
			inMetadata[counter++] = ((InputPort) i.next()).getMetadata();
		}
		// put aside: getOutputPort(WRITE_TO_PORT).getMetadata()
		if (!transformation.init(inMetadata, null)) {
			throw new ComponentNotReadyException("Error when initializing reformat function !");
		}
		dataBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
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
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);
		MergeJoin join;
		DynamicJavaCode dynaTransCode;

		try {
			if (xattribs.exists("transformClass")){
				join = new MergeJoin(xattribs.getString("id"),
						xattribs.getString("joinKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
						xattribs.getString("transformClass"));
			}else{
				// do we have child node wich Java source code ?
				dynaTransCode = DynamicJavaCode.fromXML(nodeXML);
				if (dynaTransCode == null) {
					throw new RuntimeException("Can't create DynamicJavaCode object - source code not found !");
				}
				return new MergeJoin(xattribs.getString("id"),
							xattribs.getString("joinKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
							dynaTransCode);
			}
			if (xattribs.exists("slaveOverrideKey")) {
				join.setSlaveOverrideKey(xattribs.getString("slaveOverrideKey").
						split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));

			}
			if (xattribs.exists("leftOuterJoin")) {
				join.setLeftOuterJoin(xattribs.getBoolean("leftOuterJoin"));
			}
			return join;
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return null;
		}
	}


	/**  Description of the Method */
	public boolean checkConfig() {
		return true;
	}
}

