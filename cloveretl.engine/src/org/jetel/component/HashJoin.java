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

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.*;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.DynamicJavaCode;

/**
 *  <h3>HashJoin Component</h3> <!-- Joins two records from two different
 * input flows based on specified key. The flow on port 0 is the driver, the flow
 * on port 1 is the slave. First, all records from slave flow are read and stored in
 * hash table. Then for every record from driver flow, corresponding record from
 * slave flow is looked up (if it exists) -->
 *
 *<table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>HashJoin</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *      <td>
 *        Joins records on input ports. It expects that on port [0], there is a
 *	driver and on port [1] is a slave<br>
 *	For each driver record, slave record is looked up in Hashtable which is created
 *	from all records on slave port.
 *	Pair of driver and slave records is sent to transformation class.<br>
 *	The method <i>transform</i> is called for every pair of driver&amps;slave.<br>
 *	It skips driver records for which there is no corresponding slave - unless outer
 *	join is specified, when only driver record is passed to <i>transform<i> method.<br>
 *	Hash join does not require input data be sorted. But it spends some time at the beginning
 *	initializing hashtable of slave records.
 *	It is generally good idea to specify how many records are expected to be stored in hashtable, especially
 *	when you expect the number to be really great. It is better to specify slightly greater number to ensure
 *	that rehashing won't occure. For small record sets - up to 512 records, there is no need to specify the
 *	size.
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
 *    <tr><td><b>type</b></td><td>"HASH_JOIN"</td></tr>
 *    <tr><td><b>id</b></td><td>component identification</td></tr>
 *    <tr><td><b>joinKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *    <tr><td><b>slaveOverrideKey</b><br><i>optional</i></td><td>field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *    <tr><td><b>transformClass</b></td><td>name of the class to be used for transforming joined data</td></tr>
 *    <tr><td><b>leftOuterJoin</b><br><i>optional</i></td><td>true/false</td></tr>
 *    <tr><td><b>hashTableSize</b><br><i>optional</i></td><td>how many records are expected (roughly) to be in hashtable.</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="JOIN" type="HASH_JOIN" joinKey="CustomerID" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *
 * @author      dpavlis
 * @since       March 09, 2004
 * @revision    $Revision$
 * @created     09. March 2004
 */
public class HashJoin extends Node {

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "HASH_JOIN";

	private final static int DEFAULT_HASH_TABLE_INITIAL_CAPACITY = 512;

	private final static int WRITE_TO_PORT = 0;
	private final static int DRIVER_ON_PORT = 0;
	private final static int SLAVE_ON_PORT = 1;

	private String transformClassName;

	private RecordTransform transformation = null;
	private DynamicJavaCode dynamicTransformation = null;

	private DataRecord[] joinedRecords;

	private boolean leftOuterJoin = false;

	private String[] joinKeys;
	private String[] slaveOverrideKeys = null;

	private RecordKey driverKey;
	private RecordKey slaveKey;

	private Map hashMap;

	// for passing data records into transform function
	private final static DataRecord[] inRecords = new DataRecord[2];

	private int hashTableInitialCapacity;


	/**
	 *Constructor for the HashJoin object
	 *
	 * @param  id              Description of the Parameter
	 * @param  joinKeys        Description of the Parameter
	 * @param  transformClass  Description of the Parameter
	 */
	public HashJoin(String id, String[] joinKeys, String transformClass) {
		super(id);
		this.joinKeys = joinKeys;
		this.transformClassName = transformClass;
		this.leftOuterJoin = false;
		this.hashTableInitialCapacity = DEFAULT_HASH_TABLE_INITIAL_CAPACITY;
		// no outer join
	}



	/**
	 *Constructor for the HashJoin object
	 *
	 * @param  id              Description of the Parameter
	 * @param  joinKeys        Description of the Parameter
	 * @param  transformClass  Description of the Parameter
	 * @param  leftOuterJoin   Description of the Parameter
	 */
	public HashJoin(String id, String[] joinKeys, RecordTransform transformClass, boolean leftOuterJoin) {
		super(id);
		this.joinKeys = joinKeys;
		this.transformation = transformClass;
		this.leftOuterJoin = leftOuterJoin;
		this.hashTableInitialCapacity = DEFAULT_HASH_TABLE_INITIAL_CAPACITY;
	}


	/**
	 *Constructor for the HashJoin object
	 *
	 * @param  id         Description of the Parameter
	 * @param  joinKeys   Description of the Parameter
	 * @param  dynaTrans  Description of the Parameter
	 */
	public HashJoin(String id, String[] joinKeys, DynamicJavaCode dynaTrans) {
		super(id);
		this.joinKeys = joinKeys;
		this.dynamicTransformation = dynaTrans;
		this.leftOuterJoin = false;
		this.hashTableInitialCapacity = DEFAULT_HASH_TABLE_INITIAL_CAPACITY;
		// no outer join
	}


	/**
	 *  Sets the leftOuterJoin attribute of the HashJoin object
	 *
	 * @param  outerJoin  The new leftOuterJoin value
	 */
	public void setLeftOuterJoin(boolean outerJoin) {
		leftOuterJoin = outerJoin;
	}


	/**
	 *  Sets the slaveOverrideKey attribute of the HashJoin object
	 *
	 * @param  slaveKeys  The new slaveOverrideKey value
	 */
	public void setSlaveOverrideKey(String[] slaveKeys) {
		this.slaveOverrideKeys = slaveKeys;
	}


	/**
	 *  Sets the hashTableInitialCapacity attribute of the HashJoin object
	 *
	 * @param  capacity  The new hashTableInitialCapacity value
	 */
	public void setHashTableInitialCapacity(int capacity) {
		if (capacity > DEFAULT_HASH_TABLE_INITIAL_CAPACITY) {
			hashTableInitialCapacity = capacity;
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
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
		(driverKey = new RecordKey(joinKeys, getInputPort(DRIVER_ON_PORT).getMetadata())).init();
		(slaveKey = new RecordKey(slaveOverrideKeys, getInputPort(SLAVE_ON_PORT).getMetadata())).init();

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
	}


	/**
	 *  Main processing method for the SimpleCopy object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		InputPort inDriverPort = getInputPort(DRIVER_ON_PORT);
		InputPort inSlavePort = getInputPort(SLAVE_ON_PORT);
		OutputPort outPort = getOutputPort(WRITE_TO_PORT);
		DataRecordMetadata slaveRecordMetadata = inSlavePort.getMetadata();
		DataRecord driverRecord;
		DataRecord slaveRecord;
		DataRecord outRecord[]= {new DataRecord(outPort.getMetadata())};

		hashMap = new HashMap(hashTableInitialCapacity);
		if (hashMap == null) {
			resultMsg = "Can't allocate HashMap of size: " + hashTableInitialCapacity;
			resultCode = Node.RESULT_FATAL_ERROR;
			return;
		}

		// first read all records from SLAVE port
		while (runIt) {
			try {
				slaveRecord = new DataRecord(slaveRecordMetadata);
				slaveRecord.init();
				slaveRecord = inSlavePort.readRecord(slaveRecord);
				if (slaveRecord != null) {
					hashMap.put(new HashKey(slaveKey, slaveRecord),
							slaveRecord);
				} else {
					// we have finished reading SLAVE records
					break;
				}
				//yield();

			} catch (IOException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			} catch (Exception ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_FATAL_ERROR;
				return;
			}
		}
		//XDEBUG START
		//		for (Iterator i=hashMap.values().iterator();i.hasNext();){
		//			System.out.println("> "+i.next());
		//		}
		//		System.out.println("***KEYS***");
		//		for (Iterator i=hashMap.keySet().iterator();i.hasNext();){
		//			System.out.println("> "+i.next());
		//		}
		//XDEBUG END

		// now read all records from DRIVER port and try to look up corresponding
		// record from SLAVE records set.
		driverRecord = new DataRecord(inDriverPort.getMetadata());
		driverRecord.init();
		outRecord[0].init();
		HashKey driverHashKey = new HashKey(driverKey, driverRecord);
		inRecords[0] = driverRecord;
		//inRecords[1] - assigned individually for every slave record

		while (runIt && driverRecord != null) {
			try {
				driverRecord = inDriverPort.readRecord(driverRecord);
				if (driverRecord != null) {
					// let's find slave record
					slaveRecord = (DataRecord) hashMap.get(driverHashKey);
					// do we have it or is OuterJoin enabled ?
					if ((slaveRecord != null) || (leftOuterJoin)) {
						// call transformation function
						inRecords[1] = slaveRecord;
						if (!transformation.transform(inRecords, outRecord)) {
							resultCode = Node.RESULT_ERROR;
							resultMsg = transformation.getMessage();
							return;
						}
						outPort.writeRecord(outRecord[0]);
					}
				}
				yield();
			} catch (IOException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			} catch (Exception ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_FATAL_ERROR;
				return;
			}
		}
		setEOF(WRITE_TO_PORT);
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
		HashJoin join;
		DynamicJavaCode dynaTransCode;

		try {
			if (xattribs.exists("transformClass")) {
				join = new HashJoin(xattribs.getString("id"),
						xattribs.getString("joinKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
						xattribs.getString("transformClass"));
			} else {
				// do we have child node wich Java source code ?
				dynaTransCode = DynamicJavaCode.fromXML(nodeXML);
				if (dynaTransCode == null) {
					throw new RuntimeException("Can't create DynamicJavaCode object - source code not found !");
				}
				join = new HashJoin(xattribs.getString("id"),
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
			if (xattribs.exists("hashTableSize")) {
				join.setHashTableInitialCapacity(xattribs.getInteger("hashTableSize"));
			}
			return join;
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
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

}

