/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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

/**
 *  <h3>Sorted Join Component</h3> <!-- Changes / reformats the data between
 *  pair of INPUT/OUTPUT ports This component is only a wrapper around
 *  transformation class implementing org.jetel.component.RecordTransform
 *  interface. The method transform is called for every record passing through
 *  this component -->
 *  <tableborder="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *
 *    <tr>
 *
 *      <td>
 *        <h4><i>Name:</i> </h4>
 *      </td>
 *
 *      <td>
 *        SortedJoin
 *      </td>
 *
 *    </tr>
 *
 *    <tr>
 *
 *      <td>
 *        <h4><i>Category:</i> </h4>
 *      </td>
 *
 *      <td>
 *
 *      </td>
 *
 *    </tr>
 *
 *    <tr>
 *
 *      <td>
 *        <h4><i>Description:</i> </h4>
 *      </td>
 *
 *      <td>
 *        Changes / reformats the data between pair of INPUT/OUTPUT ports.<br>
 *        This component is only a wrapper around transformation class
 *        implementing <i>org.jetel.component.RecordTransform</i> interface. The
 *        method <i>transform</i> is called for every record passing through
 *        this component.<br>
 *
 *      </td>
 *
 *    </tr>
 *
 *    <tr>
 *
 *      <td>
 *        <h4><i>Inputs:</i> </h4>
 *      </td>
 *
 *      <td>
 *        [0..n]- input records (min 2 ports active)
 *      </td>
 *
 *    </tr>
 *
 *    <tr>
 *
 *      <td>
 *        <h4><i>Outputs:</i> </h4>
 *      </td>
 *
 *      <td>
 *        [0] - one output port
 *      </td>
 *
 *    </tr>
 *
 *    <tr>
 *
 *      <td>
 *        <h4><i>Comment:</i> </h4>
 *      </td>
 *
 *      <td>
 *
 *      </td>
 *
 *    </tr>
 *
 *  </table>
 *  <br>
 *
 *  <tableborder="1">
 *
 *    <th>
 *      XML attributes:
 *    </th>
 *
 *    <tr>
 *
 *      <td>
 *        <b>type</b>
 *      </td>
 *
 *      <td>
 *        "SORTED_JOIN"
 *      </td>
 *
 *    </tr>
 *
 *    <tr>
 *
 *      <td>
 *        <b>id</b>
 *      </td>
 *
 *      <td>
 *        component identification
 *      </td>
 *
 *      <tr>
 *
 *        <td>
 *          <b>transformClass</b>
 *        </td>
 *
 *        <td>
 *          name of the class to be used for transforming joined data
 *        </td>
 *
 *      </tr>
 *
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="JOIN" type="SORTED_JOIN" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *
 *@author     dpavlis
 *@created    4. èerven 2003
 *@since      April 4, 2002
 */
public class SortedJoin extends Node {

	/**
	 *  Description of the Field
	 */
	public final static String COMPONENT_TYPE = "SORTED_JOIN";

	private final static int WRITE_TO_PORT = 0;
	private final static int DRIVER_ON_PORT = 0;
	private final static int SLAVE_ON_PORT = 1;

	private final static int CURRENT = 0;
	private final static int PREVIOUS = 1;

	private String transformClassName;

	private RecordTransform transformation = null;

	private DataRecord[] joinedRecords;

	private boolean leftOuterJoin = false;

	private String[] joinKeys;

	private RecordKey recordKeys[];

	private ByteBuffer dataBuffer;
	private FileRecordBuffer recordBuffer;


	/**
	 *  Constructor for the SortedJoin object
	 *
	 *@param  id              Description of the Parameter
	 *@param  joinKeys        Description of the Parameter
	 *@param  transformClass  Description of the Parameter
	 */
	public SortedJoin(String id, String[] joinKeys, String transformClass) {
		super(id);
		this.joinKeys = joinKeys;
		this.transformClassName = transformClass;
		this.leftOuterJoin = false;
		// no outer join
	}


	/**
	 *  Constructor for the SortedJoin object
	 *
	 *@param  id              Description of the Parameter
	 *@param  joinKeys        Description of the Parameter
	 *@param  transformClass  Description of the Parameter
	 *@param  leftOuterJoin   Description of the Parameter
	 */
	public SortedJoin(String id, String[] joinKeys, RecordTransform transformClass, boolean leftOuterJoin) {
		super(id);
		this.joinKeys = joinKeys;
		this.transformation = transformClass;
		this.leftOuterJoin = leftOuterJoin;
	}


	/**
	 *  Gets the Type attribute of the SimpleCopy object
	 *
	 *@return    The Type value
	 *@since     April 4, 2002
	 */
	public String getType() {
		return COMPONENT_TYPE;
	}


	/**
	 *  Sets the leftOuterJoin attribute of the SortedJoin object
	 *
	 *@param  outerJoin  The new leftOuterJoin value
	 */
	public void setLeftOuterJoin(boolean outerJoin) {
		leftOuterJoin = outerJoin;
	}


	/**
	 *  Description of the Method
	 *
	 *@param  port                      Description of the Parameter
	 *@param  curRecord                 Description of the Parameter
	 *@param  nextRecord                Description of the Parameter
	 *@param  key                       Description of the Parameter
	 *@exception  IOException           Description of the Exception
	 *@exception  InterruptedException  Description of the Exception
	 *@exception  JetelException        Description of the Exception
	 */
	private void fillRecordBuffer(InputPort port, DataRecord curRecord, DataRecord nextRecord, RecordKey key)
			 throws IOException, InterruptedException, JetelException {
		int compResult;

		recordBuffer.clear();
		if (curRecord != null) {
			dataBuffer.clear();
			curRecord.serialize(dataBuffer);
			dataBuffer.flip();
			recordBuffer.push(dataBuffer);
			while (nextRecord != null) {
				nextRecord = port.readRecord(nextRecord);
				if (nextRecord != null) {
					switch (key.compare(curRecord, nextRecord)) {
						case 0:
							dataBuffer.clear();
							curRecord.serialize(dataBuffer);
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
	 *  Gets the correspondingRecord attribute of the SortedJoin object
	 *
	 *@param  driver                    Description of the Parameter
	 *@param  slave                     Description of the Parameter
	 *@param  slavePort                 Description of the Parameter
	 *@param  key                       Description of the Parameter
	 *@return                           The correspondingRecord value
	 *@exception  IOException           Description of the Exception
	 *@exception  InterruptedException  Description of the Exception
	 */
	private int getCorrespondingRecord(DataRecord driver, DataRecord slave, InputPort slavePort, RecordKey key[])
			 throws IOException, InterruptedException {

		while (slave != null) {
			switch (key[DRIVER_ON_PORT].compare(key[SLAVE_ON_PORT], driver, slave)) {
				case 1:
					if (leftOuterJoin) {
						return 1;
					}
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
	 *  Description of the Method
	 *
	 *@param  driver                    Description of the Parameter
	 *@param  slave                     Description of the Parameter
	 *@param  out                       Description of the Parameter
	 *@param  port                      Description of the Parameter
	 *@return                           Description of the Return Value
	 *@exception  IOException           Description of the Exception
	 *@exception  InterruptedException  Description of the Exception
	 */
	private boolean flushCombinations(DataRecord driver, DataRecord slave, DataRecord out, OutputPort port)
			 throws IOException, InterruptedException {
		DataRecord inRecords[] = {driver, slave};
		recordBuffer.rewind();
		dataBuffer.clear();

		while (recordBuffer.shift(dataBuffer) != null) {
			dataBuffer.flip();
			slave.deserialize(dataBuffer);
			// call transform function here
			if (!transformation.transform(inRecords, out)) {
				resultMsg = transformation.getMessage();
				return false;
			}
			// TODO::::
			//port.writeRecord(out);
			dataBuffer.clear();
		}
		return true;
	}


	/**
	 *  Description of the Method
	 *
	 *@param  driver                    Description of the Parameter
	 *@param  out                       Description of the Parameter
	 *@param  port                      Description of the Parameter
	 *@return                           Description of the Return Value
	 *@exception  IOException           Description of the Exception
	 *@exception  InterruptedException  Description of the Exception
	 */
	private boolean flushDriverOnly(DataRecord driver, DataRecord out, OutputPort port)
			 throws IOException, InterruptedException {
		DataRecord inRecords[] = {driver, null};

		if (!transformation.transform(inRecords, out)) {
			resultMsg = transformation.getMessage();
			return false;
		}
		// TODO::::
		//port.writeRecord(out);
		return true;
	}


	/**
	 *  Description of the Method
	 *
	 *@param  metadata  Description of the Parameter
	 *@param  count     Description of the Parameter
	 *@return           Description of the Return Value
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
	 *@since    April 4, 2002
	 */
	public void run() {
		boolean isDifferent;
		//int currDriver=0;
		//int currSlave=0;
		//
		ByteBuffer data = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);

		// get all ports involved
		InputPort driverPort = getInputPort(DRIVER_ON_PORT);
		InputPort slavePort = getInputPort(SLAVE_ON_PORT);
		OutputPort outPort = getOutputPort(WRITE_TO_PORT);

		//initialize input records driver & slave
		DataRecord[] driverRecords = allocateRecords(driverPort.getMetadata(), 2);
		DataRecord[] slaveRecords = allocateRecords(slavePort.getMetadata(), 2);

		// initialize output record
		//DataRecord outRecord=new DataRecord(outPort.getMetadata());
		DataRecord outRecord = driverRecords[0];
		// only work around
		outRecord.init();

		// create file buffer for slave records
		recordBuffer = new FileRecordBuffer(null);
		// systen TEMP path

		//for the first time (as initialization), we expect that records are different
		isDifferent = true;

		try {
			// first initial load of records
			driverRecords[0] = driverPort.readRecord(driverRecords[0]);
			slaveRecords[1] = slavePort.readRecord(slaveRecords[1]);
			while (runIt && driverRecords[0] != null) {

				if (isDifferent) {
					// debug
					System.out.println("####### Slave record on input: #######");
					System.out.println(slaveRecords[0]);
					switch (getCorrespondingRecord(driverRecords[0], slaveRecords[1], slavePort, recordKeys)) {
						case -1:
							// driver lower
							driverRecords[0] = driverPort.readRecord(driverRecords[0]);
							isDifferent = true;
							continue;
						case 0:
							// match
							fillRecordBuffer(slavePort, slaveRecords[0], slaveRecords[1], recordKeys[SLAVE_ON_PORT]);
							isDifferent = false;
							System.out.println("####### Slave record (2) on input: #######");
							System.out.println(slaveRecords[0]);
							break;
						case 1:
							// no corresponding slave (can be only if leftOuterJoin)
							flushDriverOnly(driverRecords[0], outRecord, outPort);
							driverRecords[1] = driverPort.readRecord(driverRecords[1]);
							continue;
					}
					flushCombinations(driverRecords[0], slaveRecords[1], outRecord, outPort);
					driverRecords[1] = driverPort.readRecord(driverRecords[1]);
					// different driver record ??
					switch (recordKeys[DRIVER_ON_PORT].compare(driverRecords[0], driverRecords[1])) {
						case 0:
							break;
						case -1:
							// detected change;
							isDifferent = true;
							break;
						case 1:
							throw new JetelException("Driver record out of order!");
					}
					DataRecord tmpRec = driverRecords[0];
					driverRecords[0] = driverRecords[1];
					driverRecords[1] = tmpRec;

				}
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
	 *@exception  ComponentNotReadyException  Description of the Exception
	 *@since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		Class tClass;
		// test that we have at least one input port and one output
		if (inPorts.size() < 2) {
			throw new ComponentNotReadyException("At least two input ports have to be defined!");
		} else if (outPorts.size() < 0) {
			// TODO:
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		// allocate array for joined records (this array will be passed to reformat function)
		joinedRecords = new DataRecord[inPorts.size()];
		recordKeys = new RecordKey[2];
		recordKeys[0] = new RecordKey(joinKeys, getInputPort(0).getMetadata());
		recordKeys[1] = new RecordKey(joinKeys, getInputPort(1).getMetadata());
		recordKeys[0].init();
		recordKeys[1].init();

		if (transformation == null) {
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
	 *@return    Description of the Returned Value
	 *@since     May 21, 2002
	 */
	public org.w3c.dom.Node toXML() {
		// TODO
		return null;
	}


	/**
	 *  Description of the Method
	 *
	 *@param  nodeXML  Description of Parameter
	 *@return          Description of the Returned Value
	 *@since           May 21, 2002
	 */
	public static Node fromXML(org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);
		SortedJoin join;

		try {
			join = new SortedJoin(xattribs.getString("id"),
					xattribs.getString("joinKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
					xattribs.getString("transformClass"));
			if (xattribs.exists("leftOuterJoin")) {
				join.setLeftOuterJoin(xattribs.getBoolean("leftOuterJoin"));
			}
			return join;
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return null;
		}
	}

}

