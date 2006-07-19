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
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.FileRecordBuffer;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

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
 *	join (<code>leftOuterJoin</code> option) is specified, when only driver record is passed to <i>transform</i> method.<br>
 *  If full outer join (<code>fullOuterJoin</code> option) is enabled, then <i>transform</i> is called even if no corresponding
 *  driver record is found for particular slave. In these cases be sure, that your transform code is prepared 
 *  processe null input records. 
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
 *    <tr><td><b>slaveOverrideKey</b><br><i>optional</i></td><td>can be used to specify different key field names for records on slave input;
 * field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *  <tr><td><b>libraryPath</b><br><i>optional</i></td><td>name of Java library file (.jar,.zip,...) where
 *  to search for class to be used for transforming joined data specified in <tt>transformClass<tt> parameter.</td></tr>
 *   <tr><td><b>transformClass</b><br><i>optional</i></td><td>name of the class to be used for transforming joined data<br>
 *    If no class name is specified then it is expected that the transformation Java source code is embedded in XML - <i>see example
 * below</i></td></tr>
 *  <tr><td><b>transform</b></td><td>contains definition of transformation in internal clover format </td></tr>
 *    <tr><td><b>leftOuterJoin</b><br><i>optional</i></td><td>true/false <i>default: FALSE</i> See description of the Sorted join component.</td></tr>
 * 	  <tr><td><b>fullOuterJoin</b><br><i>optional</i></td><td>true/false <i>default: FALSE</i> See description of the Sorted join component.</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="JOIN" type="MERGE_JOIN" joinKey="CustomerID" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *<pre>&lt;Node id="JOIN" type="HASH_JOIN" joinKey="EmployeeID" leftOuterJoin="false"&gt;
 *import org.jetel.component.DataRecordTransform;
 *import org.jetel.data.*;
 * 
 *public class reformatJoinTest extends DataRecordTransform{
 *
 *	public boolean transform(DataRecord[] source, DataRecord[] target){
 *		
 *		target[0].getField(0).setValue(source[0].getField(0).getValue());
 *		target[0].getField(1).setValue(source[0].getField(1).getValue());
 *		target[0].getField(2).setValue(source[0].getField(2).getValue());
 *		if (source[1]!=null){
 *			target[0].getField(3).setValue(source[1].getField(0).getValue());
 *			target[0].getField(4).setValue(source[1].getField(1).getValue());
 *		}
 *		return true;
 *	}
 *}
 *
 *&lt;/Node&gt;</pre>
 * @author      dpavlis
 * @since       April 4, 2002
 * @revision    $Revision$
 * @created     4. June 2003
 */
public class MergeJoin extends Node {

	private static final String XML_FULLOUTERJOIN_ATTRIBUTE = "fullOuterJoin";
	private static final String XML_LEFTOUTERJOIN_ATTRIBUTE = "leftOuterJoin";
	private static final String XML_SLAVEOVERRIDEKEY_ATTRIBUTE = "slaveOverrideKey";
	private static final String XML_JOINKEY_ATTRIBUTE = "joinKey";
	private static final String XML_TRANSFORMCLASS_ATTRIBUTE = "transformClass";
	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "MERGE_JOIN";

	private final static int WRITE_TO_PORT = 0;
	private final static int DRIVER_ON_PORT = 0;
	private final static int SLAVE_ON_PORT = 1;

	private final static int CURRENT = 0;
	private final static int PREVIOUS = 1;
	private final static int TEMPORARY = 1;

	private String transformClassName;
	private String transformSource = null;

	private RecordTransform transformation = null;

	private boolean leftOuterJoin = false;
	private boolean fullOuterJoin = false;

	private String[] joinKeys;
	private String[] slaveOverrideKeys = null;

	private RecordKey recordKeys[];

	private ByteBuffer dataBuffer;
	private FileRecordBuffer recordBuffer;

	// for passing data records into transform function
	private final static DataRecord[] inRecords = new DataRecord[2];
	private DataRecord[] outRecords=new DataRecord[2];

	private Properties transformationParameters;
	
	static Log logger = LogFactory.getLog(MergeJoin.class);
	
	/**
	 *  Constructor for the SortedJoin object
	 *
	 * @param  id              id of component
	 * @param  joinKeys        field names composing key
	 * @param  transformClass  class (name) to be used for transforming data
	 * @param  leftOuterJoin   indicates, whether to perform left outer join
	 */
	public MergeJoin(String id, String[] joinKeys, String transform,
			String transformClass, boolean leftOuterJoin) {
		super(id);
		this.joinKeys = joinKeys;
		this.transformSource = transform;
		this.transformClassName = transformClass;
		this.leftOuterJoin = leftOuterJoin;
	}

	/**
	 *  Sets on/off fullOuterJoin indicator
	 *
	 * @param  outerJoin  The new leftOuterJoin value
	 */
	public void setFullOuterJoin(boolean outerJoin) {
		fullOuterJoin = outerJoin;
		// if full outer then left as well
		// (it is set only to relax checking for both left & full outer)
		if (fullOuterJoin){
		    leftOuterJoin=true;
		}
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
				    if (fullOuterJoin){
				        return 1;
				    }else{
					slave = slavePort.readRecord(slave);
				    }
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
		outRecords[0]= out;

		while (recordBuffer.shift(dataBuffer) != null) {
			dataBuffer.flip();
			slave.deserialize(dataBuffer);
			// **** call transform function here ****
            try{
                if (!transformation.transform(inRecords, outRecords)) {
                    resultCode = Node.RESULT_ERROR;
                    resultMsg = transformation.getMessage();
                    return false;
                }              
            }catch(NullPointerException ex){
                logger.error("Null pointer exception when transforming input data",ex);
                logger.info("Possibly incorrectly handled outer-join situation");
                throw new RuntimeException("Null pointer exception when transforming input data",ex);
            }
			port.writeRecord(out);
			dataBuffer.clear();
		}
		return true;
	}


	/**
	 *  If there is no corresponding slave record and is defined left outer join, then
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
		outRecords[0]= out;

		try{
		    if (!transformation.transform(inRecords, outRecords)) {
		        resultMsg = transformation.getMessage();
		        return false;
		    }
		}catch(NullPointerException ex){
		    logger.error("Null pointer exception when transforming input data",ex);
		    logger.info("Possibly incorrectly handled outer-join situation");
		    throw new RuntimeException("Null pointer exception when transforming input data",ex);
		}
		port.writeRecord(out);
		return true;
	}

	/**
	 *  If there is no corresponding driver record and is defined full outer join, then
	 *  output slave only.
	 *
	 * @param  driver                    Description of the Parameter
	 * @param  out                       Description of the Parameter
	 * @param  port                      Description of the Parameter
	 * @return                           Description of the Return Value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 */
	private boolean flushSlaveOnly(DataRecord slave, DataRecord out, OutputPort port)
	throws IOException, InterruptedException {
	    inRecords[0] = null;
	    inRecords[1] = slave;
	    outRecords[0]= out;
	    
	    try{
		    if (!transformation.transform(inRecords, outRecords)) {
		        resultMsg = transformation.getMessage();
		        return false;
		    }
		}catch(NullPointerException ex){
		    logger.error("Null pointer exception when transforming input data",ex);
		    logger.info("Possibly incorrectly handled outer-join situation");
		    throw new RuntimeException("Null pointer exception when transforming input data",ex);
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
							// looks like full outer join is defined
						    if (fullOuterJoin){
						        flushSlaveOnly(slaveRecords[CURRENT], outRecord, outPort);
						        slaveRecords[CURRENT]=slavePort.readRecord(slaveRecords[CURRENT]);
						        continue;
						    }else{ // should not happen if no outer join
						        throw new RuntimeException(getType() + " - program internal error !");
						    }
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
				SynchronizeUtils.cloverYield();
			}
			// if full outer join defined and there are some slave records left, flush them
			if (fullOuterJoin) {
			    	while(slaveRecords[CURRENT]!=null){
			    	    flushSlaveOnly(slaveRecords[CURRENT], outRecord, outPort);
			    	    slaveRecords[CURRENT]=slavePort.readRecord(slaveRecords[CURRENT]);
			    	}
			}
		} catch (IOException ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
			return;
		} catch (Exception ex) {
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
			return;
		}
		// signal end of records stream to transformation function
		transformation.finished();
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
		if (slaveOverrideKeys == null) {
			slaveOverrideKeys = joinKeys;
		}
		recordKeys = new RecordKey[2];
		recordKeys[0] = new RecordKey(joinKeys, getInputPort(DRIVER_ON_PORT).getMetadata());
		recordKeys[1] = new RecordKey(slaveOverrideKeys, getInputPort(SLAVE_ON_PORT).getMetadata());
		recordKeys[0].init();
		recordKeys[1].init();

		// init transformation
		DataRecordMetadata[] inMetadata =(DataRecordMetadata[]) getInMetadata().toArray(new DataRecordMetadata[0]);
        DataRecordMetadata[] outMetadata=new DataRecordMetadata[] {getOutputPort(WRITE_TO_PORT).getMetadata()};
        try {
            transformation = RecordTransformFactory.createTransform(
            		transformSource, transformClassName, this, inMetadata, outMetadata, transformationParameters);
        } catch(Exception e) {
            throw new ComponentNotReadyException(this, e);
        }
		dataBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
	}


    /**
     * @param transformationParameters The transformationParameters to set.
     */
    public void setTransformationParameters(Properties transformationParameters) {
        this.transformationParameters = transformationParameters;
    }
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		
		if (transformClassName != null) {
			xmlElement.setAttribute(XML_TRANSFORMCLASS_ATTRIBUTE, transformClassName);
		} else {
//        comment by Martin Zatopek - must be changed (now I removing TransformationGraph singleton)
//			Document doc = TransformationGraphXMLReaderWriter.getReference().getOutputXMLDocumentReference();
//			Text text = doc.createTextNode(this.dynamicTransformation.getSourceCode());
//			xmlElement.appendChild(text);
		}
		
		
		if (joinKeys != null) {
			StringBuffer buf = new StringBuffer(joinKeys[0]);
			for (int i=1; i< joinKeys.length; i++) {
				buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + joinKeys[i]); 
			}
			xmlElement.setAttribute(XML_JOINKEY_ATTRIBUTE, buf.toString());
		}
		
		if (slaveOverrideKeys != null) {
			StringBuffer buf = new StringBuffer(slaveOverrideKeys[0]);
			for (int i=1; i< slaveOverrideKeys.length; i++) {
				buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + slaveOverrideKeys[i]); 
			}
			xmlElement.setAttribute(XML_SLAVEOVERRIDEKEY_ATTRIBUTE, buf.toString());
		}
		
		xmlElement.setAttribute(XML_LEFTOUTERJOIN_ATTRIBUTE, String.valueOf(this.leftOuterJoin));
		xmlElement.setAttribute(XML_FULLOUTERJOIN_ATTRIBUTE, String.valueOf(this.fullOuterJoin));
		
		if (transformationParameters != null) {
			Enumeration propertyAtts = transformationParameters.propertyNames();
			while (propertyAtts.hasMoreElements()) {
				String attName = (String)propertyAtts.nextElement();
				xmlElement.setAttribute(attName,transformationParameters.getProperty(attName));
			}
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
		MergeJoin join;

		try {
            join = new MergeJoin(
                    xattribs.getString(Node.XML_ID_ATTRIBUTE),
                    xattribs.getString(XML_JOINKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
                    xattribs.getString(XML_TRANSFORM_ATTRIBUTE, null), 
                    xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
                    xattribs.getBoolean(XML_LEFTOUTERJOIN_ATTRIBUTE,false));
			if (xattribs.exists(XML_SLAVEOVERRIDEKEY_ATTRIBUTE)) {
				join.setSlaveOverrideKey(xattribs.getString(XML_SLAVEOVERRIDEKEY_ATTRIBUTE).
						split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));

			}
			if (xattribs.exists(XML_FULLOUTERJOIN_ATTRIBUTE)) {
				join.setFullOuterJoin(xattribs.getBoolean(XML_FULLOUTERJOIN_ATTRIBUTE));
			}
			join.setTransformationParameters(xattribs.attributes2Properties(
	                new String[]{XML_TRANSFORMCLASS_ATTRIBUTE}));
			
			return join;
		} catch (Exception ex) {
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
	}

	/**  Description of the Method */
	public boolean checkConfig() {
		return true;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}
}

