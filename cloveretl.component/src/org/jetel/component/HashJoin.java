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
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

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
 *  Joins records on input ports. It expects driver stream at port [0] and 
 *  slave streams at other input ports. Slave streams are expected to be small enough
 *  to store all the slave records in hashtable.<br>
 *  Each driver record is joined with corresponding slave records according
 *  to join keys specification. 
 *	For each driver record, slave records are looked up in Hashtables which are created
 *	from all records on slave inputs.
 *	Tuple of driver and slave records is sent to transformation class.<br>
 *	The method <i>transform</i> is called for every tuple of driver and
 *  corresponding slaves.<br>
 *  There are three join modes available: inner, left outer, full outer.<br>
 *  Inner mode processess only driver records for which all associated slaves are available.
 *  Left outer mode furthermore processes driver records with missing slaves.
 *  Full outer mode additionally calls transformation method for slaves without driver.<br>
 *  In case you use outer mode, be sure your transformation code is able to handle null
 *  input records.
 *	Hash join does not require input data to be sorted. But it spends some time at the beginning
 *	initializing hashtable of slave records.
 *	It is generally good idea to specify how many records are expected to be stored in each hashtable
 *  (there is one hashtable per each slave input), especially
 *	when you expect the number to be really great. It is better to specify slightly greater number to ensure
 *	that rehashing won't occure. For small record sets - up to 512 records, there is no need to specify the
 *	size.
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0] - driver record input<br>
 *	      [1+] - slave record inputs<br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - sole output port
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
 *    <tr><td><b>joinKey</b></td><td>join key specification in format<br>
 *    <tt>mapping1#mapping2...</tt>, where <tt>mapping</tt> has format
 *    <tt>driver_field1=slave_field1|driver_field2=slave_field2|...</tt><br>
 *    In case slave_field is missing it is supposed to be the same as the driver_field. When driver_field
 *    is missing (ie there's nothin before '='), it will be taken from the first mapping. 
 *    Order of mappings corresponds to order of slave input ports. In case a mapping is empty or missing for some slave, the component
 *    will use first mapping instead of it.</td></tr>
 *  <tr><td><b>libraryPath</b><br><i>optional</i></td><td>name of Java library file (.jar,.zip,...) where
 *  to search for class to be used for transforming joined data specified in <tt>transformClass<tt> parameter.</td></tr>
 *  <tr><td><b>transform</b></td><td>contains definition of transformation in internal clover format </td>
 *    <tr><td><b>transformClass</b><br><i>optional</i></td><td>name of the class to be used for transforming joined data<br>
 *    If no class name is specified then it is expected that the transformation Java source code is embedded in XML - <i>see example
 * below</i></td></tr>
 *    <tr><td><b>joinType</b><br><i>optional</i></td><td>inner/leftOuter/fullOuter Specifies type of join operation. Default is inner.</td></tr>
 *    <tr><td><b>hashTableSize</b><br><i>optional</i></td><td>how many records are expected (roughly) to be in hashtable.</td></tr>
 *    <tr><td><b>slaveDuplicates</b><br><i>optional</i></td><td>true/false - allow records on slave port with duplicate keys. Default is false - multiple
 *    duplicate records are discarded - only the first one is used for join.</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="JOIN" type="HASH_JOIN" joinKey="CustomerID" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *	  
 *<pre>&lt;Node id="JOIN" type="HASH_JOIN" joinKey="EmployeeID*ID" joinType="inner"&gt;
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
 *	  
 * @author      dpavlis, Jan Hadrava
 * @since       March 09, 2004
 * @revision    $Revision$
 * @created     09. March 2004
 */
/**
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class HashJoin extends Node {
	public enum Join {
		INNER,
		LEFT_OUTER,
		FULL_OUTER,
	}

	private static final String XML_HASHTABLESIZE_ATTRIBUTE = "hashTableSize";
	private static final String XML_JOINTYPE_ATTRIBUTE = "joinType";
	private static final String XML_JOINKEY_ATTRIBUTE = "joinKey";
	private static final String XML_TRANSFORMCLASS_ATTRIBUTE = "transformClass";
	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE ="slaveDuplicates";
	// legacy attributes
	private static final String XML_LEFTOUTERJOIN_ATTRIBUTE = "leftOuterJoin";
	private static final String XML_SLAVEOVERRIDEKEY_ATTRIBUTE = "slaveOverrideKey";

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "HASH_JOIN";

	private final static int DEFAULT_HASH_TABLE_INITIAL_CAPACITY = 512;

	private final static int WRITE_TO_PORT = 0;
	private final static int DRIVER_ON_PORT = 0;
	private final static int FIRST_SLAVE_PORT = 1;

	private String transformClassName;

	private RecordTransform transformation = null;
	private String transformSource = null;

	private Join join;
	private boolean slaveDuplicates=false;

	private String[][] driverJoiners;
	private String[][] slaveJoiners;
	
	private RecordKey[] driverKeys;
	private RecordKey[] slaveKeys;

	private HashMap<HashKey, MapItem>[] hashMap;
	private int hashTableInitialCapacity;

	private Properties transformationParameters;

	static Log logger = LogFactory.getLog(HashJoin.class);

	private int slaveCnt;

	private InputPort driverPort;
	private OutputPort outPort;
	DataRecord[] inRecords;
	DataRecord[] outRecords;

	/**
	 *Constructor for the HashJoin object
	 *
	 * @param id		Description of the Parameter
	 * @param driverJoiners	Array of driver joiners (each element contains list of join keys for one slave)
	 * @param slaveJoiners	Array of slave joiners (each element contains list of join keys for one slave)
	 * @param transform
	 * @param transformClass  class (name) to be used for transforming data
	 * @param join join type
	 * @param slaveDuplicates enables/disables duplicate slaves
	 */
	public HashJoin(String id, String[][] driverJoiners, String[][] slaveJoiners, String transform,
			String transformClass, Join join) {
		super(id);
		this.transformSource =transform;
		this.transformClassName = transformClass;
		this.join = join;
		this.hashTableInitialCapacity = DEFAULT_HASH_TABLE_INITIAL_CAPACITY;
		this.driverJoiners = driverJoiners;
		this.slaveJoiners = slaveJoiners;
	}

	public HashJoin(String id, String[][] driverJoiners, String[][] slaveJoiners, 
			RecordTransform transform, Join join) {
        this(id, driverJoiners, slaveJoiners, null, null, join);
		this.transformation = transform;
	}

	//	/**
//	*  Sets the leftOuterJoin attribute of the HashJoin object
//	*
//	* @param  outerJoin  The new leftOuterJoin value
//	*/
//	public void setLeftOuterJoin(boolean outerJoin) {
//	leftOuterJoin = outerJoin;
//	}

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


	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	public void init() throws ComponentNotReadyException {
		super.init();

		driverPort = getInputPort(DRIVER_ON_PORT);
		outPort = getOutputPort(WRITE_TO_PORT);

		slaveCnt = inPorts.size() - FIRST_SLAVE_PORT;
		if (driverJoiners.length < 1) {
			throw new ComponentNotReadyException("driver key list not specified");
		}
		if (driverJoiners.length < slaveCnt) {
			logger.warn("Driver keys aren't specified for all slave inputs - deducing missing keys");
			String[][] replJoiners = new String[slaveCnt][];
			for (int i = 0; i < driverJoiners.length; i++) {
				replJoiners[i] = driverJoiners[i];
			}
			// use first master key specification to deduce all missing driver key specifications
			for (int i = driverJoiners.length; i < slaveCnt; i++) {
				replJoiners[i] = driverJoiners[0];
			}
			driverJoiners = replJoiners;
		}
		if (slaveJoiners.length < slaveCnt) {
			logger.warn("Slave keys aren't specified for all slave inputs - deducing missing keys");
			String[][] replJoiners = new String[slaveCnt][];
			for (int i = 0; i < slaveJoiners.length; i++) {
				replJoiners[i] = slaveJoiners[i];
			}
			// use first master key specification to deduce all missing driver key specifications
			for (int i = slaveJoiners.length; i < slaveCnt; i++) {
				replJoiners[i] = driverJoiners[0];
			}
			slaveJoiners = replJoiners;
		}

		inRecords = new DataRecord[1 + slaveCnt];
		inRecords[0] = new DataRecord(driverPort.getMetadata());
		inRecords[0].init();
		outRecords = new DataRecord[1];
		outRecords[0] = new DataRecord(outPort.getMetadata());
		outRecords[0].init();

		driverKeys = new RecordKey[slaveCnt];
		slaveKeys = new RecordKey[slaveCnt];
		for (int idx = 0; idx < slaveCnt; idx++) {
			driverKeys[idx] = new RecordKey(driverJoiners[idx], driverPort.getMetadata());
			driverKeys[idx].init();
			slaveKeys[idx] = new RecordKey(slaveJoiners[idx], getInputPort(FIRST_SLAVE_PORT + idx).getMetadata());
			slaveKeys[idx].init();
		}

		// allocate maps		
		try {
			hashMap = (HashMap<HashKey, MapItem>[])new HashMap[slaveCnt];
			for (int idx = 0; idx < slaveCnt; idx++) {
				hashMap[idx] = new HashMap<HashKey, MapItem>(hashTableInitialCapacity);
			}
		} catch (OutOfMemoryError ex) {
			logger.fatal(ex);
			throw new ComponentNotReadyException("Can't allocate HashMap of size: "	+ hashTableInitialCapacity);
		}

		// init transformation
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] {
				getOutputPort(WRITE_TO_PORT).getMetadata()};
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[1 + slaveCnt];
		inMetadata[0] = getInputPort(DRIVER_ON_PORT).getMetadata();
		for (int idx = 0; idx < slaveCnt; idx++) {
			inMetadata[1 + idx] = getInputPort(FIRST_SLAVE_PORT + idx).getMetadata();
		}
		if (transformation != null){
			transformation.init(transformationParameters, inMetadata, outMetadata);
		}else{
			transformation = RecordTransformFactory.createTransform(
					transformSource, transformClassName, this, inMetadata, outMetadata, transformationParameters);
		}
	}


	/**
	 * @param transformationParameters
	 *            The transformationParameters to set.
	 */
	public void setTransformationParameters(Properties transformationParameters) {
		this.transformationParameters = transformationParameters;
	}

	/**
	 * read records from all slave input ports and stores them to hashtables 
	 */
	private void loadSlaveData() {
		InputReader[] slaveReader = new InputReader[slaveCnt];
		// read slave ports in separate threads
		for (int idx = 0; idx < slaveCnt; idx++) {
			slaveReader[idx] = new InputReader(idx);
			slaveReader[idx].start();
		}
		// wait for slave input threads to finish their job
		boolean killIt = false;
		for (int idx = 0; idx < slaveCnt; idx++) {
			while (slaveReader[idx].getState() != Thread.State.TERMINATED) {
				if (killIt) {
					slaveReader[idx].interrupt();
					break;
				}
				killIt = !runIt;
				try {
					slaveReader[idx].join(1000);
				} catch (InterruptedException e) {
					logger.warn(getId() + "thread interrupted, it will interrupt child threads", e);
					killIt = true;
				}
			}
		}
	}

	/**
	 * Flush orphaned slaves.
	 * @throws TransformException
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	private void flushOrphaned() throws TransformException, IOException, InterruptedException {
		// flush slaves without driver record
		for (int idx = 0; idx < slaveCnt + 1; idx++) {
			inRecords[idx] = null;
		}
		for (int slaveIdx = 0; slaveIdx < slaveCnt; slaveIdx++) {
			for (Entry<HashKey, MapItem> pair: hashMap[slaveIdx].entrySet()) {
				if (pair.getValue().indicator && slaveDuplicates) {
					continue;	// all slave records in collection were already used
				}
				Iterator<DataRecord> itor = pair.getValue().records.iterator();
				if (pair.getValue().indicator) {
					itor.next();	// first slave record in collection was already used
				}
				// process unused records
				while (itor.hasNext()) {
					if (!runIt) {
						return;
					}
					DataRecord record = itor.next();
					inRecords[FIRST_SLAVE_PORT + slaveIdx] = record;
					if (!transformation.transform(inRecords, outRecords)) {
						logger.error(transformation.getMessage());
						transformation.finished();
						setEOF(WRITE_TO_PORT);
						throw new TransformException(transformation.getMessage());
					}
					outPort.writeRecord(outRecords[0]);
					outRecords[0].reset();
				} // for 
			} // for
			inRecords[FIRST_SLAVE_PORT + slaveIdx] = null;
		} // for all slaves
	}

	/**
	 * Reads all driver records and performs transformation for them
	 * @throws TransformException
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	private void flush() throws TransformException, IOException, InterruptedException {
		HashKey[] hashKey = new HashKey[slaveCnt];
		MapItem[] slaveRecords = new MapItem[slaveCnt];
		DataRecord driverRecord = inRecords[0];

		for (int idx = 0; idx < slaveCnt; idx++) {
			hashKey[idx] = new HashKey(driverKeys[idx], driverRecord);
		}

		while (runIt) {
			int slaveIdx;
			if (driverPort.readRecord(driverRecord) == null) { // no more input data
				break;
			}
			for (slaveIdx = 0; slaveIdx < slaveCnt; slaveIdx++) {
				slaveRecords[slaveIdx] = hashMap[slaveIdx].get(hashKey[slaveIdx]);
				if (slaveRecords[slaveIdx] == null) {
					if (join == Join.INNER) {	// missing slave
						break;
					}
					slaveRecords[slaveIdx] = new MapItem();
					slaveRecords[slaveIdx].records.add(null);
				}
				slaveRecords[slaveIdx].indicator = true;
			}
			if (slaveIdx < slaveCnt) {	// missing slaves
				continue;	// read next driver
			}

			// we need to generate all combinations of slaves 
			int[] cnt = new int[slaveCnt];
			for (slaveIdx = 0; slaveIdx < slaveCnt; slaveIdx++) {
				cnt[slaveIdx] = slaveDuplicates ? slaveRecords[slaveIdx].records.size() : 1;
			}
			for (int recIdx = 0; true; recIdx++) {
				int q = recIdx;
				for (slaveIdx = 0; slaveIdx < slaveCnt - 1; slaveIdx++) {					
					inRecords[1 + slaveIdx] = slaveRecords[slaveIdx].records.get(q%cnt[slaveIdx]);
					q /= cnt[slaveIdx];
				}
				if (q >= cnt[slaveCnt - 1]) { // all combinations exhausted
					break;
				}
				inRecords[1 + slaveCnt - 1] = slaveRecords[slaveIdx].records.get(q);

				if (!transformation.transform(inRecords, outRecords)) {
					logger.error(transformation.getMessage());
					transformation.finished();
					setEOF(WRITE_TO_PORT);
					throw new TransformException(transformation.getMessage());
				}
				outPort.writeRecord(outRecords[0]);
				outRecords[0].reset();
	 		}
			SynchronizeUtils.cloverYield();
		} // while		
	}

	@Override
	public Result execute() throws Exception {
		loadSlaveData();
		flush();
		if (join == Join.FULL_OUTER) {
			flushOrphaned();
		}
		transformation.finished();
		setEOF(WRITE_TO_PORT);
		return runIt ? Node.Result.OK : Node.Result.ABORTED;
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
		} 

		if (transformSource!=null){
			xmlElement.setAttribute(XML_TRANSFORM_ATTRIBUTE,transformSource);
		}

		xmlElement.setAttribute(XML_JOINKEY_ATTRIBUTE, createJoinSpec(driverJoiners, slaveJoiners));


		xmlElement.setAttribute(XML_JOINTYPE_ATTRIBUTE,
				join == Join.FULL_OUTER ? "fullOuter" : join == Join.LEFT_OUTER ? "leftOuter" : "inner");

		if (hashTableInitialCapacity > DEFAULT_HASH_TABLE_INITIAL_CAPACITY ) {
			xmlElement.setAttribute(XML_HASHTABLESIZE_ATTRIBUTE, String.valueOf(hashTableInitialCapacity));
		}

		if (slaveDuplicates){
			xmlElement.setAttribute(XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE, String.valueOf(slaveDuplicates));
		}

		Enumeration propertyAtts = transformationParameters.propertyNames();
		while (propertyAtts.hasMoreElements()) {
			String attName = (String)propertyAtts.nextElement();
			xmlElement.setAttribute(attName,transformationParameters.getProperty(attName));
		}
	}

	private static String createJoinSpec(String[][] driverJoiners, String[][] slaveJoiners) {
		if (driverJoiners.length != slaveJoiners.length) {
			return null;
		}

		String joinStr = "";
		for (int i = 0; true; i++) {
			if (driverJoiners[i].length != slaveJoiners[i].length) {
				return null;
			}
			for (int j = 0; true; j++) {
				joinStr += driverJoiners[i][j] + "=" + slaveJoiners[i][j];
				if (j == driverJoiners[i].length - 1) {
					break;	// leave inner loop
				}
				joinStr += Defaults.Component.KEY_FIELDS_DELIMITER;
			}
			if (i == driverJoiners.length - 1) {
				break;	// leave outer loop
			}
			joinStr += "#";
		}
		return joinStr;
	}

	/**
	 * Parses join string.
	 * @param joinBy Join string
	 * @return Each element of outer array contains array of arrays of strings. Each subarray represents one driver/slave key list.
	 * First element of outer array is for driver key lists, the second one is for slave key lists.
	 * @throws XMLConfigurationException
	 */
	private static String[][][] parseJoiners(String joinBy) throws XMLConfigurationException {	
		String[][][] res = new String[2][][];
		String[] mappings = joinBy.split("#");
		res[0] = new String[mappings.length][];
		res[1] = new String[mappings.length][];

		for (int i = 0; i < mappings.length; i++) {
			if (i > 0 && mappings[i].length() == 0) {
				// use first mapping instead of the empty one
				res[0][i] = res[0][0];
				res[1][i] = res[1][0];
				continue;
			}
			String[] pairs = mappings[i].split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			res[0][i] = new String[pairs.length];	// master key
			res[1][i] = new String[pairs.length];	// slave key
			for (int j = 0; j < pairs.length; j++) {
				String[] fields = pairs[j].split("=", 2);
				if (fields.length == 0) {
					throw new XMLConfigurationException("Invalid key mapping: " + mappings[i]);
				}
				if (fields.length == 1 || fields[1].length() == 0) {	// only master field is specified
					res[0][i][j] = res[1][i][j] = fields[0];	// use it for both master and slave
				} else if (fields[0].length() == 0 && i > 0) {			// only slave key is specified
					res[0][i][j] = res[0][0][j];	// inherit master from first mapping 
					res[1][i][j] = fields[1];
				} else {
					res[0][i][j] = fields[0];
					res[1][i][j] = fields[1];
				}
			}
		}
		return res;
	}

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		HashJoin join;

		try {
			String joinStr = xattribs.getString(XML_JOINTYPE_ATTRIBUTE, "inner");
			Join joinType;
			if (joinStr == null || joinStr.equalsIgnoreCase("inner")) {
				joinType = Join.INNER;
			} else if (joinStr.equalsIgnoreCase("leftOuter")) {
				joinType = Join.LEFT_OUTER;
			} else if (joinStr.equalsIgnoreCase("fullOuter")) {
				joinType = Join.FULL_OUTER;
			} else {
				throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" 
						+ "Invalid joinType specification: " + joinStr);				
			}

			String[][][] joiners = parseJoiners(xattribs.getString(XML_JOINKEY_ATTRIBUTE, ""));

			// legacy attributes handling {
			if (!xattribs.exists(XML_JOINTYPE_ATTRIBUTE) && xattribs.getBoolean(XML_LEFTOUTERJOIN_ATTRIBUTE, false)) {
				joinType = Join.LEFT_OUTER;
			}
			if (xattribs.exists(XML_SLAVEOVERRIDEKEY_ATTRIBUTE)) {
				String[] slaveKeys = xattribs.getString(XML_SLAVEOVERRIDEKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				if (slaveKeys.length != joiners[0][0].length) {
					throw new XMLConfigurationException("Driver key and slave key doesn't match");
				}
				for (int i = 0; i < joiners[1].length; i++) {
					joiners[1][i] = slaveKeys; 
				}
			}
			// }

			join = new HashJoin(
					xattribs.getString(XML_ID_ATTRIBUTE),
					joiners[0], joiners[1],
					xattribs.getString(XML_TRANSFORM_ATTRIBUTE, null), 
					xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
					joinType);

			if (xattribs.exists(XML_HASHTABLESIZE_ATTRIBUTE)) {
				join.setHashTableInitialCapacity(xattribs.getInteger(XML_HASHTABLESIZE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE)) {
				join.setSlaveDuplicates(xattribs.getBoolean(XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE));
			}
			join.setTransformationParameters(xattribs.attributes2Properties(
					new String[]{XML_ID_ATTRIBUTE,XML_JOINKEY_ATTRIBUTE,
							XML_TRANSFORM_ATTRIBUTE,XML_TRANSFORMCLASS_ATTRIBUTE, XML_JOINTYPE_ATTRIBUTE,
							XML_HASHTABLESIZE_ATTRIBUTE,XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE}));

			return join;
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        checkInputPorts(status, 2, Integer.MAX_VALUE);
        checkOutputPorts(status, 1, 1);

        try {
        	
    		driverPort = getInputPort(DRIVER_ON_PORT);
    		outPort = getOutputPort(WRITE_TO_PORT);

    		slaveCnt = inPorts.size() - FIRST_SLAVE_PORT;
    		if (driverJoiners.length < 1) {
    			throw new ComponentNotReadyException("driver key list not specified");
    		}
    		if (driverJoiners.length < slaveCnt) {
    			logger.warn("Driver keys aren't specified for all slave inputs - deducing missing keys");
    			String[][] replJoiners = new String[slaveCnt][];
    			for (int i = 0; i < driverJoiners.length; i++) {
    				replJoiners[i] = driverJoiners[i];
    			}
    			// use first master key specification to deduce all missing driver key specifications
    			for (int i = driverJoiners.length; i < slaveCnt; i++) {
    				replJoiners[i] = driverJoiners[0];
    			}
    			driverJoiners = replJoiners;
    		}
    		if (slaveJoiners.length < slaveCnt) {
    			logger.warn("Slave keys aren't specified for all slave inputs - deducing missing keys");
    			String[][] replJoiners = new String[slaveCnt][];
    			for (int i = 0; i < slaveJoiners.length; i++) {
    				replJoiners[i] = slaveJoiners[i];
    			}
    			// use first master key specification to deduce all missing driver key specifications
    			for (int i = slaveJoiners.length; i < slaveCnt; i++) {
    				replJoiners[i] = driverJoiners[0];
    			}
    			slaveJoiners = replJoiners;
    		}

    		inRecords = new DataRecord[1 + slaveCnt];
    		inRecords[0] = new DataRecord(driverPort.getMetadata());
    		inRecords[0].init();
    		outRecords = new DataRecord[1];
    		outRecords[0] = new DataRecord(outPort.getMetadata());
    		outRecords[0].init();

    		driverKeys = new RecordKey[slaveCnt];
    		slaveKeys = new RecordKey[slaveCnt];
    		for (int idx = 0; idx < slaveCnt; idx++) {
    			driverKeys[idx] = new RecordKey(driverJoiners[idx], driverPort.getMetadata());
    			driverKeys[idx].init();
    			slaveKeys[idx] = new RecordKey(slaveJoiners[idx], getInputPort(FIRST_SLAVE_PORT + idx).getMetadata());
    			slaveKeys[idx].init();
    		}

    		// allocate maps		
    		try {
    			hashMap = (HashMap<HashKey, MapItem>[])new HashMap[slaveCnt];
    			for (int idx = 0; idx < slaveCnt; idx++) {
    				hashMap[idx] = new HashMap<HashKey, MapItem>(hashTableInitialCapacity);
    			}
    		} catch (OutOfMemoryError ex) {
    			logger.fatal(ex);
    			throw new ComponentNotReadyException("Can't allocate HashMap of size: "	+ hashTableInitialCapacity);
    		}
        	
        	
//            init();
//            free();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        }
        
        return status;
	}

	public String getType(){
		return COMPONENT_TYPE;
	}

	public boolean isSlaveDuplicates() {
		return slaveDuplicates;
	}

	public void setSlaveDuplicates(boolean slaveDuplicates) {
		this.slaveDuplicates = slaveDuplicates;
	}
	
	/**
	 * Represents a set of slave records with identical key.
	 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
	 *
	 */
	private static class MapItem {
		public ArrayList<DataRecord> records;
		public boolean indicator;
		public MapItem() {
			records = new ArrayList<DataRecord>(1);
			indicator = false;
		}
	}
	
	/**
	 * Reads records from one slave input and stores them to appropriate data structures.
	 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
	 *
	 */
	private class InputReader extends Thread {
		private InputPort inPort;
		private Map<HashKey, MapItem> map;
		RecordKey recKey;
		DataRecordMetadata metadata;

		public InputReader(int slaveIdx) {
			super(Thread.currentThread().getName() + ".InputThread#" + slaveIdx);
			runIt = true;
			map = hashMap[slaveIdx];
			inPort = getInputPort(FIRST_SLAVE_PORT + slaveIdx);
			metadata = inPort.getMetadata();
			recKey = slaveKeys[slaveIdx];
		}
		
		public void run() {
			while (runIt) {
				try {
					DataRecord record = new DataRecord(metadata);
					record.init();
					if (inPort.readRecord(record) == null) { // no more input data
						return;
					}
					HashKey key = new HashKey(recKey, record);
					MapItem item = map.get(key);
					if (item == null) { // this is first record associated with current key
						// create map item
						item = new MapItem();
						// put it into map
						map.put(key, item);
					}
					item.records.add(record);
				} catch (InterruptedException e) {
					logger.error(getId() + ": thread forcibly aborted", e);
					return;
				} catch (IOException e) {
					logger.error(getId() + ": thread failed", e);
					return;
				}					
			} // while
		}
	}

}

