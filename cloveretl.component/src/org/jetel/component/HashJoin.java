/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.component;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.DataRecordMap;
import org.jetel.data.DataRecordMap.DataRecordIterator;
import org.jetel.data.DataRecordMap.DataRecordLookup;
import org.jetel.data.Defaults;
import org.jetel.data.NullRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.joinKey.JoinKeyUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
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
 *  <tr><td><b>transform</b></td><td>contains definition of transformation as java source, in internal clover format or in Transformation Language</td>
 *    <tr><td><b>transformClass</b><br><i>optional</i></td><td>name of the class to be used for transforming joined data<br>
 *    If no class name is specified then it is expected that the transformation Java source code is embedded in XML - <i>see example
 * below</i></td></tr>
 *  <tr><td><b>transformURL</b></td><td>path to the file with transformation code for
 *  	 joined records which has conformity smaller then conformity limit</td></tr>
 *  <tr><td><b>charset</b><i>optional</i></td><td>encoding of extern source</td></tr>
 *    <tr><td><b>joinType</b><br><i>optional</i></td><td>inner/leftOuter/fullOuter Specifies type of join operation. Default is inner.</td></tr>
 *    <tr><td><b>hashTableSize</b><br><i>optional</i></td><td>how many records are expected (roughly) to be in hashtable.</td></tr>
 *    <tr><td><b>slaveDuplicates</b><br><i>optional</i></td><td>true/false - allow records on slave port with duplicate keys. Default is false - multiple
 *    duplicate records are discarded - only the first one is used for join.</td></tr>
 *  <tr><td><b>errorActions </b><i>optional</i></td><td>defines if graph is to stop, when transformation returns negative value.
 *  Available actions are: STOP or CONTINUE. For CONTINUE action, error message is logged to console or file (if errorLog attribute
 *  is specified) and for STOP there is thrown TransformExceptions and graph execution is stopped. <br>
 *  Error action can be set for each negative value (value1=action1;value2=action2;...) or for all values the same action (STOP 
 *  or CONTINUE). It is possible to define error actions for some negative values and for all other values (MIN_INT=myAction).
 *  Default value is <i>-1=CONTINUE;MIN_INT=STOP</i></td></tr>
 *  <tr><td><b>errorLog</b><br><i>optional</i></td><td>path to the error log file. Each error (after which graph continues) is logged in 
 *  following way: masterRecordNumber;errorCode;errorMessage;semiResult - fields are delimited by Defaults.Component.KEY_FIELDS_DELIMITER.</td></tr>
 *  <tr><td><i>..optional attribute..</i></td><td>any additional attribute is passed to transformation
 * class in Properties object - as a key->value pair. There is no limit to how many optional
 * attributes can be used.</td>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="JOIN" type="HASH_JOIN" joinKey="CustomerID" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *	  
 *<pre>&lt;Node id="JOIN" type="HASH_JOIN" joinKey="EmployeeID*ID" joinType="inner"&gt;
 *import org.jetel.component.DataRecordTransform;
 *import org.jetel.data.*;
 * 
 *public class reformatJoinTest extends DataRecordTransform{
 *
 *	public int transform(DataRecord[] source, DataRecord[] target){
 *		
 *		target[0].getField(0).setValue(source[0].getField(0).getValue());
 *		target[0].getField(1).setValue(source[0].getField(1).getValue());
 *		target[0].getField(2).setValue(source[0].getField(2).getValue());
 *
 *		if (source[1]!=null){
 *			target[0].getField(3).setValue(source[1].getField(0).getValue());
 *			target[0].getField(4).setValue(source[1].getField(1).getValue());
 *		}
 *
 *		return ALL;
 *	}
 *}
 *
 *&lt;/Node&gt;</pre>
 *	  
 * @author      dpavlis, Jan Hadrava
 * @since       March 09, 2004
 * @revision    $Revision$
 * @created     09. March 2004
 *
 *
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class HashJoin extends Node {
	public enum Join {
		INNER, LEFT_OUTER, FULL_OUTER,
	}

	private static final String XML_HASHTABLESIZE_ATTRIBUTE = "hashTableSize";
	private static final String XML_JOINTYPE_ATTRIBUTE = "joinType";
	private static final String XML_JOINKEY_ATTRIBUTE = "joinKey";
	private static final String XML_TRANSFORMCLASS_ATTRIBUTE = "transformClass";
	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_TRANSFORMURL_ATTRIBUTE = "transformURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE = "slaveDuplicates";
	// legacy attributes
	private static final String XML_LEFTOUTERJOIN_ATTRIBUTE = "leftOuterJoin";
	private static final String XML_SLAVEOVERRIDEKEY_ATTRIBUTE = "slaveOverrideKey";
	private static final String XML_ERROR_ACTIONS_ATTRIBUTE = "errorActions";
	private static final String XML_ERROR_LOG_ATTRIBUTE = "errorLog";

	/** Description of the Field */
	public final static String COMPONENT_TYPE = "HASH_JOIN";

	private final static int DEFAULT_HASH_TABLE_INITIAL_CAPACITY = 512;

	private final static int WRITE_TO_PORT = 0;
	private final static int DRIVER_ON_PORT = 0;
	private final static int FIRST_SLAVE_PORT = 1;

	private String transformClassName;

	private RecordTransform transformation = null;
	private String transformSource = null;
	private String transformURL = null;
	private String charset = null;

	private Join join;
	private boolean slaveDuplicates = false;

	private String[][] driverJoiners;
	private String[][] slaveJoiners;

	private RecordKey[] driverKeys;
	private RecordKey[] slaveKeys;

	private boolean slaveOverriden = false;

	private DataRecordMap[] hashMap;
	private int hashTableInitialCapacity;

	private Properties transformationParameters;

	static Log logger = LogFactory.getLog(HashJoin.class);

	private int slaveCnt;

	private InputPort driverPort;
	private OutputPort outPort;
	DataRecord[] inRecords;
	DataRecord[] outRecords;
	private String joinKey;
	private String slaveOverrideKey;

	private String errorActionsString;
	private Map<Integer, ErrorAction> errorActions = new HashMap<Integer, ErrorAction>();
	private String errorLogURL;
	private FileWriter errorLog;
	private int masterCounter;

	/**
	 *Constructor for the HashJoin object
	 * 
	 * @param id
	 *            Description of the Parameter
	 * @param driverJoiners
	 *            Array of driver joiners (each element contains list of join keys for one slave)
	 * @param slaveJoiners
	 *            Array of slave joiners (each element contains list of join keys for one slave)
	 * @param transform
	 * @param transformClass
	 *            class (name) to be used for transforming data
	 * @param join
	 *            join type
	 * @param slaveDuplicates
	 *            enables/disables duplicate slaves
	 */
	public HashJoin(String id, String[][] driverJoiners, String[][] slaveJoiners, String transform,
			String transformClass, String transformURL, Join join, boolean slaveOverriden) {
		super(id);
		this.transformSource = transform;
		this.transformClassName = transformClass;
		this.transformURL = transformURL;
		this.join = join;
		this.hashTableInitialCapacity = DEFAULT_HASH_TABLE_INITIAL_CAPACITY;
		this.driverJoiners = driverJoiners;
		this.slaveJoiners = slaveJoiners;
		this.slaveOverriden = slaveOverriden;
	}

	public HashJoin(String id, String[][] driverJoiners, String[][] slaveJoiners, RecordTransform transform, Join join,
			boolean slaveOverriden) {
		this(id, driverJoiners, slaveJoiners, null, null, null, join, slaveOverriden);
		this.transformation = transform;
	}

	public HashJoin(String id, String joinKey, String transform, String transformClass, String transformURL, Join join) {
		super(id);
		this.transformSource = transform;
		this.transformClassName = transformClass;
		this.transformURL = transformURL;
		this.join = join;
		this.hashTableInitialCapacity = DEFAULT_HASH_TABLE_INITIAL_CAPACITY;
		this.joinKey = joinKey;
	}

	public HashJoin(String id, String joinKey, RecordTransform transform, Join join) {
		this(id, joinKey, null, null, null, join);
		this.transformation = transform;
	}

	// /**
	// * Sets the leftOuterJoin attribute of the HashJoin object
	// *
	// * @param outerJoin The new leftOuterJoin value
	// */
	// public void setLeftOuterJoin(boolean outerJoin) {
	// leftOuterJoin = outerJoin;
	// }

	/**
	 * Sets the hashTableInitialCapacity attribute of the HashJoin object
	 * 
	 * @param capacity
	 *            The new hashTableInitialCapacity value
	 */
	public void setHashTableInitialCapacity(int capacity) {
		if (capacity > DEFAULT_HASH_TABLE_INITIAL_CAPACITY) {
			hashTableInitialCapacity = capacity;
		}
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized())
			return;
		super.init();

		driverPort = getInputPort(DRIVER_ON_PORT);
		outPort = getOutputPort(WRITE_TO_PORT);

		slaveCnt = inPorts.size() - FIRST_SLAVE_PORT;
		if (driverJoiners == null) {// need to parse join key
			List<DataRecordMetadata> inMetadata = getInMetadata();
			String[][][] joiners = JoinKeyUtils.parseHashJoinKey(joinKey, inMetadata);
			driverJoiners = joiners[0];
			if (slaveOverrideKey != null) {
				String[] slaveKeys = slaveOverrideKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				if (slaveKeys.length != joiners[0][0].length) {
					throw new ComponentNotReadyException(this, XML_SLAVEOVERRIDEKEY_ATTRIBUTE, "Driver key and slave key doesn't match");
				}
				for (int i = 0; i < joiners[1].length; i++) {
					joiners[1][i] = slaveKeys;
				}
			}
			slaveJoiners = joiners[1];
		}
		if (driverJoiners.length < 1) {
			throw new ComponentNotReadyException(this, XML_JOINKEY_ATTRIBUTE, "Driver key list not specified");
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
		inRecords[0] = DataRecordFactory.newRecord(driverPort.getMetadata());
		inRecords[0].init();
		outRecords = new DataRecord[1];
		outRecords[0] = DataRecordFactory.newRecord(outPort.getMetadata());
		outRecords[0].init();
		outRecords[0].reset();

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
			hashMap = new DataRecordMap[slaveCnt];
			for (int idx = 0; idx < slaveCnt; idx++) {
				hashMap[idx] = new DataRecordMap(slaveKeys[idx], slaveDuplicates, hashTableInitialCapacity, false);
			}
		} catch (OutOfMemoryError ex) {
			logger.fatal(ex);
			throw new ComponentNotReadyException("Can't allocate HashMap of size: " + hashTableInitialCapacity);
		}

		// init transformation
		if (transformation == null) {
			transformation = getTransformFactory().createTransform();
		}
		// init transformation
        if (!transformation.init(transformationParameters, getInMetadataArray(), getOutMetadataArray())) {
            throw new ComponentNotReadyException("Error when initializing tranformation function.");
        }
		errorActions = ErrorAction.createMap(errorActionsString);
		if (errorLogURL != null) {
			try {
				errorLog = new FileWriter(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), errorLogURL));
			} catch (IOException e) {
				throw new ComponentNotReadyException(this, XML_ERROR_LOG_ATTRIBUTE, e);
			}
		}
	}

	private TransformFactory<RecordTransform> getTransformFactory() {
    	TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
    	transformFactory.setTransform(transformSource);
    	transformFactory.setTransformClass(transformClassName);
    	transformFactory.setTransformUrl(transformURL);
    	transformFactory.setCharset(charset);
    	transformFactory.setComponent(this);
    	transformFactory.setInMetadata(getInMetadata());
    	transformFactory.setOutMetadata(getOutMetadata());
    	return transformFactory;
	}
	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		transformation.preExecute();

		if (firstRun()) {// a phase-dependent part of initialization
			// all necessary elements have been initialized in init()
		} else {
			inRecords[0] = DataRecordFactory.newRecord(driverPort.getMetadata());
			inRecords[0].init();
			transformation.reset();
		}
		if (errorLogURL != null) {
			try {
				errorLog = new FileWriter(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), errorLogURL));
			} catch (IOException e) {
				throw new ComponentNotReadyException(this, XML_ERROR_LOG_ATTRIBUTE, e);
			}
		}
	}

	@Override
	public void reset() throws ComponentNotReadyException {
		super.reset();
	}

	@Override
	public void free() {
		hashMap = null;
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
					logger.debug(getId() + " thread interrupted, it will interrupt child threads", e);
					killIt = true;
				}
			}
		}
	}

	/**
	 * Flush orphaned slaves.
	 * 
	 * @throws TransformException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void flushOrphaned() throws TransformException, IOException, InterruptedException {
		// flush slaves without driver record
		for (int idx = 0; idx < slaveCnt + 1; idx++) {
			inRecords[idx] = NullRecord.NULL_RECORD;
		}

		for (int slaveIdx = 0; slaveIdx < slaveCnt; slaveIdx++) {
			Iterator<DataRecord> itor = hashMap[slaveIdx].getOrphanedIterator();
			while (itor.hasNext()) {
				if (!runIt) {
					return;
				}
				transformAndWriteRecord(itor.next(), slaveIdx);
			}
			inRecords[FIRST_SLAVE_PORT + slaveIdx] = NullRecord.NULL_RECORD;
		}
	}

	private void transformAndWriteRecord(DataRecord record, int slaveIdx) throws TransformException, IOException,
			InterruptedException {
		inRecords[FIRST_SLAVE_PORT + slaveIdx] = record;
		int transformResult = -1;

		try {
			transformResult = transformation.transform(inRecords, outRecords);
		} catch (Exception exception) {
			transformResult = transformation.transformOnError(exception, inRecords, outRecords);
		}

		if (transformResult < 0) {
			handleException(transformation, transformResult, masterCounter);
		} else {
			outPort.writeRecord(outRecords[0]);
		}
		outRecords[0].reset();
	}

	/**
	 * Reads all driver records and performs transformation for them
	 * 
	 * @throws TransformException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void flush() throws TransformException, IOException, InterruptedException {
		DataRecord driverRecord = inRecords[0];
		masterCounter = 0;
		// move to preExecute/init?
		DataRecordLookup[] mapLookups = new DataRecordLookup[slaveCnt];
		for (int i = 0; i < slaveCnt; i++) {
			mapLookups[i] = hashMap[i].createDataRecordLookup(driverKeys[i], driverRecord);
		}
		// end of move

		if (slaveDuplicates)
			flushMulti(mapLookups, driverRecord);
		else
			flushSingle(mapLookups, driverRecord);
	}

	/**
	 * @param mapLookups
	 */
	private void flushSingle(DataRecordLookup[] mapLookups, DataRecord driverRecord) throws TransformException,
			IOException, InterruptedException {
		while (runIt && driverPort.readRecord(driverRecord) != null) {
			int slaveIdx;

			for (slaveIdx = 0; slaveIdx < slaveCnt; slaveIdx++) {
				inRecords[1 + slaveIdx] = mapLookups[slaveIdx].getAndMark();
				if (inRecords[1 + slaveIdx] == null) {
					if (join == Join.INNER) { // missing slave
						break;
					}
					inRecords[1 + slaveIdx] = NullRecord.NULL_RECORD;
				}
			}
			if (slaveIdx < slaveCnt) { // missing slaves
				continue; // read next driver
			}

			int transformResult = -1;

			try {
				transformResult = transformation.transform(inRecords, outRecords);
			} catch (Exception exception) {
				transformResult = transformation.transformOnError(exception, inRecords, outRecords);
			}

			if (transformResult < 0) {
				handleException(transformation, transformResult, masterCounter);
			} else {
				outPort.writeRecord(outRecords[0]);
			}

			outRecords[0].reset();

			SynchronizeUtils.cloverYield();
			masterCounter++;
		}
	}

	/**
	 * @param mapLookups
	 */
	private void flushMulti(DataRecordLookup[] mapLookups, DataRecord driverRecord) throws TransformException,
			IOException, InterruptedException {
		DataRecordIterator[] iterators = new DataRecordIterator[slaveCnt];
		while (runIt && driverPort.readRecord(driverRecord) != null) {
			int slaveIdx;

			for (slaveIdx = 0; slaveIdx < slaveCnt; slaveIdx++) {
				iterators[slaveIdx] = mapLookups[slaveIdx].getAllAndMark();
				if (iterators[slaveIdx] == null) {
					if (join == Join.INNER) { // missing slave
						break;
					}
					iterators[slaveIdx] = hashMap[0].getNULLIterator();
				}
			}
			if (slaveIdx < slaveCnt) { // missing slaves
				continue; // read next driver
			}

			for (int i = 0; i < iterators.length; i++) {
				inRecords[i + 1] = iterators[i].next();
			}
			int currentIterator = iterators.length - 1;

			while (currentIterator >= 0) {
				transform();

				while (iterators[currentIterator].hasNext()) {
					inRecords[currentIterator + 1] = iterators[currentIterator].next();
					transform();
				}
				currentIterator--;
				while (currentIterator >= 0) {
					if (iterators[currentIterator].hasNext()) {
						inRecords[currentIterator + 1] = iterators[currentIterator].next();

						for (int i = currentIterator + 1; i < iterators.length; i++) {
							iterators[i].reset();
							inRecords[i + 1] = iterators[i].next();
						}

						currentIterator = iterators.length - 1;
						break;
					}
					currentIterator--;
				}
			}
			SynchronizeUtils.cloverYield();
			masterCounter++;
		}
	}

	private void transform() throws TransformException, IOException, InterruptedException {
		int transformResult = -1;

		try {
			transformResult = transformation.transform(inRecords, outRecords);
		} catch (Exception exception) {
			transformResult = transformation.transformOnError(exception, inRecords, outRecords);
		}

		if (transformResult < 0) {
			handleException(transformation, transformResult, masterCounter);
		} else {
			outPort.writeRecord(outRecords[0]);
		}
		outRecords[0].reset();
	}

	private void handleException(RecordTransform transform, int transformResult, int recNo) throws TransformException,
			IOException {
		ErrorAction action = errorActions.get(transformResult);
		if (action == null) {
			action = errorActions.get(Integer.MIN_VALUE);
			if (action == null) {
				action = ErrorAction.DEFAULT_ERROR_ACTION;
			}
		}
		String message = "Transformation for master record:\n " + inRecords[0] + "finished with code: " + transformResult + ". Error message: " + transformation.getMessage();
		if (action == ErrorAction.CONTINUE) {
			if (errorLog != null) {
				errorLog.write(String.valueOf(recNo));
				errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
				errorLog.write(String.valueOf(transformResult));
				errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
				message = transformation.getMessage();
				if (message != null) {
					errorLog.write(message);
				}
				errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
				Object semiResult = transformation.getSemiResult();
				if (semiResult != null) {
					errorLog.write(semiResult.toString());
				}
				errorLog.write("\n");
			} else {
				//CL-2020
				//if no error log is defined, the message is quietly ignored
				//without messy logging in console
				//only in case non empty message given from transformation, the message is printed out
				if (!StringUtils.isEmpty(transformation.getMessage())) {
					logger.warn(message);
				}
			}
		} else {
			throw new TransformException(message);
		}
	}

	@Override
	public Result execute() throws Exception {
		loadSlaveData();
		flush();

		if (join == Join.FULL_OUTER) {
			flushOrphaned();
		}

		if (errorLog != null) {
			errorLog.flush();
		}

		setEOF(WRITE_TO_PORT);

		return (runIt ? Result.FINISHED_OK : Result.ABORTED);
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();

		if (hashMap != null) {
			for (DataRecordMap mapItem : hashMap) {
				mapItem.clear();
			}
		}

		transformation.postExecute();
		transformation.finished();
		
		try {
			if (errorLog != null) {
				errorLog.close();
			}
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}

	}

	/**
	 * Description of the Method
	 * 
	 * @return Description of the Returned Value
	 * @since May 21, 2002
	 */
	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);

		if (transformClassName != null) {
			xmlElement.setAttribute(XML_TRANSFORMCLASS_ATTRIBUTE, transformClassName);
		}

		if (transformSource != null) {
			xmlElement.setAttribute(XML_TRANSFORM_ATTRIBUTE, transformSource);
		}

		if (transformURL != null) {
			xmlElement.setAttribute(XML_TRANSFORMURL_ATTRIBUTE, transformURL);
		}

		if (charset != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charset);
		}
		xmlElement.setAttribute(XML_JOINKEY_ATTRIBUTE, createJoinSpec(driverJoiners, slaveJoiners));

		xmlElement.setAttribute(XML_JOINTYPE_ATTRIBUTE, join == Join.FULL_OUTER ? "fullOuter" : join == Join.LEFT_OUTER ? "leftOuter" : "inner");

		if (hashTableInitialCapacity > DEFAULT_HASH_TABLE_INITIAL_CAPACITY) {
			xmlElement.setAttribute(XML_HASHTABLESIZE_ATTRIBUTE, String.valueOf(hashTableInitialCapacity));
		}

		if (slaveDuplicates) {
			xmlElement.setAttribute(XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE, String.valueOf(slaveDuplicates));
		}
		if (errorActionsString != null) {
			xmlElement.setAttribute(XML_ERROR_ACTIONS_ATTRIBUTE, errorActionsString);
		}

		if (errorLogURL != null) {
			xmlElement.setAttribute(XML_ERROR_LOG_ATTRIBUTE, errorLogURL);
		}

		Enumeration propertyAtts = transformationParameters.propertyNames();
		while (propertyAtts.hasMoreElements()) {
			String attName = (String) propertyAtts.nextElement();
			xmlElement.setAttribute(attName, transformationParameters.getProperty(attName));
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
					break; // leave inner loop
				}
				joinStr += Defaults.Component.KEY_FIELDS_DELIMITER;
			}
			if (i == driverJoiners.length - 1) {
				break; // leave outer loop
			}
			joinStr += "#";
		}
		return joinStr;
	}

	/**
	 * Description of the Method
	 * 
	 * @param nodeXML
	 *            Description of Parameter
	 * @return Description of the Returned Value
	 * @throws AttributeNotFoundException 
	 * @since May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		HashJoin join;

		String joinStr = xattribs.getString(XML_JOINTYPE_ATTRIBUTE, "inner");
		Join joinType;

		if (joinStr == null || joinStr.equalsIgnoreCase("inner")) {
			joinType = Join.INNER;
		} else if (joinStr.equalsIgnoreCase("leftOuter")) {
			joinType = Join.LEFT_OUTER;
		} else if (joinStr.equalsIgnoreCase("fullOuter")) {
			joinType = Join.FULL_OUTER;
		} else {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ":" + "Invalid joinType specification: " + joinStr);
		}

		// legacy attributes handling {
		if (!xattribs.exists(XML_JOINTYPE_ATTRIBUTE) && xattribs.exists(XML_LEFTOUTERJOIN_ATTRIBUTE)) {
			joinType = xattribs.getBoolean(XML_LEFTOUTERJOIN_ATTRIBUTE) ? Join.LEFT_OUTER : Join.INNER;
		}

		join = new HashJoin(xattribs.getString(XML_ID_ATTRIBUTE), 
				xattribs.getString(XML_JOINKEY_ATTRIBUTE), 
				xattribs.getStringEx(XML_TRANSFORM_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF), 
				xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null), 
				xattribs.getStringEx(XML_TRANSFORMURL_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF), 
				joinType);

		if (xattribs.exists(XML_SLAVEOVERRIDEKEY_ATTRIBUTE)) {
			join.setSlaveOverrideKey(xattribs.getString(XML_SLAVEOVERRIDEKEY_ATTRIBUTE));
		}
		join.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE, null));
		if (xattribs.exists(XML_HASHTABLESIZE_ATTRIBUTE)) {
			join.setHashTableInitialCapacity(xattribs.getInteger(XML_HASHTABLESIZE_ATTRIBUTE));
		}
		if (xattribs.exists(XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE)) {
			join.setSlaveDuplicates(xattribs.getBoolean(XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE));
		}
		if (xattribs.exists(XML_ERROR_ACTIONS_ATTRIBUTE)) {
			join.setErrorActions(xattribs.getString(XML_ERROR_ACTIONS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_ERROR_LOG_ATTRIBUTE)) {
			join.setErrorLog(xattribs.getString(XML_ERROR_LOG_ATTRIBUTE));
		}
		join.setTransformationParameters(xattribs.attributes2Properties(new String[] { XML_ID_ATTRIBUTE, XML_JOINKEY_ATTRIBUTE, XML_TRANSFORM_ATTRIBUTE, XML_TRANSFORMCLASS_ATTRIBUTE, XML_JOINTYPE_ATTRIBUTE, XML_HASHTABLESIZE_ATTRIBUTE, XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE }));
		return join;
	}

	public void setErrorLog(String errorLog) {
		this.errorLogURL = errorLog;
	}

	public void setErrorActions(String string) {
		this.errorActionsString = string;
	}

	/**
	 * Description of the Method
	 * 
	 * @return Description of the Return Value
	 */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkInputPorts(status, 2, Integer.MAX_VALUE) || !checkOutputPorts(status, 1, 1)) {
			return status;
		}

		if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }

		int slaveCnt = inPorts.size() - FIRST_SLAVE_PORT;

		try {

			driverPort = getInputPort(DRIVER_ON_PORT);
			outPort = getOutputPort(WRITE_TO_PORT);

			if (driverJoiners == null) {
				List<DataRecordMetadata> inMetadata = getInMetadata();
				String[][][] joiners = JoinKeyUtils.parseHashJoinKey(joinKey, inMetadata);
				driverJoiners = joiners[0];
				if (slaveOverrideKey != null) {
					String[] slaveKeys = slaveOverrideKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
					if (slaveKeys.length != joiners[0][0].length) {
						throw new ComponentNotReadyException(this, XML_SLAVEOVERRIDEKEY_ATTRIBUTE, "Driver key and slave key doesn't match");
					}
					for (int i = 0; i < joiners[1].length; i++) {
						joiners[1][i] = slaveKeys;
					}
				}
				slaveJoiners = joiners[1];
			}
			if (driverJoiners.length < 1) {
				throw new ComponentNotReadyException(this, XML_JOINKEY_ATTRIBUTE, "Driver key list not specified");
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
			inRecords[0] = DataRecordFactory.newRecord(driverPort.getMetadata());
			inRecords[0].init();
			outRecords = new DataRecord[1];
			outRecords[0] = DataRecordFactory.newRecord(outPort.getMetadata());
			outRecords[0].init();

			driverKeys = new RecordKey[slaveCnt];
			slaveKeys = new RecordKey[slaveCnt];
			for (int idx = 0; idx < slaveCnt; idx++) {
				driverKeys[idx] = new RecordKey(driverJoiners[idx], driverPort.getMetadata());
				slaveKeys[idx] = new RecordKey(slaveJoiners[idx], getInputPort(FIRST_SLAVE_PORT + idx).getMetadata());

				if (slaveOverriden) {
					RecordKey.checkKeys(driverKeys[idx], XML_JOINKEY_ATTRIBUTE, slaveKeys[idx], XML_SLAVEOVERRIDEKEY_ATTRIBUTE, status, this);
				} else {
					RecordKey.checkKeys(driverKeys[idx], XML_JOINKEY_ATTRIBUTE, slaveKeys[idx], XML_JOINKEY_ATTRIBUTE, status, this);
				}
			}

			if (errorActionsString != null) {
				ErrorAction.checkActions(errorActionsString);
			}

			if (errorLog != null) {
				FileUtils.canWrite(getGraph().getRuntimeContext().getContextURL(), errorLogURL);
			}

			// init();
			// free();
		} catch (ComponentNotReadyException e) {
			ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.exceptionChainToMessage(e), ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL);
			if (!StringUtils.isEmpty(e.getAttributeName())) {
				problem.setAttributeName(e.getAttributeName());
			}
			status.add(problem);
		}

        //check transformation
		if (transformation == null) {
			getTransformFactory().checkConfig(status);
		}

		return status;
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	public boolean isSlaveDuplicates() {
		return slaveDuplicates;
	}

	public void setSlaveDuplicates(boolean slaveDuplicates) {
		this.slaveDuplicates = slaveDuplicates;
	}

	/**
	 * Reads records from one slave input and stores them to appropriate data structures.
	 * 
	 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
	 * 
	 */
	private class InputReader extends Thread {
		private InputPort inPort;
		private DataRecordMap map;
		DataRecordMetadata metadata;

		public InputReader(int slaveIdx) {
			super(Thread.currentThread().getName() + ".InputThread#" + slaveIdx);
			runIt = true;
			map = hashMap[slaveIdx];
			inPort = getInputPort(FIRST_SLAVE_PORT + slaveIdx);
			metadata = inPort.getMetadata();
		}

		@Override
		public void run() {
			DataRecord record = DataRecordFactory.newRecord(metadata);
			record.init();

			while (runIt) {
				try {
					if (inPort.readRecord(record) == null) { // no more input data
						return;
					}
					map.put(record.duplicate());
				} catch (InterruptedException e) {
					logger.debug(getId() + ": thread forcibly aborted", e);
					return;
				} catch (IOException e) {
					logger.error(getId() + ": thread failed", e);
					return;
				}
			} // while
		}
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setSlaveOverrideKey(String slaveOverrideKey) {
		this.slaveOverrideKey = slaveOverrideKey;
	}
}
