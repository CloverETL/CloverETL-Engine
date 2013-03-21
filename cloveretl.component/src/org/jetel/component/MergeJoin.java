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
import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.NullRecord;
import org.jetel.data.RecordOrderedKey;
import org.jetel.data.reader.DriverReader;
import org.jetel.data.reader.IInputReader;
import org.jetel.data.reader.IInputReader.InputOrdering;
import org.jetel.data.reader.SlaveReader;
import org.jetel.data.reader.SlaveReaderDup;
import org.jetel.enums.OrderEnum;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.MiscUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.joinKey.JoinKeyUtils;
import org.jetel.util.joinKey.OrderedKey;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
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
 *  Joins sorted records on input ports. It expects driver stream at port [0] and 
 *  slave streams at other input ports.
 *  Each driver record is joined with corresponding slave records according
 *  to join keys specification.<br> 
 *	The method <i>transform</i> is every tuple composed of driver and corresponding
 *  slaves.<br>
 *  There are three join modes available: inner, left outer, full outer.<br>
 *  Inner mode processess only driver records for which all associated slaves are available.
 *  Left outer mode furthermore processes driver records with missing slaves.
 *  Full outer mode additionally calls transformation method for slaves without driver.<br>
 *  In case you use outer mode, be sure your transformation code is able to handle null
 *  input records.
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0] - sorted driver record input<br>
 *	  [1+] - sorted slave record inputs<br>
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
 *    <tr><td><b>type</b></td><td>"MERGE_JOIN"</td></tr>
 *    <tr><td><b>id</b></td><td>component identification</td></tr>
 *    <tr><td><b>joinKey</b></td><td>join key specification in format<br>
 *    <tt>mapping1#mapping2...</tt>, where <tt>mapping</tt> has format
 *    <tt>driver_field1=slave_field1|driver_field2=slave_field2|...</tt><br>
 *    In case slave_field is missing it is supposed to be the same as the driver_field. When driver_field
 *    is missing (ie there's nothin before '='), it will be taken from the first mapping. 
 *    Order of mappings corresponds to order of slave input ports. In case a mapping is empty or missing for some slave, the component
 *    will use first mapping instead of it.</td></tr>
 *   <tr><td><b>transformClass</b><br><i>optional</i></td><td>name of the class to be used for transforming joined data<br>
 *    If no class name is specified then it is expected that the transformation Java source code is embedded in XML - <i>see example
 * below</i></td></tr>
 *  <tr><td><b>transform</b></td><td>contains definition of transformation as java source, in internal clover format or in Transformation Language</td></tr>
 *  <tr><td><b>transformURL</b></td><td>path to the file with transformation code for
 *  	 joined records which has conformity smaller then conformity limit</td></tr>
 *  <tr><td><b>charset</b><i>optional</i></td><td>encoding of extern source</td></tr>
 *    <tr><td><b>joinType</b><br><i>optional</i></td><td>inner/leftOuter/fullOuter Specifies type of join operation. Default is inner.</td></tr>
 *    <tr><td><b>slaveDuplicates</b><br><i>optional</i></td><td>true/false - allow records on slave port with duplicate keys. If false - multiple
 *    duplicate records are discarded - only the last one is used for join. Default is true.</td></tr>
 *    <tr><td><b>ascendingInputs</b><br><i>optional</i></td><td>true/false - Switch for inputs data ordering (true=ascending, false=descending) Value of this attribute suggests this component how to process data. All inputs must be ordered in the same way. Default is true.</td></tr>
 *  <tr><td><b>errorActions </b><i>optional</i></td><td>defines if graph is to stop, when transformation returns negative value.
 *  Available actions are: STOP or CONTINUE. For CONTINUE action, error message is logged to console or file (if errorLog attribute
 *  is specified) and for STOP there is thrown TransformExceptions and graph execution is stopped. <br>
 *  Error action can be set for each negative value (value1=action1;value2=action2;...) or for all values the same action (STOP 
 *  or CONTINUE). It is possible to define error actions for some negative values and for all other values (MIN_INT=myAction).
 *  Default value is <i>-1=CONTINUE;MIN_INT=STOP</i></td></tr>
 *  <tr><td><b>errorLog</b><br><i>optional</i></td><td>path to the error log file. Each error (after which graph continues) is logged in 
 *  following way: keyFieldsValue;errorCode;errorMessage;semiResult - fields are delimited by Defaults.Component.KEY_FIELDS_DELIMITER.</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="JOIN" type="MERGE_JOIN" joinKey="CustomerID" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *<pre>&lt;Node id="JOIN" type="HASH_JOIN" joinKey="EmployeeID*EmployeeID" joinType="inner"&gt;
 *import org.jetel.component.DataRecordTransform;
 *import org.jetel.data.*;
 * 
 *public class reformatJoinTest extends DataRecordTransform{
 *
 *	public int transform(DataRecord[] source, DataRecord[] target){
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
 *
 *}
 *
 *&lt;/Node&gt;</pre>
 * @author      dpavlis, Jan Hadrava
 * @since       April 4, 2002
 * @revision    $Revision$
 * @created     4. June 2003
 *
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class MergeJoin extends Node {
	public enum Join {
		INNER,
		LEFT_OUTER,
		FULL_OUTER,
	}
	
	private static final String XML_JOINTYPE_ATTRIBUTE = "joinType";
	public static final String XML_JOINKEY_ATTRIBUTE = "joinKey";
	private static final String XML_TRANSFORMCLASS_ATTRIBUTE = "transformClass";
	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_TRANSFORMURL_ATTRIBUTE = "transformURL";
	private static final String XML_LOCALE_ATTRIBUTE = "locale";
	private static final String XML_CASE_SENSITIVE_ATTRIBUTE = "caseSensitive";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE ="slaveDuplicates";
	// legacy attributes
	private static final String XML_FULLOUTERJOIN_ATTRIBUTE = "fullOuterJoin";
	private static final String XML_LEFTOUTERJOIN_ATTRIBUTE = "leftOuterJoin";
	public static final String XML_SLAVEOVERRIDEKEY_ATTRIBUTE = "slaveOverrideKey";
	private static final String XML_ASCENDING_INPUTS_ATTRIBUTE ="ascendingInputs";
	private static final String XML_ERROR_ACTIONS_ATTRIBUTE = "errorActions";
    private static final String XML_ERROR_LOG_ATTRIBUTE = "errorLog";
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "MERGE_JOIN";

	private final static int WRITE_TO_PORT = 0;
	private final static int DRIVER_ON_PORT = 0;
	private final static int FIRST_SLAVE_PORT = 1;

	private String transformClassName;
	private String transformSource = null;
	private String transformURL = null;
	private String locale = null;
	private boolean caseSensitive;
	private String charset = null;

	private RecordTransform transformation = null;

	private DataRecord[] inRecords;
	private DataRecord[] outRecords;
	
	private Properties transformationParameters;
	
//	private static Log logger = LogFactory.getLog(MergeJoin.class);
	
	static Log logger = LogFactory.getLog(MergeJoin.class);
//	private boolean oldJoinKey = false;

	private OrderedKey[][] joiners;
	private Join join;

	private boolean slaveDuplicates;
//	private boolean slaveOverriden;

	private int inputCnt;
	private int slaveCnt;
	
	private RecordOrderedKey driverKey;
	private RecordOrderedKey[] slaveKeys;

	IInputReader[] reader;
	boolean anyInputEmpty;
	IInputReader minReader;
	boolean[] minIndicator;
	int minCnt;

	OutputPort outPort;
	private String joinKeys;
	
	@Deprecated
	private boolean ascendingInputs = true;

	private String errorActionsString;
	private Map<Integer, ErrorAction> errorActions = new HashMap<Integer, ErrorAction>();
	private String errorLogURL;
	private FileWriter errorLog;

	/**
	 *  Constructor for the SortedJoin object
	 *
	 * @param id		id of component
	 * @param joiners	parsed join string (first element contains driver key list, following elements contain slave key lists)
	 * @param transform
	 * @param transformClass  class (name) to be used for transforming data
	 * @param join join type
	 * @param slaveDuplicates enables/disables duplicate slaves
	 * @param ascendingInputs switch for inputs ordering (true=ascending, false=descending) all inputs must be ordered in the same way
	 */
	public MergeJoin(String id, String joinKey, String transform,
			String transformClass, String transformURL, Join join, boolean slaveDuplicates, boolean ascendingInputs) {
		super(id);

		this.transformSource = transform;
		this.joinKeys = joinKey;
		this.transformClassName = transformClass;
		this.transformURL = transformURL;
		this.join = join;
		this.slaveDuplicates = slaveDuplicates;
		this.ascendingInputs = ascendingInputs;
	}

	public MergeJoin(String id, String joinKey, RecordTransform transform, Join join, boolean slaveDuplicates, boolean ascendingInputs){
		this(id, joinKey, null, null, null, join, slaveDuplicates, ascendingInputs);
		this.transformation = transform;
	}
	
	/**
	 * Replace minimal record runs with the following ones. Change min indicator array
	 * to reflect new set of runs.
	 * @return Number of minimal runs, zero when no more data are available.
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private int loadNext() throws InterruptedException, IOException {
	    // assume that each input contains at least one more record
	    anyInputEmpty = false;

	    minCnt = 0;
		int minIdx = 0;
		for (int i = 0; i < inputCnt; i++) {
			if (minIndicator[i]) {
				reader[i].loadNextRun();

				// check inputs ordering 
				if (reader[i].getOrdering() == InputOrdering.UNSORTED) 		
					throw new IllegalStateException("Data input "+i+" is not sorted in ascending order. "+reader[i].getInfo());
				if (reader[i].getOrdering() == InputOrdering.DESCENDING)
					throw new IllegalStateException("input " + i + " has wrong ordering; change ordering on field or ordering of input "+i);
			}

			// change the flag to true if the current reader reached the EOF
			anyInputEmpty |= (reader[i].getSample() == null);
		}// for

		for (int i = 0; i < inputCnt; i++) {
			
			int comparison = 0;
			if (minIdx != i){ // compare data from different input ports
				comparison = reader[minIdx].compare(reader[i]);
			}
			
			switch (comparison) {
			case -1: // current is greater than minimal
				minIndicator[i] = false;
				break;
			case 0: // current is equal to minimal
				minCnt++;
				minIndicator[i] = true;
				break;
			case 1: // current is smaller than minimal
				minCnt = 1;
				minIdx = i;
				minIndicator[i] = true;
				break; // all previous indicators will be reset later
			}
		} // for
		for (int i = minIdx - 1; i >= 0; i--) {
			minIndicator[i] = false;
		}
		minReader = reader[minIdx];
		if (reader[minIdx].getSample() == null) {	// no more data
			minCnt = 0;
		}
		return minCnt;
	}

	private void handleException(RecordTransform transform, int transformResult) throws TransformException, IOException{
		ErrorAction action = errorActions.get(transformResult);
		if (action == null) {
			action = errorActions.get(Integer.MIN_VALUE);
			if (action == null) {
				action = ErrorAction.DEFAULT_ERROR_ACTION;
			}
		}
		String message = "Transformation for master record:\n " + inRecords[0] + "finished with code: "	+ transformResult + 
			". Error message: " + transformation.getMessage();
		if (action == ErrorAction.CONTINUE) {
			if (errorLog != null){
				errorLog.write(driverKey.getKeyString(inRecords[0]));
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

	/**
	 * Transform all tuples created from minimal input runs.
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws TransformException 
	 */
	private boolean flushMin() throws IOException, InterruptedException, TransformException {
		// create initial combination
		for (int i = 0; i < inputCnt; i++) {
				inRecords[i] = minIndicator[i] ? reader[i].next() : NullRecord.NULL_RECORD;
		}
		while (true) {
			outRecords[0].reset();

			int transformResult = -1;

			try {
				transformResult = transformation.transform(inRecords, outRecords);
			} catch (Exception exception) {
				transformResult = transformation.transformOnError(exception, inRecords, outRecords);
			}

			if (transformResult >= 0) {
				outPort.writeRecord(outRecords[0]);
			}else{
				handleException(transformation, transformResult);
			}
			// generate next combination
			int chngCnt = 0;
			for (int i = inputCnt - 1; i >= 0; i--) {
				if (!minIndicator[i]) {
					continue;
				}
				chngCnt++;
				inRecords[i] = reader[i].next();
				if (inRecords[i] != null) {	// have new combination
					break;
				}
				if (chngCnt == minCnt) { // no more combinations available
					return true;
				}
				// need rewind
				reader[i].rewindRun();
				inRecords[i] = reader[i].next();	// this is supposed to return non-null value
			}
		}
	}

	@Override
	public Result execute() throws Exception {
	    boolean eofBroadcasted = false;

	    while (loadNext() > 0){
		    // if any input is empty and the inner join is selected, we don't need to read any more data records
		    if (anyInputEmpty && join == Join.INNER) {
		        broadcastEOF();
		        eofBroadcasted = true;

		        break;
		    }

		    if (join == Join.INNER && minCnt != inputCnt) { // not all records for current key available
				continue;
			}
			if (join == Join.LEFT_OUTER && !minIndicator[0]) {	// driver record for current key not available
				continue;
			}
			if (!flushMin()) {
				String resultMsg = transformation.getMessage();
				broadcastEOF();
				throw new JetelException(resultMsg);
			}
		}

	    while (areData()){//send incomplete combination or wait for all data
			while (loadNext() > 0) {
				if ((join == Join.LEFT_OUTER && reader[DRIVER_ON_PORT].hasData()) || join == Join.FULL_OUTER){
					if (!flushMin()) {
						String resultMsg = transformation.getMessage();
						broadcastEOF();
						throw new JetelException(resultMsg);
					}
				}
			}
		}

	    if (!eofBroadcasted) {
	        broadcastEOF();
	    }

	    if (errorLog != null){
			errorLog.flush();
		}

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

    @Override
    public void postExecute() throws ComponentNotReadyException {
    	super.postExecute();
        transformation.postExecute();
        transformation.finished();
        
    	try {
    	    if (errorLog != null){
    			errorLog.close();
    		}
    	}
    	catch (Exception e) {
    		throw new ComponentNotReadyException(e);
    	}
    }
	
	/**
	 * Checks if there are data on any writer avaible. Sets minIndicator to true for readers which have data
	 * 
	 * @return true if any of the readers have data, false in other case
	 */
	private boolean areData() {
		boolean areData = false;
		for (int i = 0; i < reader.length; i++) {
			if (reader[i].hasData()) {
				areData = true;
				minIndicator[i] = true;
			}else{
				minIndicator[i] = false;
			}
		}
		return areData;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		inputCnt = inPorts.size();
		slaveCnt = inputCnt - 1;
		if (joiners == null || joiners[0] == null) {
			List<DataRecordMetadata> inMetadata = getInMetadata();
			OrderedKey[][] tmp = JoinKeyUtils.parseMergeJoinOrderedKey(joinKeys, inMetadata);
			if (joiners == null) {
				joiners = tmp;
			}else{//slave override key was set by setSlaveOverrideKey(String[]) method
				joiners[0] = tmp[0];
			}
			if (joiners.length < inputCnt) {
				logger.warn("Join keys aren't specified for all slave inputs - deducing missing keys");
				OrderedKey[][] replJoiners = new OrderedKey[inputCnt][];
    			for (int i = 0; i < joiners.length; i++) {
    				replJoiners[i] = joiners[i];
    			}
    			// use driver key list for all missing slave key specifications
    			for (int i = joiners.length; i < inputCnt; i++) {
    				replJoiners[i] = joiners[0];
    			}
    			joiners = replJoiners;
			}
		}
		driverKey = buildRecordKey(joiners[0], getInputPort(DRIVER_ON_PORT).getMetadata());
		driverKey.init();
		slaveKeys = new RecordOrderedKey[slaveCnt];
		for (int idx = 0; idx < slaveCnt; idx++) {
			slaveKeys[idx] = buildRecordKey(joiners[1 + idx], getInputPort(FIRST_SLAVE_PORT + idx).getMetadata());
			slaveKeys[idx].init();
		}		
		reader = new IInputReader[inputCnt];
		reader[0] = new DriverReader(getInputPort(DRIVER_ON_PORT), driverKey);
		if (slaveDuplicates) {
			for (int i = 0; i < slaveCnt; i++) {
				reader[i + 1] = new SlaveReaderDup(getInputPort(FIRST_SLAVE_PORT + i), slaveKeys[i]);
			}
		} else {
			for (int i = 0; i < slaveCnt; i++) {
				reader[i + 1] = new SlaveReader(getInputPort(FIRST_SLAVE_PORT + i), slaveKeys[i], true);
			}			
		}
		minReader = reader[0];
		minIndicator = new boolean[inputCnt];
		for (int i = 0; i < inputCnt; i++) {
			minIndicator[i] = true;
		}
		inRecords = new DataRecord[inputCnt];
		outRecords = new DataRecord[]{DataRecordFactory.newRecord(getOutputPort(WRITE_TO_PORT).getMetadata())};
		outRecords[0].init();
		outRecords[0].reset();
		outPort = getOutputPort(WRITE_TO_PORT);
		// init transformation
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] {
				getOutputPort(WRITE_TO_PORT).getMetadata()};
		DataRecordMetadata[] inMetadata = getInMetadataArray();
		
		if (transformation == null) {
			transformation = getTransformFactory(inMetadata, outMetadata).createTransform();
        }
        
		// init transformation
        if (!transformation.init(transformationParameters, inMetadata, outMetadata)) {
            throw new ComponentNotReadyException("Error when initializing tranformation function.");
        }

        errorActions = ErrorAction.createMap(errorActionsString);
	}
	
	private TransformFactory<RecordTransform> getTransformFactory(DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata) {
    	TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
    	transformFactory.setTransform(transformSource);
    	transformFactory.setTransformClass(transformClassName);
    	transformFactory.setTransformUrl(transformURL);
    	transformFactory.setCharset(charset);
    	transformFactory.setComponent(this);
    	transformFactory.setInMetadata(inMetadata);
    	transformFactory.setOutMetadata(outMetadata);
    	return transformFactory;
	}

	/**
	 * Constructs a RecordComparator based on particular metadata and settings
	 * 
	 * @param metaData
	 * @return
	 * @throws ComponentNotReadyException 
	 */
	private RecordOrderedKey buildRecordKey(OrderedKey joiners[], DataRecordMetadata metaData) throws ComponentNotReadyException {
		boolean[] ordering = new boolean[joiners.length];
		Arrays.fill(ordering, ascendingInputs);

		String metadataLocale = metaData.getLocaleStr();
		int[] fields = new int[joiners.length];
		boolean[] aOrdering = new boolean[joiners.length];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = metaData.getFieldPosition(joiners[i].getKeyName());
			if (joiners[i].getOrdering() == OrderEnum.ASC) {
				aOrdering[i] = true;
			} else if (joiners[i].getOrdering() == null) {	// old fashion - field name without ordering
				joiners[i].setOrdering(ascendingInputs ? OrderEnum.ASC : OrderEnum.DESC);
				aOrdering[i] = ascendingInputs;
			} else if (joiners[i].getOrdering() != OrderEnum.DESC) {
				throw new ComponentNotReadyException("Wrong order definition in join key: " + joiners[i].getOrdering());
			}
			if (metadataLocale == null)	metadataLocale = metaData.getField(fields[i]).getLocaleStr();
		}
		
		if (metadataLocale != null || locale != null) {
			metadataLocale = metadataLocale != null ? metadataLocale : locale;
			RuleBasedCollator col = (RuleBasedCollator)Collator.getInstance(MiscUtils.createLocale(metadataLocale));
			col.setStrength(caseSensitive ? Collator.TERTIARY : Collator.SECONDARY);
			col.setDecomposition(Collator.CANONICAL_DECOMPOSITION);
			RecordOrderedKey recordKey = new RecordOrderedKey(fields, aOrdering, metaData);
			recordKey.setCollator(col);
			return recordKey;
		} else {
			return new RecordOrderedKey(fields, aOrdering, metaData);
		}
	}


    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
        transformation.preExecute();

    	if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	}
    	else {
    		reader[0].reset();
    		for (int i =0; i < slaveCnt; i++)
    			reader[i+1].reset(); 
    		transformation.reset();
    		for (int i = 0; i < inputCnt; i++) {
    			minIndicator[i] = true;
    		}

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
		super.free();
		if (reader != null) {
			if (reader[0] != null) {
				reader[0].free();
			}
			for (int i = 0; i < slaveCnt; i++) {
				if (reader[i+1] != null) {
					reader[i + 1].free();
				}
			}
		} 
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
	@Override
	@SuppressWarnings("unchecked")
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		
		if (transformClassName != null) {
			xmlElement.setAttribute(XML_TRANSFORMCLASS_ATTRIBUTE, transformClassName);
		} 
		
		if (transformSource!=null){
			xmlElement.setAttribute(XML_TRANSFORM_ATTRIBUTE,transformSource);
		}
		
		if (transformURL != null) {
			xmlElement.setAttribute(XML_TRANSFORMURL_ATTRIBUTE, transformURL);
		}
		
		if (charset != null){
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charset);
		}
		xmlElement.setAttribute(XML_JOINKEY_ATTRIBUTE, JoinKeyUtils.toString(joiners));		
		
		xmlElement.setAttribute(XML_JOINTYPE_ATTRIBUTE,
				join == Join.FULL_OUTER ? "fullOuter" : join == Join.LEFT_OUTER ? "leftOuter" : "inner");

		xmlElement.setAttribute(XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE, String.valueOf(slaveDuplicates));
		if (errorActionsString != null){
			xmlElement.setAttribute(XML_ERROR_ACTIONS_ATTRIBUTE, errorActionsString);
		}
		
		if (errorLogURL != null){
			xmlElement.setAttribute(XML_ERROR_LOG_ATTRIBUTE, errorLogURL);
		}

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
	 * @throws AttributeNotFoundException 
	 * @since           May 21, 2002
	 */
	   public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		MergeJoin join;

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

		// legacy attributes handling {
		if (!xattribs.exists(XML_JOINTYPE_ATTRIBUTE) && xattribs.exists(XML_LEFTOUTERJOIN_ATTRIBUTE)) {
			joinType = xattribs.getBoolean(XML_LEFTOUTERJOIN_ATTRIBUTE) ? Join.LEFT_OUTER : Join.INNER;
		}
		if (!xattribs.exists(XML_JOINTYPE_ATTRIBUTE) && xattribs.exists(XML_FULLOUTERJOIN_ATTRIBUTE)) {
			joinType = xattribs.getBoolean(XML_FULLOUTERJOIN_ATTRIBUTE) ? Join.FULL_OUTER : Join.INNER;
		}

		join = new MergeJoin(xattribs.getString(XML_ID_ATTRIBUTE),
				xattribs.getString(XML_JOINKEY_ATTRIBUTE),
				xattribs.getStringEx(XML_TRANSFORM_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF), 
				xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
				xattribs.getStringEx(XML_TRANSFORMURL_ATTRIBUTE,null, RefResFlag.SPEC_CHARACTERS_OFF),
				joinType,
				xattribs.getBoolean(XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE, true),
				xattribs.getBoolean(XML_ASCENDING_INPUTS_ATTRIBUTE, true));
		
		if (xattribs.exists(XML_SLAVEOVERRIDEKEY_ATTRIBUTE)) {
			join.setSlaveOverrideKey(xattribs.getString(XML_SLAVEOVERRIDEKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
		}
		if (xattribs.exists(XML_LOCALE_ATTRIBUTE)) {
			join.setLocale(xattribs.getString(XML_LOCALE_ATTRIBUTE));
		}
		if (xattribs.exists(XML_CASE_SENSITIVE_ATTRIBUTE)) {
			join.setCaseSensitive(xattribs.getBoolean(XML_CASE_SENSITIVE_ATTRIBUTE));
		}
		join.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE, null));
		if (xattribs.exists(XML_ERROR_ACTIONS_ATTRIBUTE)){
			join.setErrorActions(xattribs.getString(XML_ERROR_ACTIONS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_ERROR_LOG_ATTRIBUTE)){
			join.setErrorLog(xattribs.getString(XML_ERROR_LOG_ATTRIBUTE));
		}
		join.setTransformationParameters(xattribs.attributes2Properties(
                new String[]{XML_ID_ATTRIBUTE,XML_JOINKEY_ATTRIBUTE,
                		XML_TRANSFORM_ATTRIBUTE,XML_TRANSFORMCLASS_ATTRIBUTE,
                		XML_LEFTOUTERJOIN_ATTRIBUTE,XML_SLAVEOVERRIDEKEY_ATTRIBUTE,
                		XML_FULLOUTERJOIN_ATTRIBUTE,XML_JOINTYPE_ATTRIBUTE}));			
		return join;
	}

	public void setErrorLog(String errorLog) {
		this.errorLogURL = errorLog;
	}

	public void setErrorActions(String string) {
		this.errorActionsString = string;		
	}

	public void setSlaveOverrideKey(String[] slaveOverrideKey) {
		if (joiners == null) {
			joiners = new OrderedKey[2][];
		}else{
			if (joiners[0] != null && joiners[0].length != slaveOverrideKey.length) {
				throw new IllegalArgumentException("Number of fields in master key doesn't match to number of fields in slave key.");
			}
		}
		OrderedKey[] slaveKeys = new OrderedKey[slaveOverrideKey.length];
		for (int i=0; i<slaveKeys.length; i++) {
			slaveKeys[i] = new OrderedKey(slaveOverrideKey[i], ascendingInputs ? OrderEnum.ASC : OrderEnum.DESC);
		}
		joiners[1] = slaveKeys;
	}
	
	/**  Description of the Method */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		if(!checkInputPorts(status, 2, Integer.MAX_VALUE)
				|| !checkOutputPorts(status, 1, 1)) {
			return status;
		}
		
		if (charset != null && !Charset.isSupported(charset)) {
			status.add(new ConfigurationProblem(
					"Charset "+charset+" not supported!", 
					ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
		}
		
		try {
			inputCnt = inPorts.size();
			slaveCnt = inputCnt - 1;
			
			if (joiners == null || joiners[0] == null) {
				List<DataRecordMetadata> inMetadata = getInMetadata();
				OrderedKey[][] tmp = JoinKeyUtils.parseMergeJoinOrderedKey(joinKeys, inMetadata);
				if (joiners == null) {
					joiners = tmp;
				}else{//slave ovveride key was set by setSlaveOverrideKey(String[]) method
					joiners[0] = tmp[0];
				}
				if (joiners.length < inputCnt) {
					logger.warn("Join keys aren't specified for all slave inputs - deducing missing keys");
					OrderedKey[][] replJoiners = new OrderedKey[inputCnt][];
					for (int i = 0; i < joiners.length; i++) {
						replJoiners[i] = joiners[i];
					}
					// use driver key list for all missing slave key specifications
					for (int i = joiners.length; i < inputCnt; i++) {
						replJoiners[i] = joiners[0];
					}
					joiners = replJoiners;
				}
			}
			driverKey = buildRecordKey(joiners[0], getInputPort(DRIVER_ON_PORT).getMetadata());
			slaveKeys = new RecordOrderedKey[slaveCnt];
			for (int idx = 0; idx < slaveCnt; idx++) {
				slaveKeys[idx] = buildRecordKey(joiners[1 + idx], getInputPort(FIRST_SLAVE_PORT + idx).getMetadata());
				RecordOrderedKey.checkKeys(driverKey, XML_JOINKEY_ATTRIBUTE, slaveKeys[idx], 
						XML_JOINKEY_ATTRIBUTE, status, this);
			}
			reader = new IInputReader[inputCnt];
			reader[0] = new DriverReader(getInputPort(DRIVER_ON_PORT), driverKey);
			if (slaveDuplicates) {
				for (int i = 0; i < slaveCnt; i++) {
					reader[i + 1] = new SlaveReaderDup(getInputPort(FIRST_SLAVE_PORT + i), slaveKeys[i]);
				}
			} else {
				for (int i = 0; i < slaveCnt; i++) {
					reader[i + 1] = new SlaveReader(getInputPort(FIRST_SLAVE_PORT + i), slaveKeys[i], true);
				}			
			}
			minReader = reader[0];
			minIndicator = new boolean[inputCnt];
			for (int i = 0; i < inputCnt; i++) {
				minIndicator[i] = true;
			}
			
			if (errorActionsString != null) {
				ErrorAction.checkActions(errorActionsString);
			}
			
			if (errorLog != null){
 				FileUtils.canWrite(getGraph().getRuntimeContext().getContextURL(), errorLogURL);
			}        	
	            	
		} catch (ComponentNotReadyException e) {
			ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL);
			if(!StringUtils.isEmpty(e.getAttributeName())) {
				problem.setAttributeName(e.getAttributeName());
			}
			status.add(problem);
		}
		
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] {getOutputPort(WRITE_TO_PORT).getMetadata()};

        //check transformation
		if (transformation == null) {
			getTransformFactory(getInMetadataArray(), outMetadata).checkConfig(status);
		}

		return status;
	}
	
	@Override
	public String getType(){
		return COMPONENT_TYPE;
	}
	
	public String getCharset() {
		return charset;
	}

	public void setLocale(String locale) {
		this.locale = locale;
	}

	public void setCaseSensitive(boolean caseSensitive) {
		this.caseSensitive = caseSensitive;
	}
	
	public void setCharset(String charset) {
		this.charset = charset;
	}

}
