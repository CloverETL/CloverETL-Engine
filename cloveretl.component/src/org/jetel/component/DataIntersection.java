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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.data.reader.DriverReader;
import org.jetel.data.reader.IInputReader;
import org.jetel.data.reader.IInputReader.InputOrdering;
import org.jetel.data.reader.SlaveReader;
import org.jetel.data.reader.SlaveReaderDup;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;
import org.jetel.util.joinKey.JoinKeyUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Data Intersection Component</h3> <!-- Finds intersection of
 * flow A (in-port0) and B (in-port1) based on specified key. Both inputs
 * must be sorted according to specified key. DataRecords only in flow A
 * are sent out through out-port0. DataRecords in both A & B are sent to
 * specified transformation function and the result is sent through out-port1.
 * DataRecords present only in flow B are sent through out-port2.-->
 * 
 *  <table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>DataIntersection</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *      <td>
 * Finds intersection of data flows (sets) <b>A (in-port0)</b> and <b>B (in-port1)</b> 
 * based on specified key. Both inputs <u><b>must be sorted</b></u> according to specified key. DataRecords only in flow <b>A</b>
 * are sent out through <b>out-port[0]</b>.
 * DataRecords in both <b>A&amp;B</b> are sent to specified <b>transformation</b> function and the result is 
 * sent through <b>out-port[1]</b>.
 * DataRecords present only in flow <b>B</b> are sent through <b>out-port[2]</b>.<br>
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0] - records from set A - <i>sorted according to specified key</i><br>
 *	  [1] - records from set B - <i>sorted according to specified key</i><br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - records only in set A<br>
 * 		  [1] - records in set A&amp;B<br>
 * 		  [2] - records only in set B	
 *      </td></tr>
 *    <tr><td><h4><i>Comment:</i> </h4>
 *      </td>
 *      <td></td>
 *    </tr>
 *  </table>
 *  <br>
 *  <table border="1">
 *    <th>XML attributes:</th>
 *    <tr><td><b>type</b></td><td>"DATA_INTERSECTION"</td></tr>
 *    <tr><td><b>id</b></td><td>component identification</td></tr>
 *    <tr><td><b>joinKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *    <tr><td><b>slaveOverrideKey</b><br><i>optional</i></td><td>can be used to specify different key field names for records on slave input; field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *    <tr><td><b>libraryPath</b><br><i>optional</i></td><td>name of Java library file (.jar,.zip,...) where
 *      to search for class to be used for transforming data specified in <tt>transformClass<tt> parameter.</td></tr>
 *    <tr><td><b>transformClass</b></td><td>name of the class to be used for transforming data</td></tr>
 *    <tr><td><b>transform</b></td><td>contains definition of transformationas java source, in internal clover format or in Transformation Language </td></tr>
 *  <tr><td><b>transformURL</b></td><td>path to the file with transformation code</td></tr>
 *  <tr><td><b>charset</b><i>optional</i></td><td>encoding of extern source</td></tr>
 *  <tr><td><b>equalNULL</b><br><i>optional</i></td><td>specifies whether two fields containing NULL values are considered equal. Default is TRUE.</td></tr>
 *  <tr><td><b>keyDuplicates</b><br><i>optional</i></td><td>true/false - allows records 
 *  to have duplicate keys. False - multiple duplicate records are discarded - only the 
 *  last one is sent to transformation. Default is TRUE.</td></tr>
 *  <tr><td><b>errorActions </b><i>optional</i></td><td>defines if graph is to stop, when transformation returns negative value.
 *  Available actions are: STOP or CONTINUE. For CONTINUE action, error message is logged to console or file (if errorLog attribute
 *  is specified) and for STOP there is thrown TransformExceptions and graph execution is stopped. <br>
 *  Error action can be set for each negative value (value1=action1;value2=action2;...) or for all values the same action (STOP 
 *  or CONTINUE). It is possible to define error actions for some negative values and for all other values (MIN_INT=myAction).
 *  Default value is <i>-1=CONTINUE;MIN_INT=STOP</i></td></tr>
 *  <tr><td><b>errorLog</b><br><i>optional</i></td><td>path to the error log file. Each error (after which graph continues) is logged in 
 *  following way: keyFieldsValue;errorCode;errorMessage;semiResult - fields are delimited by Defaults.Component.KEY_FIELDS_DELIMITER.</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="INTERSEC" type="DATA_INTERSECT" joinKey="CustomerID" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *<pre>&lt;Node id="INTERSEC" type="DATA_INTERSECT" joinKey="EmployeeID"&gt;
 *&lt;attr name="javaSource"&gt;
 *import org.jetel.component.DataRecordTransform;
 *import org.jetel.data.*;
 * 
 *public class intersectionTest extends DataRecordTransform {
 *
 *    public int transform(DataRecord[] source, DataRecord[] target) {
 *        target[0].getField(0).setValue(source[0].getField(0).getValue());
 *        target[0].getField(1).setValue(source[0].getField(1).getValue());
 *        target[0].getField(2).setValue(source[1].getField(2).getValue());
 *
 *        return ALL;
 *    }
 *
 *}
 *&lt;/attr&gt;
 *&lt;/Node&gt;</pre>
 * @author      dpavlis
 * @since       April 29, 2005
 * @created     29. April 2005
 */
public class DataIntersection extends Node {

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DATA_INTERSECTION";

	private static final String XML_SLAVEOVERRIDEKEY_ATTRIBUTE = "slaveOverrideKey";
	private static final String XML_JOINKEY_ATTRIBUTE = "joinKey";
	private static final String XML_TRANSFORMCLASS_ATTRIBUTE = "transformClass";
    private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_TRANSFORMURL_ATTRIBUTE = "transformURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
    private static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";
	private static final String XML_KEY_DUPLICATES_ATTRIBUTE ="keyDuplicates";
	private static final String XML_ERROR_ACTIONS_ATTRIBUTE = "errorActions";
    private static final String XML_ERROR_LOG_ATTRIBUTE = "errorLog";

	private final static int WRITE_TO_PORT_A = 0;
	private final static int WRITE_TO_PORT_A_B = 1;
	private final static int WRITE_TO_PORT_B = 2;
	private final static int DRIVER_ON_PORT = 0;
	private final static int SLAVE_ON_PORT = 1;
	
	private final static int A_INDEX = 0;
	private final static int B_INDEX = 1;

	private String transformClassName;
    private String transformSource = null;
	private String transformURL = null;
	private String charset = null;

	private String errorActionsString;
	private Map<Integer, ErrorAction> errorActions = new HashMap<Integer, ErrorAction>();
	private String errorLogURL;
	private FileWriter errorLog;

	private RecordTransform transformation = null;

	private String[] joinKeys;
	private String[] slaveOverrideKeys = null;
	private boolean keyDuplicates;

	private RecordKey recordKeys[];
    private boolean equalNULLs = true;

	// for passing data records into transform function
	private DataRecord[] inRecords = new DataRecord[2];
	private DataRecord[] outRecords=new DataRecord[2];

	private Properties transformationParameters;

	private DriverReader driverReader;

	private IInputReader slaveReader;

	private InputOrdering driverReaderOrdering;
	
	private InputOrdering slaveReaderOrdering;
	
	private DataRecord tmp;

	private String joinKey;
	
	static Log logger = LogFactory.getLog(DataIntersection.class);
	
	/**
	 *  Constructor for the SortedJoin object
	 *
	 * @param  id              id of component
	 * @param  joinKeys        field names composing key
	 * @param  transformClass  class (name) to be used for transforming data
	 */
	public DataIntersection(String id, String[] joinKeys, String transform,
			String transformClass, String transformURL) {
		super(id);
		this.joinKeys = joinKeys;
		this.transformClassName = transformClass;
		this.transformSource = transform;
		this.transformURL = transformURL;
	}

	public DataIntersection(String id, String[] joinKeys, DataRecordTransform transform) {
		this(id, joinKeys, null, null, null);
		this.transformation = transform;
	}

	public DataIntersection(String id, String joinKey, String transform, String transformClass,
			String transformURL){
		super(id);
		this.joinKey = joinKey;
		this.transformClassName = transformClass;
		this.transformSource = transform;
		this.transformURL = transformURL;
	}
	
	public DataIntersection(String id, String joinKey, DataRecordTransform transform){
		this(id, joinKey, null, null, null);
		this.transformation = transform;
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
//	private int getCorrespondingRecord(DataRecord driver, DataRecord slave, InputPort slavePort, RecordKey key[]){
//
//		if (slave != null) {
//			return key[DRIVER_ON_PORT].compare(key[SLAVE_ON_PORT], driver, slave);
//		}
//		return -1;
//	}


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
	 * @throws TransformException 
	 */
	private final boolean flushCombinations(DriverReader driver, IInputReader slave, 
			DataRecord out, OutputPort port)
	throws IOException, InterruptedException, TransformException {
	    outRecords[0]= out;
		if (keyDuplicates) {
			while ((inRecords[0] = driver.next()) != null) {
				while ((inRecords[1] = slave.next()) != null) {
					int transformResult = -1;

					try {
						transformResult = transformation.transform(inRecords, outRecords);
					} catch (Exception exception) {
						transformResult = transformation.transformOnError(exception, inRecords, outRecords);
					}

					if (transformResult < 0) {
						handleException(transformation, transformResult);
						return false;
					}

					port.writeRecord(out);
				}
				slaveReader.rewindRun();
			}
		}else{
			inRecords[0] = driver.next();
			inRecords[1] = slave.next();

			int transformResult = -1;

			try {
				transformResult = transformation.transform(inRecords, outRecords);
			} catch (Exception exception) {
				transformResult = transformation.transformOnError(exception, inRecords, outRecords);
			}

			if (transformResult < 0) {
				handleException(transformation, transformResult);
				return false;
			}

			port.writeRecord(out);
		}
		return true;
}

	private void handleException(RecordTransform transform, int transformResult) throws TransformException, IOException{
		ErrorAction action = errorActions.get(transformResult);
		if (action == null) {
			action = errorActions.get(Integer.MIN_VALUE);
			if (action == null) {
				action = ErrorAction.DEFAULT_ERROR_ACTION;
			}
		}
		String message = "Transformation for records:\n " + inRecords[0] + "and:\n"
			+ inRecords[1] + "finished with code: "	+ transformResult + ". Error message: " + transformation.getMessage();
		if (action == ErrorAction.CONTINUE) {
			if (errorLog != null){
				errorLog.write(recordKeys[DRIVER_ON_PORT].getKeyString(inRecords[DRIVER_ON_PORT]));
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
	 *  If there is no corresponding slave record and is defined left outer join, then
	 *  output driver only.
	 *
	 * @param  reader                    Description of the Parameter
	 * @param  port                      Description of the Parameter
	 * @return                           Description of the Return Value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 */
	private final boolean flush(IInputReader reader, OutputPort port) 
			throws IOException,InterruptedException{
		if (keyDuplicates) {
			while ((tmp = reader.next()) != null) {
				port.writeRecord(tmp);
			}
		}else if ((tmp = reader.next()) != null){
			port.writeRecord(tmp);
		}
		return true;
	}

	/**
	 *  Description of the Method
	 *
	 * @param  metadata  Description of the Parameter
	 * @param  count     Description of the Parameter
	 * @return           Description of the Return Value
	 */
//	private DataRecord[] allocateRecords(DataRecordMetadata metadata, int count) {
//		DataRecord[] data = new DataRecord[count];
//
//		for (int i = 0; i < count; i++) {
//			data[i] = new DataRecord(metadata);
//			data[i].init();
//		}
//		return data;
//	}
	

    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
		transformation.preExecute();
		
		driverReaderOrdering = InputOrdering.UNDEFINED;
		slaveReaderOrdering = InputOrdering.UNDEFINED;
		
    	if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	}
    	else {
    		transformation.reset();
    		driverReader.reset();
    		slaveReader.reset();
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
	public Result execute() throws Exception {

		OutputPort outPortA = getOutputPort(WRITE_TO_PORT_A);
		OutputPort outPortB = getOutputPort(WRITE_TO_PORT_B);
		OutputPort outPortAB = getOutputPort(WRITE_TO_PORT_A_B);
		
		// initialize output record
		DataRecord outRecord = DataRecordFactory.newRecord(outPortAB.getMetadata());
		outRecord.init();
		driverReader.loadNextRun();
		slaveReader.loadNextRun();
		driverReaderOrdering = driverReader.getOrdering();
		slaveReaderOrdering = slaveReader.getOrdering();
		
		// main processing loop
		do {
			switch (driverReader.compare(slaveReader)) {
			case -1:
				// driver lower
				// no corresponding slave
				flush(driverReader, outPortA);
				driverReader.loadNextRun();
				break;
			case 0:
				// match - perform transformation
				outRecord.reset();
				flushCombinations(driverReader, slaveReader, outRecord, outPortAB);
				driverReader.loadNextRun();
				slaveReader.loadNextRun();
				break;
			case 1:
				// slave lower - no corresponding master
				flush(slaveReader, outPortB);
				slaveReader.loadNextRun();
				break;
			}
			//all input data has to be in ascending order
			if (!isDriverStreamAscending()) {
				throw new RuntimeException("Data input 0 is not sorted in ascending order. "+driverReader.getInfo());
			}
			if (!isSlaveStreamAscending()) {
				throw new RuntimeException("Data input 1 is not sorted in ascending order. "+slaveReader.getInfo());
			}
		}while (runIt && (driverReader.hasData() || slaveReader.hasData()));
		 
		if (!runIt) {
			return Result.ABORTED;
		}

		// flush remaining driver records
		flush(driverReader, outPortA) ;

		// flush remaining slave records
		flush(slaveReader, outPortB) ; 

		if (errorLog != null){
			errorLog.flush();
		}

		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
    }

	/**
	 * This method checks whether the master input records are in ascending order.
	 * Method driverReader.getOrdering() is used for this purpose. This method returns 
	 * UNDEFINED order in the start of processing of input records. Then is changed to
	 * a specific value - ASCENDING (everything is correct) and DESCENDING (invalid order).
	 * The order flag can be changed back to UNDEFINED value that means a change of ordering
	 * happens and it is reported as invalid ordering.
	 */
	private boolean isDriverStreamAscending() {
		if (driverReader.getOrdering() != InputOrdering.ASCENDING) {
			if (driverReaderOrdering != InputOrdering.UNDEFINED || driverReader.getOrdering() != InputOrdering.UNDEFINED) {
				return false;
			}
		} else {
			driverReaderOrdering = InputOrdering.ASCENDING;
		}
		return true;
	}

	/**
	 * This method checks whether the master input records are in ascending order.
	 * Method driverReader.getOrdering() is used for this purpose. This method returns 
	 * UNDEFINED order in the start of processing of input records. Then is changed to
	 * a specific value - ASCENDING (everything is correct) and DESCENDING (invalid order).
	 * The order flag can be changed back to UNDEFINED value that means a change of ordering
	 * happens and it is reported as invalid ordering.
	 */
	private boolean isSlaveStreamAscending() {
		if (slaveReader.getOrdering() != InputOrdering.ASCENDING) {
			if (slaveReaderOrdering != InputOrdering.UNDEFINED || slaveReader.getOrdering() != InputOrdering.UNDEFINED) {
				return false;
			}
		} else {
			slaveReaderOrdering = InputOrdering.ASCENDING;
		}
		return true;
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


	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}

	@Override
	public synchronized void free() {
		super.free();
		if (driverReader != null) {
			driverReader.free();
		} if (slaveReader != null) {
			slaveReader.free();
		}
	}

	/**
	 * Description of the Method
	 * 
	 * @exception ComponentNotReadyException
	 *                Description of the Exception
	 * @since April 4, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		InputPort driverPort = getInputPort(DRIVER_ON_PORT);
		InputPort slavePort = getInputPort(SLAVE_ON_PORT);

		if (joinKeys == null) {
			String[][][] tmp = JoinKeyUtils.parseHashJoinKey(joinKey, getInMetadata());
			joinKeys = tmp[A_INDEX][0];
			if (slaveOverrideKeys == null) {
				slaveOverrideKeys = tmp[B_INDEX][0];
			}
		}
		recordKeys = new RecordKey[2];
		recordKeys[0] = new RecordKey(joinKeys, driverPort.getMetadata());
		recordKeys[1] = new RecordKey(slaveOverrideKeys, slavePort.getMetadata());
		recordKeys[0].init();
		recordKeys[1].init();
		//specify whether two fields with NULL value indicator set
        // are considered equal
        recordKeys[0].setEqualNULLs(equalNULLs);
        recordKeys[1].setEqualNULLs(equalNULLs);

        // init transformation
		//create instance of record transformation
        if (transformation == null) {
			transformation = getTransformFactory().createTransform();
        }
		// init transformation
        if (!transformation.init(transformationParameters, getInMetadataArray(), getTransformOutMetadata())) {
            throw new ComponentNotReadyException("Error when initializing tranformation function.");
        }

        errorActions = ErrorAction.createMap(errorActionsString);
		driverReader = new DriverReader(driverPort, recordKeys[DRIVER_ON_PORT]);
		slaveReader = keyDuplicates ? new SlaveReaderDup(slavePort, recordKeys[SLAVE_ON_PORT]) :
			new SlaveReader(slavePort, recordKeys[SLAVE_ON_PORT], false);
	}

	private TransformFactory<RecordTransform> getTransformFactory() {
    	TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
    	transformFactory.setTransform(transformSource);
    	transformFactory.setTransformClass(transformClassName);
    	transformFactory.setTransformUrl(transformURL);
    	transformFactory.setCharset(charset);
    	transformFactory.setComponent(this);
    	transformFactory.setInMetadata(getInMetadata());
    	transformFactory.setOutMetadata(getTransformOutMetadata());
    	return transformFactory;
	}

	private DataRecordMetadata[] getTransformOutMetadata() {
        return new DataRecordMetadata[] { getOutputPort(WRITE_TO_PORT_A_B).getMetadata() };
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
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @throws AttributeNotFoundException 
	 * @since           May 21, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		DataIntersection intersection;

		intersection = new DataIntersection(
                xattribs.getString(XML_ID_ATTRIBUTE),
                xattribs.getString(XML_JOINKEY_ATTRIBUTE),
                xattribs.getStringEx(XML_TRANSFORM_ATTRIBUTE, null ,RefResFlag.SPEC_CHARACTERS_OFF), 
                xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
                xattribs.getStringEx(XML_TRANSFORMURL_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
    	intersection.setSlaveDuplicates(xattribs.getBoolean(
    			XML_KEY_DUPLICATES_ATTRIBUTE, true));
		intersection.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE, null));
		if (xattribs.exists(XML_SLAVEOVERRIDEKEY_ATTRIBUTE)) {
			intersection.setSlaveOverrideKey(xattribs.getString(XML_SLAVEOVERRIDEKEY_ATTRIBUTE).
					split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));

		}
        if (xattribs.exists(XML_EQUAL_NULL_ATTRIBUTE)){
            intersection.setEqualNULLs(xattribs.getBoolean(XML_EQUAL_NULL_ATTRIBUTE));
        }

		intersection.setTransformationParameters(xattribs.attributes2Properties(
                new String[]{XML_ID_ATTRIBUTE,XML_JOINKEY_ATTRIBUTE,
                		XML_TRANSFORM_ATTRIBUTE,XML_TRANSFORMCLASS_ATTRIBUTE,
                		XML_SLAVEOVERRIDEKEY_ATTRIBUTE,XML_EQUAL_NULL_ATTRIBUTE}));
		if (xattribs.exists(XML_ERROR_ACTIONS_ATTRIBUTE)){
			intersection.setErrorActions(xattribs.getString(XML_ERROR_ACTIONS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_ERROR_LOG_ATTRIBUTE)){
			intersection.setErrorLog(xattribs.getString(XML_ERROR_LOG_ATTRIBUTE));
		}
		
		return intersection;
	}

	public void setErrorLog(String errorLog) {
		this.errorLogURL = errorLog;
	}

	public void setErrorActions(String string) {
		this.errorActionsString = string;		
	}

	/**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);

        //port number checking
        if(!checkInputPorts(status, 2, 2)
        		|| !checkOutputPorts(status, 3, 3)) {
        	return status;
        }
        
        if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }
        
        DataRecordMetadata driverMetadata = getInputPort(DRIVER_ON_PORT).getMetadata();
        DataRecordMetadata slaveMetadata = getInputPort(SLAVE_ON_PORT).getMetadata();
        //compiliance of input and output metadata checking
		checkMetadata(status, driverMetadata, getOutputPort(WRITE_TO_PORT_A).getMetadata());
		checkMetadata(status, slaveMetadata, getOutputPort(WRITE_TO_PORT_B).getMetadata());

		//join key checking
		if (joinKeys == null) {
			try {
				String[][][] tmp = JoinKeyUtils.parseHashJoinKey(joinKey, getInMetadata());
				joinKeys = tmp[A_INDEX][0];
				if (slaveOverrideKeys == null) {
					slaveOverrideKeys = tmp[B_INDEX][0];
				}
			} catch (ComponentNotReadyException e) {
				status.add(e, Severity.WARNING, this, Priority.NORMAL, XML_JOINKEY_ATTRIBUTE);
			}
		}
		recordKeys = new RecordKey[2];
		recordKeys[0] = new RecordKey(joinKeys, driverMetadata);
		recordKeys[1] = new RecordKey(slaveOverrideKeys, slaveMetadata);
		RecordKey.checkKeys(recordKeys[0], XML_JOINKEY_ATTRIBUTE, recordKeys[1], 
				XML_SLAVEOVERRIDEKEY_ATTRIBUTE, status, this);
        
        if (errorActionsString != null){
        	try {
				ErrorAction.checkActions(errorActionsString);
			} catch (ComponentNotReadyException e) {
				status.add(new ConfigurationProblem(e, Severity.ERROR, this, Priority.NORMAL, XML_ERROR_ACTIONS_ATTRIBUTE));
			}
        }
        
        if (errorLog != null){
        	try {
				FileUtils.canWrite(getGraph().getRuntimeContext().getContextURL(), errorLogURL);
			} catch (ComponentNotReadyException e) {
				status.add(new ConfigurationProblem(e, Severity.WARNING, this, Priority.NORMAL, XML_ERROR_LOG_ATTRIBUTE));
			}
        }

        //check transformation
        if (transformation == null) {
        	getTransformFactory().checkConfig(status);
        }

        return status;
    }
	
	@Override
	public String getType(){
		return COMPONENT_TYPE;
	}
    
    public void setEqualNULLs(boolean equal){
        this.equalNULLs=equal;
    }

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public boolean isSlaveDuplicates() {
		return keyDuplicates;
	}

	public void setSlaveDuplicates(boolean slaveDuplicates) {
		this.keyDuplicates = slaveDuplicates;
	}

}

