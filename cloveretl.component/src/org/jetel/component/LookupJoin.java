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
import org.jetel.data.NullRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
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
import org.jetel.lookup.DBLookupTable;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <h3>LookupJoin Component</h3>
 * <!-- Joins records from input port and lookup table based on specified key.
 * The flow on port 0 is the driver, record from lookup table is the slave. For
 * every record from driver flow, corresponding record from slave flow is looked
 * up (if it exists). -->
 * 
 * <table border="1">
 * 
 * <th> Component: </th>
 * <tr>
 * <td>
 * <h4><i>Name:</i> </h4>
 * </td>
 * <td>LookupJoin</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Category:</i> </h4>
 * </td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Description:</i> </h4>
 * </td>
 * <td> Joins records on input port and from lookup table. It expects that on
 * port [0], there is a driver and from lookup table is a slave<br>
 * For each driver record, slave record is looked up in lookup table. Pair of
 * driver and slave records is sent to transformation class.<br>
 * The method <i>transform</i> is called for every pair of driver&amps;slave.<br>
 * It skips driver records for which there is no corresponding slave - unless
 * outer join (leftOuterJoin option) is specified, when only driver record is
 * passed to transform method. </td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Inputs:</i> </h4>
 * </td>
 * <td> [0] - primary records<br>
 * </td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Outputs:</i> </h4>
 * </td>
 * <td> [0] - joined records<br>
 *      [1] - (optional) skipped driver records</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Comment:</i> </h4>
 * </td>
 * <td></td>
 * </tr>
 * </table> <br>
 * <table border="1">
 * <th>XML attributes:</th>
 * <tr>
 * <td><b>type</b></td>
 * <td>"LOOKUP_JOIN"</td>
 * </tr>
 * <tr>
 * <td><b>id</b></td>
 * <td>component identification</td>
 * </tr>
 * <tr>
 * <td><b>joinKey</b></td>
 * <td>field names separated by Defaults.Component.KEY_FIELDS_DELIMITER_REGEX).
 * </td>
 * </tr>
 * <tr>
 * <td><b>transform</b></td>
 * <td>contains definition of transformation as java source, in internal clover format or in Transformation Language</td>
 * <tr>
 * <td><b>transformClass</b><br>
 * <i>optional</i></td>
 * <td>name of the class to be used for transforming joined data<br>
 * If no class name is specified then it is expected that the transformation
 * Java source code is embedded in XML
 *  <tr><td><b>transformURL</b></td><td>path to the file with transformation code for
 *  	 joined records which has conformity smaller then conformity limit</td></tr>
 *  <tr><td><b>charset</b><i>optional</i></td><td>encoding of extern source</td></tr>
 * <tr>
 * <td><b>lookupTable</b></td>
 * <td>name of lookup table where are slave records</td>
 * <tr>
 * <td><b>leftOuterJoin</b><i>optional</i>
 * <td>true/false<I> default: FALSE</I> See description.</td>
 * <tr>
 * <td><b>freeLookupTable</b><i>optional</i>
 * <td>true/false<I> default: FALSE</I> idicates if close lookup table after
 * finishing execute() method. All records, which are stored only in memory will
 * be lost.</td>
 *  <tr><td><b>errorActions </b><i>optional</i></td><td>defines if graph is to stop, when transformation returns negative value.
 *  Available actions are: STOP or CONTINUE. For CONTINUE action, error message is logged to console or file (if errorLog attribute
 *  is specified) and for STOP there is thrown TransformExceptions and graph execution is stopped. <br>
 *  Error action can be set for each negative value (value1=action1;value2=action2;...) or for all values the same action (STOP 
 *  or CONTINUE). It is possible to define error actions for some negative values and for all other values (MIN_INT=myAction).
 *  Default value is <i>-1=CONTINUE;MIN_INT=STOP</i></td></tr>
 *  <tr><td><b>errorLog</b><br><i>optional</i></td><td>path to the error log file. Each error (after which graph continues) is logged in 
 *  following way: inRecordNumber;errorCode;errorMessage;semiResult - fields are delimited by Defaults.Component.KEY_FIELDS_DELIMITER.</td></tr>
 * </table>
 * <h4>Example:</h4>
 * 
 * <pre>
 *   &lt;Node id=&quot;JOIN&quot; type=&quot;LOOKUP_JOIN&quot;&gt;
 *   &lt;attr name=&quot;lookupTable&quot;&gt;LookupTable0&lt;/attr&gt;
 *   &lt;attr name=&quot;joinKey&quot;&gt;EmployeeID&lt;/attr&gt;
 *   &lt;attr name=&quot;transform&quot;&gt;
 *   import org.jetel.component.DataRecordTransform;
 *   import org.jetel.data.DataRecord;
 *   import org.jetel.data.RecordKey;
 *   import org.jetel.data.lookup.LookupTable;
 *   import org.jetel.exception.JetelException;
 *   import org.jetel.graph.TransformationGraph;
 *   
 *   public class reformatTest extends DataRecordTransform{
 *   
 *   	public int transform(DataRecord[] source, DataRecord[] target){
 *   		if (source[1]==null) return SKIP; // skip this one
 *   
 *   		target[0].getField(0).setValue(source[0].getField(0).getValue());
 *     		target[0].getField(1).setValue(source[0].getField(1).getValue());
 *   		target[0].getField(2).setValue(source[0].getField(2).getValue());
 *   		target[0].getField(3).setValue(source[1].getField(0).getValue().toString());
 *   		target[0].getField(4).setValue(source[1].getField(1).getValue());
 *   
 *   		return ALL;
 *   	}
 *
 *   }
 *   &lt;/attr&gt;
 *   &lt;/Node&gt;
 * </pre>
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; (c) JavlinConsulting
 *         s.r.o. www.javlinconsulting.cz
 * 
 * @since Dec 11, 2006
 * 
 */
public class LookupJoin extends Node {

	private static final String XML_LOOKUP_TABLE_ATTRIBUTE = "lookupTable";

	private static final String XML_FREE_LOOKUP_TABLE_ATTRIBUTE = "freeLookupTable";

	private static final String XML_JOIN_KEY_ATTRIBUTE = "joinKey";

	private static final String XML_TRANSFORM_CLASS_ATTRIBUTE = "transformClass";

	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_TRANSFORMURL_ATTRIBUTE = "transformURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";

	private static final String XML_LEFTOUTERJOIN_ATTRIBUTE = "leftOuterJoin";
	private static final String XML_ERROR_ACTIONS_ATTRIBUTE = "errorActions";
    private static final String XML_ERROR_LOG_ATTRIBUTE = "errorLog";

	public final static String COMPONENT_TYPE = "LOOKUP_JOIN";

	private final static int WRITE_TO_PORT = 0;
	private final static int REJECTED_PORT = 1;
	private final static int READ_FROM_PORT = 0;

	private String transformClassName = null;

	private RecordTransform transformation = null;
	private String transformURL = null;
	private String charset = null;

	private String transformSource = null;

	private String lookupTableName;

	private boolean freeLookupTable = false;

	private String[] joinKey;

	private boolean leftOuterJoin = false;

	private Properties transformationParameters = null;

	private Lookup lookup;

	private String errorActionsString;
	private Map<Integer, ErrorAction> errorActions = new HashMap<Integer, ErrorAction>();
	private String errorLogURL;
	private FileWriter errorLog;

	private RecordKey recordKey;

	static Log logger = LogFactory.getLog(Reformat.class);

	/**
	 * @param id component identification
	 * @param lookupTableName
	 * @param joinKey
	 * @param transform
	 * @param transformClass
	 */
	public LookupJoin(String id, String lookupTableName, String[] joinKey,
			String transform, String transformClass, String transformURL) {
		super(id);
		this.lookupTableName = lookupTableName;
		this.joinKey = joinKey;
		this.transformClassName = transformClass;
		this.transformSource = transform;
		this.transformURL = transformURL;
	}

	public LookupJoin(String id, String lookupTableName, String[] joinKey,
			RecordTransform transform) {
		this(id, lookupTableName, joinKey, null, null, null);
		this.transformation = transform;
	}

    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
		transformation.preExecute();
    	
    	if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	}
    	else {
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

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#postExecute(org.jetel.graph.TransactionMethod)
     */
    @Override
    public void postExecute() throws ComponentNotReadyException {
    	super.postExecute();
    	transformation.postExecute();
        transformation.finished();
        
    	try {
		    if (errorLog != null) {
				errorLog.close();
			}
    	} catch (Exception e) {
    		//maybe this should be only logged and post execution should continue
    		throw new ComponentNotReadyException(e);
    	}
    }
    
	@Override
	public Result execute() throws Exception {
		// initialize in and out records
		InputPort inPort = getInputPort(WRITE_TO_PORT);
		DataRecord inRecord = DataRecordFactory.newRecord(inPort.getMetadata());
		inRecord.init();
		lookup = getGraph().getLookupTable(lookupTableName).createLookup(recordKey, inRecord);
		OutputPort rejectedPort = getOutputPort(REJECTED_PORT);
		DataRecord[] outRecord = { DataRecordFactory.newRecord(getOutputPort(READ_FROM_PORT)
				.getMetadata()) };
		outRecord[0].init();
		outRecord[0].reset();
		DataRecord[] inRecords = new DataRecord[] { inRecord, null };
		int counter = 0;
		
		// test if the lookup needs runtime metadata
		LookupTable lookupTable = getGraph().getLookupTable(lookupTableName);
		boolean createTransformation = runtimeMetadata(lookupTable);
		
		while (inRecord != null && runIt) {
			inRecord = inPort.readRecord(inRecord);
			if (inRecord != null) {
				// find slave record in database
			    lookup.seek();
                inRecords[1] = lookup.hasNext() ? lookup.next() : NullRecord.NULL_RECORD;

				// create the transformation
    			if (createTransformation){
    				// get metadata
    				DataRecordMetadata lookupMetadata = lookupTable.getMetadata();
    				DataRecordMetadata inMetadata[] = {	getInputPort(READ_FROM_PORT).getMetadata(), lookupMetadata };
    				DataRecordMetadata outMetadata[] = { getOutputPort(WRITE_TO_PORT).getMetadata() };
    				
    				// create the transformation
    	        	TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
    	        	transformFactory.setTransform(transformSource);
    	        	transformFactory.setTransformClass(transformClassName);
    	        	transformFactory.setTransformUrl(transformURL);
    	        	transformFactory.setCharset(charset);
    	        	transformFactory.setComponent(this);
    	        	transformFactory.setInMetadata(inMetadata);
    	        	transformFactory.setOutMetadata(outMetadata);
    				transformation = transformFactory.createTransform();

    				// init transformation
    		        if (!transformation.init(transformationParameters, inMetadata, outMetadata)) {
    		            throw new ComponentNotReadyException("Error when initializing tranformation function.");
    		        }
    				createTransformation = false;
    			}

                do {
					if ((inRecords[1] != NullRecord.NULL_RECORD || leftOuterJoin)) {
						
						outRecord[0].reset();
						
						int transformResult = -1;
						try {
							transformResult = transformation.transform(inRecords, outRecord);
						} catch (Exception exception) {
							transformResult = transformation.transformOnError(exception, inRecords, outRecord);
						}

						if (transformResult >= 0) {
							writeRecord(WRITE_TO_PORT, outRecord[0]);
						}else if (transformResult < 0) {
							ErrorAction action = errorActions.get(transformResult);
							if (action == null) {
								action = errorActions.get(Integer.MIN_VALUE);
								if (action == null) {
									action = ErrorAction.DEFAULT_ERROR_ACTION;
								}
							}
							String message = "Transformation finished with code: " + transformResult + ". Error message: " + 
								transformation.getMessage();
							if (action == ErrorAction.CONTINUE) {
								if (errorLog != null){
									errorLog.write(String.valueOf(counter));
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
					}else{
						if (rejectedPort != null) {
							writeRecord(REJECTED_PORT, inRecord);
						}							
					}
					// get next record from lookup table with the same key
	                inRecords[1] = lookup.hasNext() ? lookup.next() : NullRecord.NULL_RECORD;
				} while (inRecords[1] != NullRecord.NULL_RECORD);

			}
			counter++;
		}

		if (freeLookupTable) {
			lookup.getLookupTable().clear();
		}
		if (errorLog != null){
			errorLog.flush();
			errorLog.close();
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public void free() {
        if (!isInitialized()) {
            return;
        }

        super.free();
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 1, 1) || !checkOutputPorts(status, 1, 2)) {
        	return status;
        }
        
        if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }

        if (getOutputPort(REJECTED_PORT) != null) {
            checkMetadata(status, getInputPort(READ_FROM_PORT).getMetadata(), getOutputPort(REJECTED_PORT).getMetadata());
        }

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

		LookupTable lookupTable = getGraph().getLookupTable(lookupTableName);

		if (lookupTable == null) {
			status.add(new ConfigurationProblem("Lookup table \"" + lookupTableName + "\" not found.", Severity.ERROR,
					this, Priority.NORMAL));
		} else if (transformation == null && !runtimeMetadata(lookupTable)) {
			DataRecordMetadata[] inMetadata = { getInputPort(READ_FROM_PORT).getMetadata(), lookupTable.getMetadata() };
			DataRecordMetadata[] outMetadata = { getOutputPort(WRITE_TO_PORT).getMetadata() };
		
            //check transformation
        	getTransformFactory(inMetadata, outMetadata).checkConfig(status);

			//check join key
        	try {
				recordKey = new RecordKey(joinKey, inMetadata[0]);
				recordKey.init();
			} catch (Exception e) {
				status.add(new ConfigurationProblem("Join key parsing error.", e, Severity.ERROR, this, Priority.NORMAL, XML_JOIN_KEY_ATTRIBUTE));
			}
        }
        
        return status;
	}

	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		// Initializing lookup table
		LookupTable lookupTable = getGraph().getLookupTable(lookupTableName);
		if (lookupTable == null) {
			throw new ComponentNotReadyException("Lookup table \""
					+ lookupTableName + "\" not found.");
		}
		if (!lookupTable.isInitialized()) {
			lookupTable.init();
		}
		DataRecordMetadata lookupMetadata = lookupTable.getMetadata();
		DataRecordMetadata inMetadata[] = {	getInputPort(READ_FROM_PORT).getMetadata(), lookupMetadata };
		DataRecordMetadata outMetadata[] = { getOutputPort(WRITE_TO_PORT).getMetadata() };
		try {
			recordKey = new RecordKey(joinKey, inMetadata[0]);
			recordKey.init();
			if (transformation == null && !runtimeMetadata(lookupTable)) {
				transformation = getTransformFactory(inMetadata, outMetadata).createTransform();
			}
			// init transformation
	        if (transformation != null && !transformation.init(transformationParameters, inMetadata, outMetadata)) {
	            throw new ComponentNotReadyException("Error when initializing tranformation function.");
	        }
		} catch (Exception e) {
			throw new ComponentNotReadyException(this, e);
		}		

		if (leftOuterJoin && getOutputPort(REJECTED_PORT) != null) {
			logger.info(this.getId() + " info: There will be no skipped records " +
					"while left outer join is switched on");
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
	 * Returns true if a lookup table doesn't have to have a metadata and doesn't have the metadata.
	 * @param lookupTable
	 * @return
	 */
	private boolean runtimeMetadata(LookupTable lookupTable) {
		return lookupTable instanceof DBLookupTable && lookupTable.getMetadata() == null;
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		LookupJoin join;
		String[] joinKey;
		// get necessary parameters
		joinKey = xattribs.getString(XML_JOIN_KEY_ATTRIBUTE).split(
				Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);

		join = new LookupJoin(xattribs.getString(XML_ID_ATTRIBUTE),
				xattribs.getString(XML_LOOKUP_TABLE_ATTRIBUTE), joinKey,
				xattribs.getStringEx(XML_TRANSFORM_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF),
				xattribs.getString(XML_TRANSFORM_CLASS_ATTRIBUTE, null),
				xattribs.getStringEx(XML_TRANSFORMURL_ATTRIBUTE, null, RefResFlag.URL));
		join.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE, null));
		join.setTransformationParameters(xattribs
						.attributes2Properties(new String[] { XML_TRANSFORM_CLASS_ATTRIBUTE }));
		if (xattribs.exists(XML_LEFTOUTERJOIN_ATTRIBUTE)) {
			join.setLeftOuterJoin(xattribs
					.getBoolean(XML_LEFTOUTERJOIN_ATTRIBUTE));
		}
		join.setFreeLookupTable(xattribs.getBoolean(
				XML_FREE_LOOKUP_TABLE_ATTRIBUTE, false));
		if (xattribs.exists(XML_ERROR_ACTIONS_ATTRIBUTE)){
			join.setErrorActions(xattribs.getString(XML_ERROR_ACTIONS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_ERROR_LOG_ATTRIBUTE)){
			join.setErrorLog(xattribs.getString(XML_ERROR_LOG_ATTRIBUTE));
		}

		return join;
	}

	public void setErrorLog(String errorLog) {
		this.errorLogURL = errorLog;
	}

	public void setErrorActions(String string) {
		this.errorActionsString = string;		
	}

	/**
	 * @param transformationParameters
	 *            The transformationParameters to set.
	 */
	public void setTransformationParameters(Properties transformationParameters) {
		this.transformationParameters = transformationParameters;
	}

	public void setLeftOuterJoin(boolean leftOuterJoin) {
		this.leftOuterJoin = leftOuterJoin;
	}

	public void setFreeLookupTable(boolean freeLookupTable) {
		this.freeLookupTable = freeLookupTable;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

}
