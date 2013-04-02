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
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.graph.runtime.tracker.ReformatComponentTokenTracker;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Reformat Component</h3>
 *
 * <!-- Changes / reformats the data between INPUT/OUTPUT ports.
 *  In general, it transforms between 1 input and several output ports.
 *  This component is only a wrapper around transformation class implementing
 *  org.jetel.component.RecordTransform interface. The method transform
 *  is called for every record passing through this component -->
 *
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Reformat</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Changes / reformats data record between one INPUT and several OUTPUT ports.<br>
 *  This component is only a wrapper around transformation class implementing
 *  <i>org.jetel.component.RecordTransform</i> interface. The method <i>transform</i>
 *  is called for every record passing through this component.<br></td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0]...[n] - output records (on several output ports)</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"REFORMAT"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>transformClass</b></td><td>name of the class to be used for transforming data</td>
 *  </tr>
 *  <tr><td><b>transform</b></td><td>contains definition of transformation as java source, in internal clover format or in Transformation Language</td></tr>
 *  <tr><td><b>transformURL</b></td><td>path to the file with transformation code</td></tr>
 *  <tr><td><b>charset </b><i>optional</i></td><td>encoding of extern source</td></tr>
 *  <tr><td><b>errorActions </b><i>optional</i></td><td>defines if graph is to stop, when transformation returns negative value.
 *  Available actions are: STOP or CONTINUE. For CONTINUE action, error message is logged to console or file (if errorLog attribute
 *  is specified) and for STOP there is thrown TransformExceptions and graph execution is stopped. <br>
 *  Error action can be set for each negative value (value1=action1;value2=action2;...) or for all values the same action (STOP 
 *  or CONTINUE). It is possible to define error actions for some negative values and for all other values (MIN_INT=myAction).
 *  Default value is <i>-1=CONTINUE;MIN_INT=STOP</i></td></tr>
 *  <tr><td><b>errorLog</b><br><i>optional</i></td><td>path to the error log file. Each error (after which graph continues) is logged in 
 *  following way: recordNumber;errorCode;errorMessage;semiResult - fields are delimited by Defaults.Component.KEY_FIELDS_DELIMITER.</td></tr>
 *  <tr><td><i>..optional attribute..</i></td><td>any additional attribute is passed to transformation
 * class in Properties object - as a key->value pair. There is no limit to how many optional
 * attributes can be used.</td>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="REF" type="REFORMAT" transformClass="org.jetel.test.reformatOrders" param1="123" param2="XYZ"/&gt;</pre>
 *  <br>
 *  Example with transformation code embedded into graph:<br>
 *  <pre>
 * &lt;Node id="REF" type="REFORMAT"&gt;
 * &lt;attr name="javaSource"&gt;
 * import org.jetel.component.DataRecordTransform;
 * import org.jetel.metadata.DataRecordMetadata;
 * import org.jetel.data.*;
 *
 * public class reformatOrders extends DataRecordTransform{
 *   DataRecord source,target;
 *   int counter;
 *   String message;
 *	
 *	public int transform(DataRecord[] _source, DataRecord[] _target){
 *	    source=_source[0]; target=_target[0];
 *	 	StringBuffer strBuf=new StringBuffer(80);
 *
 *		try{
 *			// mapping among source + target fields
 *			// some fields get assigned directly from source fields, some
 *			// are assigned from internall variables
 *			SetVal.setInt(target,&quot;OrderKey&quot;,counter);
 *			SetVal.setInt(target,&quot;OrderID&quot;,GetVal.getInt(source,&quot;OrderID&quot;));
 *			SetVal.setString(target,&quot;CustomerID&quot;,GetVal.getString(source,&quot;CustomerID&quot;));
 *			SetVal.setValue(target,&quot;OrderDate&quot;,GetVal.getDate(source,&quot;OrderDate&quot;));
 *			SetVal.setString(target,&quot;ShippedDate&quot;,&quot;02.02.1999&quot;);
 *			SetVal.setInt(target,&quot;ShipVia&quot;,GetVal.getInt(source,&quot;ShipVia&quot;));
 *			SetVal.setString(target,&quot;ShipTo&quot;,parameters.getProperty("param2","unknown");
 *		}catch(Exception ex){
 *			message=ex.getMessage()+&quot; -&gt;occured with record :&quot;+counter;
 *			throw new RuntimeException(message);
 *		}
 *
 *		counter++;
 *
 *		return ALL;
 *	}
 *
 * }
 * &lt;/attr&gt;
 * &lt;/Node&gt;
 * </pre>
 * 
 * <pre>
 * &lt;Node errorActions="MIN_INT=CONTINUE" errorLog="${DATATMP_DIR}/log.txt" id="REF" transformURL="${TRANS_DIR}/reformatOrders.java" type="REFORMAT"/&gt;
 * <hr>
 * <i><b>Note:</b> DataRecord and in turn its individual fields retain their last assigned value till
 * explicitly changed ba calling <code>setValue()</code>, <code>fromString()</code> or other method.<br>
 * Values are not reset to defaults or NULLs between individual calls to <code>transform()</code> method. It is
 * programmer's responsibility to secure assignment of proper values each time the <code>transform()</code> method
 * is called.</i>
 * <hr>
 * <br> 
 *
 * @author      dpavlis
 * @since       April 4, 2002
 */
public class Reformat extends Node {

	public static final String XML_TRANSFORMCLASS_ATTRIBUTE = "transformClass";
	public static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	public static final String XML_TRANSFORMURL_ATTRIBUTE = "transformURL";
	public static final String XML_CHARSET_ATTRIBUTE = "charset";
	public static final String XML_ERROR_ACTIONS_ATTRIBUTE = "errorActions";
	public static final String XML_ERROR_LOG_ATTRIBUTE = "errorLog";
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "REFORMAT";

	private final static int READ_FROM_PORT = 0;

    private String transform = null;
	private String transformClass = null;
	private String transformURL = null;
	private String charset = null;
	private RecordTransform transformation = null;
	private String errorActionsString;
	private Map<Integer, ErrorAction> errorActions = new HashMap<Integer, ErrorAction>();
	private String errorLogURL;
	private FileWriter errorLog;

	private Properties transformationParameters = null;
	
	static Log logger = LogFactory.getLog(Reformat.class);

    /**
	 *Constructor for the Reformat object
	 *
	 * @param  id              unique identification of component
	 * @param  transformClass  Name of transformation CLASS (e.g. org.jetel.test.DemoTrans)
	 */
	public Reformat(String id, String transform, String transformClass, String transformURL) {
		super(id);
        this.transform = transform;
        this.transformClass = transformClass;
        this.transformURL = transformURL;
	}
    
    public Reformat(String id, RecordTransform transformation) {
        super(id);
        this.transformation = transformation;
    }
    
	/**
	 * Returns the name of the attribute which contains transformation
	 * 
	 * @return the name
	 */
	public static String getTransformAttributeName() {
		return XML_TRANSFORM_ATTRIBUTE;
	}

    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
		transformation.preExecute();

    	if (firstRun()) {//a phase-dependent part of initialization
            if (errorLogURL != null) {
            	try {
    				errorLog = new FileWriter(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), errorLogURL));
    			} catch (IOException e) {
    				throw new ComponentNotReadyException(this, XML_ERROR_LOG_ATTRIBUTE, e);
    			}
            }
    	}
    	else {
    		transformation.reset();
    	    if (errorLogURL != null) {
    	    	try {
    				errorLog = new FileWriter(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), errorLogURL));
    			} catch (IOException e) {
    				throw new ComponentNotReadyException(this, XML_ERROR_LOG_ATTRIBUTE, e);
    			}
    	    }
    	}
    }    

	
	@Override
	public Result execute() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord inRecord[] = {DataRecordFactory.newRecord(inPort.getMetadata())};
		int numOutputPorts=getOutPorts().size();
		DataRecord outRecord[] = new DataRecord[numOutputPorts]; 
		
		inRecord[0].init();
		// initialize output ports
		for (int i = 0; i < numOutputPorts; i++) {
			outRecord[i] = DataRecordFactory.newRecord(getOutputPort(i).getMetadata());
			outRecord[i].init();
			outRecord[i].reset();
		}

		int counter = 0;
		// MAIN PROCESSING LOOP
		while ((inRecord[0] = readRecord(READ_FROM_PORT, inRecord[0])) != null && runIt) {
			for (int i=0;i<numOutputPorts;i++){
			    outRecord[i].reset();
			}

			int transformResult = -1;

			try {
				transformResult = transformation.transform(inRecord, outRecord);
			} catch (Exception exception) {
				transformResult = transformation.transformOnError(exception, inRecord, outRecord);
			}

			if (transformResult == RecordTransform.ALL) {
				for (int outPort = 0; outPort < numOutputPorts; outPort++) {
					writeRecord(outPort, outRecord[outPort]);
				}
			} else if (transformResult >= 0) {
				writeRecord(transformResult, outRecord[transformResult]);
			} else if (transformResult == RecordTransform.SKIP) {
				// DO NOTHING - skip the record
			} else if (transformResult <= RecordTransform.STOP) {
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
			counter++;
			SynchronizeUtils.cloverYield();
		}

		if (errorLog != null){
			errorLog.flush();
		}

		broadcastEOF();

		return (runIt ? Result.FINISHED_OK : Result.ABORTED);
	}

    @Override
    public void postExecute() throws ComponentNotReadyException {
    	super.postExecute();
		transformation.postExecute();
		transformation.finished();

    	try {
    		if (errorLog != null) {
    			errorLog.close();
    		}
    	}
    	catch (Exception e) {
    		throw new ComponentNotReadyException(e);
    	}
    }    


	/**
	 *  Initialization of component
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
        
		//create instance of record transformation
        if (transformation == null) {
			transformation = getTransformFactory().createTransform();
		}
        
		// init transformation
        if (!transformation.init(transformationParameters, getInMetadataArray(), getOutMetadataArray())) {
            throw new ComponentNotReadyException("Error when initializing tranformation function.");
        }

        errorActions = ErrorAction.createMap(errorActionsString);
	}

	private TransformFactory<RecordTransform> getTransformFactory() {
    	TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
    	transformFactory.setTransform(transform);
    	transformFactory.setTransformClass(transformClass);
    	transformFactory.setTransformUrl(transformURL);
    	transformFactory.setCharset(charset);
    	transformFactory.setComponent(this);
    	transformFactory.setInMetadata(getInMetadata());
    	transformFactory.setOutMetadata(getOutMetadata());
    	return transformFactory;
	}
	
    /**
	 * @param transformationParameters
	 *            The transformationParameters to set.
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
		Reformat reformat;

        reformat = new Reformat(
                        xattribs.getString(XML_ID_ATTRIBUTE),
                        xattribs.getStringEx(XML_TRANSFORM_ATTRIBUTE, null,RefResFlag.SPEC_CHARACTERS_OFF), 
                        xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
                        xattribs.getStringEx(XML_TRANSFORMURL_ATTRIBUTE,null,RefResFlag.SPEC_CHARACTERS_OFF));
		reformat.setTransformationParameters(xattribs.attributes2Properties(
				new String[]{XML_ID_ATTRIBUTE,XML_TRANSFORM_ATTRIBUTE,XML_TRANSFORMCLASS_ATTRIBUTE}));
		reformat.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE, null));
		if (xattribs.exists(XML_ERROR_ACTIONS_ATTRIBUTE)){
			reformat.setErrorActions(xattribs.getString(XML_ERROR_ACTIONS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_ERROR_LOG_ATTRIBUTE)){
			reformat.setErrorLog(xattribs.getString(XML_ERROR_LOG_ATTRIBUTE));
		}
		return reformat;
	}


	public void setErrorLog(String errorLog) {
		this.errorLogURL = errorLog;
	}

	public void setErrorActions(String string) {
		this.errorActionsString = string;		
	}

	/**
	 *  Checks that component is configured properly
	 *
	 * @return    Description of the Return Value
	 */
        @Override
        public ConfigurationStatus checkConfig(ConfigurationStatus status) {
    		super.checkConfig(status);
   		 
    		if(!checkInputPorts(status, 1, 1) || !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
    			return status;
    		}
    		
    		if (charset != null && !Charset.isSupported(charset)) {
            	status.add(new ConfigurationProblem(
                		"Charset "+charset+" not supported!", 
                		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
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

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}

	@Override
    public synchronized void free() {
        super.free();
    }

	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new ReformatComponentTokenTracker(this);
	}

}

