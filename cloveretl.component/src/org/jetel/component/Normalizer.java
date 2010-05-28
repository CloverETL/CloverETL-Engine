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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.normalize.CTLRecordNormalize;
import org.jetel.component.normalize.CTLRecordNormalizeAdapter;
import org.jetel.component.normalize.RecordNormalize;
import org.jetel.component.normalize.RecordNormalizeTL;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ITLCompiler;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.compile.DynamicJavaCode;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;

/**
 *  <h3>Normalizer Component</h3>
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Normalizer</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Normalizes input records - ie decomposes each input record to several output records using user-specified transformation.
 * The transformation is supposed to implement interface <code>RecordNormalize</code>.
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>One input port to read the records to be normalized.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>One output port to write results of normalization.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"NORMALIZER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>normalizeClass</b></td><td>name of the class to be used for normalizing data.</td></tr>
 *  <tr><td><b>normalize</b></td><td>contains definition of transformation in Java or TransformLang.</td></tr>
 *  <tr><td><b>normalizeURL</b></td><td>path to the file with normalizing code</td></tr>
 *  <tr><td><b>charset</b><i>optional</i></td><td>encoding of extern source</td></tr>
 *  </tr>
 *  <tr><td><b>errorActions </b><i>optional</i></td><td>defines if graph is to stop, when denormalize functions return negative value.
 *  Available actions are: STOP or CONTINUE. For CONTINUE action, error message is logged to console or file (if errorLog attribute
 *  is specified) and for STOP there is thrown TransformExceptions and graph execution is stopped. <br>
 *  Error action can be set for each negative value (value1=action1;value2=action2;...) or for all values the same action (STOP 
 *  or CONTINUE). It is possible to define error actions for some negative values and for all other values (MIN_INT=myAction).
 *  Default value is <i>-1=CONTINUE;MIN_INT=STOP</i></td></tr>
 *  <tr><td><b>errorLog</b><br><i>optional</i></td><td>path to the error log file. Each error (after which graph continues) is logged in 
 *  following way: methodName;inputRecordNumber;errorCode;errorMessage - fields are delimited by Defaults.Component.KEY_FIELDS_DELIMITER,
 *  methodName can be <i>transform</i> or <i>count</i>.</td></tr>
 *  </table>
 *
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/15/06  
 * @see         org.jetel.data.parser.FixLenDataParser
 */
public class Normalizer extends Node {

	private static final String XML_TRANSFORMCLASS_ATTRIBUTE = "normalizeClass";
	private static final String XML_TRANSFORM_ATTRIBUTE = "normalize";
	private static final String XML_TRANSFORMURL_ATTRIBUTE = "normalizeURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_ERROR_ACTIONS_ATTRIBUTE = "errorActions";
    private static final String XML_ERROR_LOG_ATTRIBUTE = "errorLog";
	
	private static final int IN_PORT = 0;
	private static final int OUT_PORT = 0;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "NORMALIZER";

	private Properties transformationParameters;

	static Log logger = LogFactory.getLog(Normalizer.class);

	private InputPort inPort;
	private OutputPort outPort;
	private RecordNormalize norm;
	
	private DataRecordMetadata inMetadata;
	private DataRecordMetadata outMetadata;
	
	private String xformClass;
	private String xform;
	private String xformURL = null;
	private String charset = null;
		
	private String errorActionsString;
	private Map<Integer, ErrorAction> errorActions = new HashMap<Integer, ErrorAction>();
	private String errorLogURL;
	private FileWriter errorLog;

	/**
	 * Sole ctor.
	 * @param id Component ID
	 * @param xform Normalization implementation source code (either Java or TransformLang).
	 * @param xformClass Normalization class.
	 */
	public Normalizer(String id, String xform, String xformClass, String xformURL) {
		super(id);
		this.xformClass = xformClass;
		this.xform = xform;
		this.xformURL = xformURL;
	}

	public Normalizer(String id, RecordNormalize xform) {
		this(id, null, null, null);
		this.norm = xform;
	}
	
	/**
	 * Returns the name of the attribute which contains transformation
	 * 
	 * @return the name
	 */
	public static String getTransformAttributeName() {
		return XML_TRANSFORM_ATTRIBUTE;
	}

	/**
	 * Creates normalization instance using specified class.
	 * @param normClass
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private RecordNormalize createNormalizer(String normClass) throws ComponentNotReadyException {
		RecordNormalize norm;
        try {
            norm =  (RecordNormalize)Class.forName(normClass).newInstance();
        }catch (InstantiationException ex){
            throw new ComponentNotReadyException("Can't instantiate transformation class: "+ex.getMessage());
        }catch (IllegalAccessException ex){
            throw new ComponentNotReadyException("Can't instantiate transformation class: "+ex.getMessage());
        }catch (ClassNotFoundException ex) {
            throw new ComponentNotReadyException("Can't find specified transformation class: " + xformClass);
        }
		return norm;
	}

	/**
	 * Creates normalization instance using given Java source.
	 * @param normCode
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private RecordNormalize createNormalizerDynamic(String normCode) throws ComponentNotReadyException {
		DynamicJavaCode dynCode = new DynamicJavaCode(normCode, this.getClass().getClassLoader());
        logger.info(" (compiling dynamic source) ");
        // use DynamicJavaCode to instantiate transformation class
        Object transObject = null;
        try {
            transObject = dynCode.instantiate();
        } catch (RuntimeException ex) {
            logger.debug(dynCode.getCompilerOutput());
            logger.debug(dynCode.getSourceCode());
            throw new ComponentNotReadyException("Transformation code is not compilable.\n" + "Reason: " + ex.getMessage());
        }
        if (transObject instanceof RecordNormalize) {
            return (RecordNormalize)transObject;
        } else {
            throw new ComponentNotReadyException("Provided transformation class doesn't implement RecordNormalize.");
        }
    }
		
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		inPort = getInputPort(IN_PORT);
		outPort = getOutputPort(OUT_PORT);	
		inMetadata = inPort.getMetadata();
		outMetadata = outPort.getMetadata();

		if (norm == null) {
			if (xformClass != null) {
				norm = createNormalizer(xformClass);
			}else if (xform == null) {
				xform = FileUtils.getStringFromURL(getGraph().getProjectURL(), xformURL, charset);
			}
			if (xformClass == null) {
				norm = createTransform(xform);
			}
        	// set graph instance to transformation (if CTL it can access lookups etc.)
        	norm.setGraph(getGraph());
		}
		if (!norm.init(transformationParameters, inMetadata, outMetadata)) {
			throw new ComponentNotReadyException("Normalizer initialization failed: " + norm.getMessage());
		}
        errorActions = ErrorAction.createMap(errorActionsString);
	}

	private RecordNormalize createTransform(String sourceCode) throws ComponentNotReadyException {
		switch (RecordTransformFactory.guessTransformType(sourceCode)) {
			case RecordTransformFactory.TRANSFORM_JAVA_SOURCE:
				return createNormalizerDynamic(sourceCode);
			case RecordTransformFactory.TRANSFORM_CLOVER_TL:
				return new RecordNormalizeTL(logger, sourceCode, getGraph());
			case RecordTransformFactory.TRANSFORM_CTL:
				ITLCompiler compiler = TLCompilerFactory.createCompiler(getGraph(),
						new DataRecordMetadata[] { getInputPort(IN_PORT).getMetadata() },
						new DataRecordMetadata[] { getOutputPort(OUT_PORT).getMetadata() }, "UTF-8");
				List<ErrorMessage> msgs = compiler.compile(sourceCode, CTLRecordNormalize.class, getId());
				if (compiler.errorCount() > 0) {
					for (ErrorMessage msg : msgs) {
						logger.error(msg.toString());
					}
					throw new ComponentNotReadyException("CTL code compilation finished with "
							+ compiler.errorCount() + " errors");
				}
				Object ret = compiler.getCompiledCode();
				if (ret instanceof TransformLangExecutor) {
					// setup interpreted runtime
					return new CTLRecordNormalizeAdapter((TransformLangExecutor) ret, logger);
				} else if (ret instanceof CTLRecordNormalize) {
					return (CTLRecordNormalize) ret;
				}
	
				// this should never happen as compiler always generates correct interface
				throw new ComponentNotReadyException("Invalid type of record transformation");
			default:
				throw new ComponentNotReadyException("Can't determine transformation code type at component ID: " + getId());
		}
	}

	/**
	 * Processes all input records.
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws TransformException
	 */
	private void processInput() throws IOException, InterruptedException, TransformException {
		DataRecord inRecord = new DataRecord(inMetadata);
		inRecord.init();
		DataRecord outRecord = new DataRecord(outMetadata);
		outRecord.init();
		outRecord.reset();
		int src = 0;
		int transformResult;
		while (runIt) {
			if (inPort.readRecord(inRecord) == null) { // no more input data
				return;
			}
			int count = norm.count(inRecord);
			if (count < 0) {
				handleException("count", count, src);
			}
			for (int idx = 0; idx < count; idx++) {
				transformResult = norm.transform(inRecord, outRecord, idx);
				if (transformResult >= 0) {
					outPort.writeRecord(outRecord);
					outRecord.reset();
				}else{
					handleException("transform", transformResult, src);
				}
			}
			norm.clean();
			SynchronizeUtils.cloverYield();
			src++;
		} // while
	}

	private void handleException(String functionName, int transformResult, int recNo) throws TransformException, IOException{
		ErrorAction action = errorActions.get(transformResult);
		if (action == null) {
			action = errorActions.get(Integer.MIN_VALUE);
			if (action == null) {
				action = ErrorAction.DEFAULT_ERROR_ACTION;
			}
		}
		String message = "Method " + functionName + " finished with code: " + transformResult + ". Error message: " + 
			norm.getMessage();
		if (action == ErrorAction.CONTINUE) {
			if (errorLog != null){
				errorLog.write(functionName);
				errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
				errorLog.write(String.valueOf(recNo));
				errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
				errorLog.write(String.valueOf(transformResult));
				errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
				message = norm.getMessage();
				if (message != null) {
					errorLog.write(message);
				}
				errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
				errorLog.write("\n");
			}else{
				logger.warn(message);
			}
		}else{
			if (errorLog != null){
				errorLog.flush();
				errorLog.close();
			}
			throw new TransformException(message);
			
		}
	}

	@Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
        if (errorLogURL != null) {
           	try {
           		errorLog = new FileWriter(FileUtils.getFile(getGraph().getProjectURL(), errorLogURL));
   			} catch (IOException e) {
    			throw new ComponentNotReadyException(this, XML_ERROR_LOG_ATTRIBUTE, e.getMessage());
    		}
        }
    }
 
	@Override
	public Result execute() throws Exception {
		try {
			processInput();
		} catch (Exception e) {
			throw e;
		} finally {
		    norm.finished();

		    if (errorLog != null){
				errorLog.flush();
			}

		    broadcastEOF();
		}

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

    
    @Override
    public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException {
    	super.postExecute(transactionMethod);
    	
    	try {
		    if (errorLog != null){
				errorLog.flush();
			}
    	}
    	catch (Exception e) {
    		throw new ComponentNotReadyException(COMPONENT_TYPE + ": " + e.getMessage(),e);
    	}
    }
	
	
	public String getType() {
		return COMPONENT_TYPE;
	}

    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
		if(!checkInputPorts(status, 1, 1)
				|| !checkOutputPorts(status, 1, 1)) {
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
				FileUtils.canWrite(getGraph().getProjectURL(), errorLogURL);
			} catch (ComponentNotReadyException e) {
				status.add(new ConfigurationProblem(e, Severity.WARNING, this, Priority.NORMAL, XML_ERROR_LOG_ATTRIBUTE));
			}
        }
		
        // transformation source for checkconfig
        String checkTransform = null;
        if (xform != null) {
        	checkTransform = xform;
        } else if (xformURL != null) {
        	checkTransform = FileUtils.getStringFromURL(getGraph().getProjectURL(), xformURL, charset);
        }
        // only the transform and transformURL parameters are checked, transformClass is ignored
        if (checkTransform != null) {
        	int transformType = RecordTransformFactory.guessTransformType(checkTransform);
			if (transformType != RecordTransformFactory.TRANSFORM_JAVA_SOURCE) {
    			try {
    				RecordNormalize norm = createTransform(checkTransform);
    				norm.setGraph(getGraph());
    				norm.init(transformationParameters, inMetadata, outMetadata);
    			} catch (ComponentNotReadyException e) {
					// find which component attribute was used
					String attribute = xform != null ? XML_TRANSFORM_ATTRIBUTE : XML_TRANSFORMURL_ATTRIBUTE;
					// report CTL error as a warning
					status.add(new ConfigurationProblem(e, Severity.WARNING, this, Priority.NORMAL, attribute));
				}
        	}
        }

        return status;
   }

	/**
	 * Sets normalization parameters.
	 * @param transformationParameters
	 */
	private void setTransformationParameters(Properties transformationParameters) {
		this.transformationParameters = transformationParameters;
	}

	/**
	 * Creates class instance according to XML specification. 
	 * @param graph
	 * @param xmlElement
	 * @return
	 * @throws XMLConfigurationException
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		Normalizer norm;

		try {
			norm = new Normalizer(
					xattribs.getString(XML_ID_ATTRIBUTE),					
					xattribs.getStringEx(XML_TRANSFORM_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF), 
					xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
					xattribs.getStringEx(XML_TRANSFORMURL_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
            if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
            	norm.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
            }

			norm.setTransformationParameters(xattribs.attributes2Properties(
					new String[] { XML_ID_ATTRIBUTE,
							XML_TRANSFORM_ATTRIBUTE,
							XML_TRANSFORMCLASS_ATTRIBUTE, }));
			if (xattribs.exists(XML_ERROR_ACTIONS_ATTRIBUTE)){
				norm.setErrorActions(xattribs.getString(XML_ERROR_ACTIONS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_ERROR_LOG_ATTRIBUTE)){
				norm.setErrorLog(xattribs.getStringEx(XML_ERROR_LOG_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF));
			}
			return norm;
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}
	
	public void setErrorLog(String errorLog) {
		this.errorLogURL = errorLog;
	}

	public void setErrorActions(String string) {
		this.errorActionsString = string;		
	}

	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);

		if (xformClass != null) {
			xmlElement.setAttribute(XML_TRANSFORMCLASS_ATTRIBUTE, xformClass);
		} 

		if (xform!=null){
			xmlElement.setAttribute(XML_TRANSFORM_ATTRIBUTE,xform);
		}
		if (xformURL != null) {
			xmlElement.setAttribute(XML_TRANSFORMURL_ATTRIBUTE, xformURL);
		}
		
		if (charset != null){
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charset);
		}

		if (errorActionsString != null){
			xmlElement.setAttribute(XML_ERROR_ACTIONS_ATTRIBUTE, errorActionsString);
		}
		
		if (errorLogURL != null){
			xmlElement.setAttribute(XML_ERROR_LOG_ATTRIBUTE, errorLogURL);
		}
		Enumeration<?> propertyAtts = transformationParameters.propertyNames();
		while (propertyAtts.hasMoreElements()) {
			String attName = (String)propertyAtts.nextElement();
			xmlElement.setAttribute(attName,transformationParameters.getProperty(attName));
		}
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
		norm.reset();
        if (errorLogURL != null) {
        	try {
				errorLog = new FileWriter(FileUtils.getFile(getGraph().getProjectURL(), errorLogURL));
			} catch (IOException e) {
				throw new ComponentNotReadyException(this, XML_ERROR_LOG_ATTRIBUTE, e.getMessage());
			}
        }
	}

	@Override
	public synchronized void free()  {
		super.free();
	}

}
