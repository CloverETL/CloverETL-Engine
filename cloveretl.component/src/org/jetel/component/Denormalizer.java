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
import org.jetel.component.denormalize.CTLRecordDenormalize;
import org.jetel.component.denormalize.CTLRecordDenormalizeAdapter;
import org.jetel.component.denormalize.RecordDenormalize;
import org.jetel.component.denormalize.RecordDenormalizeTL;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ITLCompiler;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
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
import org.jetel.util.compile.DynamicJavaClass;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;

/**
 *  <h3>Denormalizer Component</h3>
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Denormalizer</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Denormalizes input records - ie composes one output record from several input records
 * with identical key using user-specified transformation.
 * The transformation is supposed to implement interface <code>RecordDenormalize</code>.
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>One input port to read the records to be denormalized.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>One output port to write results of denormalization.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DENORMALIZER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>denormalizeClass</b></td><td>name of the class to be used for normalizing data.</td></tr>
 *  <tr><td><b>denormalize</b></td><td>contains definition of transformation in Java or TransformLang.</td></tr>
 *  <tr><td><b>denormalizeURL</b></td><td>path to the file with transformation code</td></tr>
 *  <tr><td><b>charset</b><i>optional</i></td><td>encoding of extern source</td></tr>
 *  <tr><td><b>key</b></td><td>list of key fields used to identify input record groups.
 *  Each group is < sequence of input records with identical key field values.</td></tr>
 *  </tr>
 *  <tr><td><b>order</b></td><td>Describe expected order of input records. "asc" for ascending, "desc" for descending,
 *  "auto" for auto-detection, "ignore" for processing input records without order checking (this may produce unexpected
 *  results when input is not ordered).</td></tr>
 *  <tr><td><b>errorActions </b><i>optional</i></td><td>defines if graph is to stop, when denormalize functions return negative value.
 *  Available actions are: STOP or CONTINUE. For CONTINUE action, error message is logged to console or file (if errorLog attribute
 *  is specified) and for STOP there is thrown TransformExceptions and graph execution is stopped. <br>
 *  Error action can be set for each negative value (value1=action1;value2=action2;...) or for all values the same action (STOP 
 *  or CONTINUE). It is possible to define error actions for some negative values and for all other values (MIN_INT=myAction).
 *  Default value is <i>-1=CONTINUE;MIN_INT=STOP</i></td></tr>
 *  <tr><td><b>errorLog</b><br><i>optional</i></td><td>path to the error log file. Each error (after which graph continues) is logged in 
 *  following way: methodName;inputRecordNumber;errorCode;errorMessage - fields are delimited by Defaults.Component.KEY_FIELDS_DELIMITER,
 *  methodName can be <i>transform</i> or <i>append</i>.</td></tr>
 *  </table>
 *
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/15/06  
 * @see         org.jetel.data.parser.FixLenDataParser
 */
public class Denormalizer extends Node {

	enum Order {
		ASC,	// ascending order
		DESC,	// descending order
		IGNORE,	// don't check order of records
		AUTO,	// check the input to be either in ascending or descending order
	}

	private static final String XML_TRANSFORMCLASS_ATTRIBUTE = "denormalizeClass";
	private static final String XML_TRANSFORM_ATTRIBUTE = "denormalize";
	private static final String XML_TRANSFORMURL_ATTRIBUTE = "denormalizeURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_KEY_ATTRIBUTE = "key";
	private static final String XML_ORDER_ATTRIBUTE = "order";
	private static final String XML_ERROR_ACTIONS_ATTRIBUTE = "errorActions";
    private static final String XML_ERROR_LOG_ATTRIBUTE = "errorLog";
    /** the name of an XML attribute used to store the "equal NULL" flag */
    private static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";
	
	private static final int IN_PORT = 0;
	private static final int OUT_PORT = 0;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DENORMALIZER";

	private Properties transformationParameters;

	static Log logger = LogFactory.getLog(Denormalizer.class);

	private InputPort inPort;
	private OutputPort outPort;
	private RecordDenormalize denorm;
	
	private DataRecordMetadata inMetadata;
	private DataRecordMetadata outMetadata;
	
	private String xformClass;
	private String xform;
	private String xformURL = null;
	private String charset = null;
	private Order order;
	private String[] key;
	RecordKey recordKey;
		
	private String errorActionsString;
	private Map<Integer, ErrorAction> errorActions = new HashMap<Integer, ErrorAction>();
	private String errorLogURL;
	private FileWriter errorLog;

    /** the flag specifying whether the null values are considered equal or not */
    private boolean equalNULL = true;

    /**
	 * Sole ctor.
	 * @param id Component ID.
	 * @param xform Denormalization implementation source code (either Java or TransformLang).
	 * @param xformClass Denormalization class.
	 * @param key
	 * @param order
	 */
	public Denormalizer(String id, String xform, String xformClass, String xformURL, 
			String[] key, Order order) {
		super(id);
		this.xformClass = xformClass;
		this.xform = xform;
		this.xformURL = xformURL;
		this.key = key;
		this.order = order;
	}

	public Denormalizer(String id, RecordDenormalize xform, String[] key, Order order) {
		this(id, null, null, null, key, order);
		this.denorm = xform;
	}

    public void setEqualNULL(boolean equalNULL) {
        this.equalNULL = equalNULL;
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
	 * Creates denormalization instance using specified class.
	 * @param denormClass
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private RecordDenormalize createDenormalizer(String denormClass) throws ComponentNotReadyException {
		RecordDenormalize denorm;
        try {
            denorm =  (RecordDenormalize)Class.forName(denormClass).newInstance();
        }catch (InstantiationException ex){
            throw new ComponentNotReadyException("Can't instantiate transformation class: "+ex.getMessage());
        }catch (IllegalAccessException ex){
            throw new ComponentNotReadyException("Can't instantiate transformation class: "+ex.getMessage());
        }catch (ClassNotFoundException ex) {
            throw new ComponentNotReadyException("Can't find specified transformation class: " + xformClass);
        }
		return denorm;
	}

	/**
	 * Creates normalization instance using given Java source.
	 * @param denormCode
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private RecordDenormalize createDenormalizerDynamic(String denormCode) throws ComponentNotReadyException {
        Object transObject = DynamicJavaClass.instantiate(denormCode, this.getClass().getClassLoader(),
        		getGraph().getRuntimeContext().getClassPathsUrls());

        if (transObject instanceof RecordDenormalize) {
			return (RecordDenormalize) transObject;
        }

        throw new ComponentNotReadyException("Provided transformation class doesn't implement RecordDenormalize.");
    }
		
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		inPort = getInputPort(IN_PORT);
		outPort = getOutputPort(OUT_PORT);	
		inMetadata = inPort.getMetadata();
		outMetadata = outPort.getMetadata();
		recordKey = new RecordKey(key, inMetadata);
		recordKey.setEqualNULLs(equalNULL);
		recordKey.init();

		if (denorm == null) {
			if (xformClass != null) {
				denorm = createDenormalizer(xformClass);
			}else if (xform == null && xformURL != null){
				xform = FileUtils.getStringFromURL(getGraph().getRuntimeContext().getContextURL(), xformURL, charset);
			}
			if (xformClass == null) {
				switch (RecordTransformFactory.guessTransformType(xform)) {
				case RecordTransformFactory.TRANSFORM_JAVA_SOURCE:
					denorm = createDenormalizerDynamic(xform);
					break;
				case RecordTransformFactory.TRANSFORM_CLOVER_TL:
					denorm = new RecordDenormalizeTL(logger, xform, getGraph());
					break;
					
				case RecordTransformFactory.TRANSFORM_CTL:
					ITLCompiler compiler = TLCompilerFactory.createCompiler(getGraph(),new DataRecordMetadata[]{inMetadata}, new DataRecordMetadata[]{outMetadata},"UTF-8");
	            	List<ErrorMessage> msgs = compiler.compile(xform, CTLRecordDenormalize.class,getId());
	            	if (compiler.errorCount() > 0) {
	            		for (ErrorMessage msg : msgs) {
	            			logger.error(msg.toString());
	            		}
	            		throw new ComponentNotReadyException("CTL code compilation finished with " + compiler.errorCount() + " errors");
	            	}
	            	Object ret = compiler.getCompiledCode();
	            	if (ret instanceof TransformLangExecutor) {
	            		// setup interpreted runtime
	            		denorm = new CTLRecordDenormalizeAdapter((TransformLangExecutor)ret, logger);
	            	} else if (ret instanceof CTLRecordDenormalize){
	            		denorm = (CTLRecordDenormalize)ret;
	            	} else {
	            		// this should never happen as compiler always generates correct interface
	            		throw new ComponentNotReadyException("Invalid type of record transformation");
	            	}
					break;
				default:
					throw new ComponentNotReadyException(
							"Can't determine transformation code type at component ID :"
									+ getId());
				}
			}
			// set graph to transformation (if CTL it can use lookups etc.)
			denorm.setGraph(getGraph());
		}
		if (!denorm.init(transformationParameters, inMetadata, outMetadata)) {
			throw new ComponentNotReadyException("Normalizer initialization failed: " + denorm.getMessage());
		}
        errorActions = ErrorAction.createMap(errorActionsString);
	}

	/**
	 * Checks for end of run (ie sequence of keys with identical key fields values).
	 * @param prevRecord
	 * @param currentRecord
	 * @return true on end of run (when key fields values differ), false otherwise
	 * @throws TransformException 
	 * @throws JetelException Indicates that input is not sorted as expected.
	 */
	private boolean endRun(DataRecord prevRecord, DataRecord currentRecord) throws TransformException {
		if (prevRecord == null) {
			return false;
		}
		if (currentRecord == null) {
			return true;
		}

		int cmpResult = recordKey.compare(prevRecord, currentRecord);

		if (cmpResult == 0) {
			return false;
		}
		if (order == Order.IGNORE) {
			return true;
		}
		if ((order == Order.ASC && cmpResult == -1) || order == Order.DESC && cmpResult == 1) {
			return true;
		}
		if (order == Order.AUTO) {
			order = cmpResult == -1 ? Order.ASC : Order.DESC;
			return true;
		}
		throw new TransformException("Input is not sorted as specified by component attributes");
	}

	/**
	 * Processes all input records.
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws TransformException
	 */
	private void processInput() throws IOException, InterruptedException, TransformException {
		DataRecord outRecord = new DataRecord(outMetadata);
		outRecord.init();
		DataRecord srcRecord[] = new DataRecord[] {new DataRecord(inMetadata),new DataRecord(inMetadata)} ;
		srcRecord[0].init();
		srcRecord[1].init();
		int src=0;
		int counter = 0;
		DataRecord prevRecord = null;
		DataRecord currentRecord = null;
		int transformResult;
		while (runIt) {
			currentRecord = inPort.readRecord(srcRecord[src]);
			if (endRun(prevRecord, currentRecord)) {
				outRecord.reset();
				transformResult = denorm.transform(outRecord);
				if (transformResult >= 0) {
					outPort.writeRecord(outRecord);
				}else{
					handleException("transform", transformResult, counter);
				}
				denorm.clean();
			}
			if (currentRecord == null) { // no more input data
				return;
			}
			counter++;
			prevRecord = currentRecord;
			src^=1;
			transformResult = denorm.append(prevRecord);
			if (transformResult < 0) {
				handleException("append", transformResult, counter);
			}
			SynchronizeUtils.cloverYield();
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
			denorm.getMessage();
		if (action == ErrorAction.CONTINUE) {
			if (errorLog != null){
				errorLog.write(functionName);
				errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
				errorLog.write(String.valueOf(recNo));
				errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
				errorLog.write(String.valueOf(transformResult));
				errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
				message = denorm.getMessage();
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
    	if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	}
    	else {
    		denorm.reset();
    	}
        if (errorLogURL != null) {
        	try {
				errorLog = new FileWriter(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), errorLogURL));
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
			denorm.finished();

		    if (errorLog != null){
				errorLog.flush();
			}

		    setEOF(OUT_PORT);
		}

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

    
    @Override
    public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException {
    	super.postExecute(transactionMethod);
    	
    	try {
		    if (errorLog != null) {
				errorLog.close();
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
				FileUtils.canWrite(getGraph().getRuntimeContext().getContextURL(), errorLogURL);
			} catch (ComponentNotReadyException e) {
				status.add(new ConfigurationProblem(e, Severity.WARNING, this, Priority.NORMAL, XML_ERROR_LOG_ATTRIBUTE));
			}
        }

//        try {
//            init();
//            free();
//        } catch (ComponentNotReadyException e) {
//            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
//            if(!StringUtils.isEmpty(e.getAttributeName())) {
//                problem.setAttributeName(e.getAttributeName());
//            }
//            status.add(problem);
//        }
        
        // transformation source for checkconfig
        String checkTransform = null;
        if (xform != null) {
        	checkTransform = xform;
        } else if (xformURL != null) {
        	checkTransform = FileUtils.getStringFromURL(getGraph().getRuntimeContext().getContextURL(), xformURL, charset);
        }
        // only the transform and transformURL parameters are checked, transformClass is ignored
        if (checkTransform != null) {
        	int transformType = RecordTransformFactory.guessTransformType(checkTransform);
        	if (transformType != RecordTransformFactory.TRANSFORM_JAVA_SOURCE ) {
        		// only CTL is checked
        		
        		InputPort inPort = getInputPort(IN_PORT);
        		OutputPort outPort = getOutputPort(OUT_PORT);	
        		DataRecordMetadata inMetadata = inPort.getMetadata();
        		DataRecordMetadata outMetadata = outPort.getMetadata();

    			try {
    				RecordDenormalizeTL denorm = new RecordDenormalizeTL(logger, checkTransform, getGraph());
    				denorm.init(transformationParameters, inMetadata, outMetadata);
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
	 * Sets denormalization parameters.
	 * @param transformationParameters
	 */
	private void setTransformationParameters(Properties transformationParameters) {
		this.transformationParameters = transformationParameters;
	}

	private static String[] parseKeyList(String keyList) {
		return keyList == null ? new String[]{} : keyList.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
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
		Denormalizer denorm;

		Order order = Order.AUTO;

		String orderString = xattribs.getString(XML_ORDER_ATTRIBUTE, "auto");
		if (orderString.compareToIgnoreCase("asc") == 0) {
			order = Order.ASC;
		} else if (orderString.compareToIgnoreCase("desc") == 0) {
			order = Order.DESC;
		} else if (orderString.compareToIgnoreCase("ignore") == 0) {
			order = Order.IGNORE;
		} else if (orderString.compareToIgnoreCase("auto") == 0) {
			order = Order.AUTO;
		} else {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + "unknown input order: '" + orderString + "'");				
		}

		try {
			denorm = new Denormalizer(
					xattribs.getString(XML_ID_ATTRIBUTE),					
					xattribs.getStringEx(XML_TRANSFORM_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF), 
					xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
					xattribs.getStringEx(XML_TRANSFORMURL_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF),
					parseKeyList(xattribs.getString(XML_KEY_ATTRIBUTE, null)),
					order
					);
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				denorm.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
			}

			denorm.setTransformationParameters(xattribs.attributes2Properties(
					new String[] { XML_ID_ATTRIBUTE,
							XML_TRANSFORM_ATTRIBUTE,
							XML_TRANSFORMCLASS_ATTRIBUTE, }));
			if (xattribs.exists(XML_ERROR_ACTIONS_ATTRIBUTE)){
				denorm.setErrorActions(xattribs.getString(XML_ERROR_ACTIONS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_ERROR_LOG_ATTRIBUTE)){
				denorm.setErrorLog(xattribs.getString(XML_ERROR_LOG_ATTRIBUTE));
			}

			denorm.setEqualNULL(xattribs.getBoolean(XML_EQUAL_NULL_ATTRIBUTE, true));

			return denorm;
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

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
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
		String orderString = "";
		if (order == Order.ASC) {
			orderString = "asc";
		} else if (order == Order.DESC) {
			orderString = "desc";
		} if (order == Order.IGNORE) {
			orderString = "ignore";
		} if (order == Order.AUTO) {
			orderString = "auto";
		}		
		xmlElement.setAttribute(XML_TRANSFORM_ATTRIBUTE, orderString);

		StringBuilder keyList = new StringBuilder();
		for (int i = 0; true; i++) {
			keyList.append(key[i]);
			if (i >= key.length) {
				break;
			}
			keyList.append(",");
		}
		if (xform!=null){
			xmlElement.setAttribute(XML_KEY_ATTRIBUTE,xform);
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

        if (!equalNULL) {
            xmlElement.setAttribute(XML_EQUAL_NULL_ATTRIBUTE, Boolean.toString(equalNULL));
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
	}

}
