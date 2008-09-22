/*
*    jETeL/Clover - Java based ETL application framework.
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
import java.util.Enumeration;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.denormalize.RecordDenormalize;
import org.jetel.component.denormalize.RecordDenormalizeTL;
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
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.compile.DynamicJavaCode;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
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
	
	private static final int IN_PORT = 0;
	private static final int OUT_PORT = 0;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DENORMALIZER";

	private static final Pattern PATTERN_CLASS = Pattern.compile("class\\s+\\w+");
	private static final Pattern PATTERN_TL_CODE = Pattern.compile("function\\s+transform");
	
	private static final int TRANSFORM_JAVA_SOURCE = 1;
	private static final int TRANSFORM_CLOVER_TL = 2;

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
		DynamicJavaCode dynCode = new DynamicJavaCode(denormCode, this.getClass().getClassLoader());
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
        if (transObject instanceof RecordDenormalize) {
            return (RecordDenormalize)transObject;
        } else {
            throw new ComponentNotReadyException("Provided transformation class doesn't implement Recorddenormalize.");
        }
    }
		
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		inPort = getInputPort(IN_PORT);
		outPort = getOutputPort(OUT_PORT);	
		inMetadata = inPort.getMetadata();
		outMetadata = outPort.getMetadata();
		recordKey = new RecordKey(key, inMetadata);
		recordKey.init();

		if (denorm == null) {
			if (xformClass != null) {
				denorm = createDenormalizer(xformClass);
			}else if (xform == null && xformURL != null){
				xform = FileUtils.getStringFromURL(getGraph().getProjectURL(), xformURL, charset);
			}
			if (xformClass == null) {
				switch (guessTransformType(xform)) {
				case TRANSFORM_JAVA_SOURCE:
					denorm = createDenormalizerDynamic(xform);
					break;
				case TRANSFORM_CLOVER_TL:
					denorm = new RecordDenormalizeTL(logger, xform, getGraph());
					break;
				default:
					throw new ComponentNotReadyException(
							"Can't determine transformation code type at component ID :"
									+ getId());
				}
			}
		}
		if (!denorm.init(transformationParameters, inMetadata, outMetadata)) {
			throw new ComponentNotReadyException("Normalizer initialization failed: " + denorm.getMessage());
		}
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
		DataRecord prevRecord = null;
		DataRecord currentRecord = null;
		while (runIt) {
			currentRecord = inPort.readRecord(srcRecord[src]);
			if (endRun(prevRecord, currentRecord)) {
				outRecord.reset();
				if (denorm.transform(outRecord) >= 0) {
					outPort.writeRecord(outRecord);
				}else{
					logger.warn(denorm.getMessage());
				}
				denorm.clean();
			}
			if (currentRecord == null) { // no more input data
				return;
			}
			prevRecord = currentRecord;
			src^=1;
			if (denorm.append(prevRecord) < 0) {
				logger.warn(denorm.getMessage());
			}
			SynchronizeUtils.cloverYield();
		} // while
	}

	@Override
	public Result execute() throws Exception {
		try {
			processInput();
		} catch (Exception e) {
			throw e;
		}finally{
			denorm.finished();
			setEOF(OUT_PORT);
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	public String getType() {
		return COMPONENT_TYPE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 1, 1)
        		|| !checkOutputPorts(status, 1, 1)) {
        	return status;
        }

        if (getInputPort(IN_PORT).getMetadata() == null) {
        	status.add(new ConfigurationProblem("Input metadata are null.", Severity.WARNING, this, Priority.NORMAL));
        }
        
        if (getOutputPort(OUT_PORT).getMetadata() == null) {
        	status.add(new ConfigurationProblem("Input metadata are null.", Severity.WARNING, this, Priority.NORMAL));
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
					xattribs.getString(XML_TRANSFORM_ATTRIBUTE, null, false), 
					xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
					xattribs.getString(XML_TRANSFORMURL_ATTRIBUTE, null),
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
			return denorm;
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
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

		Enumeration propertyAtts = transformationParameters.propertyNames();
		while (propertyAtts.hasMoreElements()) {
			String attName = (String)propertyAtts.nextElement();
			xmlElement.setAttribute(attName,transformationParameters.getProperty(attName));
		}
	}

    /**
     * Guesses type of transformation code based on
     * code itself - looks for certain patterns within the code
     * 
     * @param transform
     * @return  guessed transformation type or -1 if can't determine
     */
    public static int guessTransformType(String transform){
      
        if (transform.indexOf(WrapperTL.TL_TRANSFORM_CODE_ID) != -1){
            // clover internal transformation language
            return TRANSFORM_CLOVER_TL;
        }
        if (PATTERN_TL_CODE.matcher(transform).find()){
            // clover internal transformation language
            return TRANSFORM_CLOVER_TL;
        }
        
        if (PATTERN_CLASS.matcher(transform).find()){
            // full java source code
            return TRANSFORM_JAVA_SOURCE;
        }
        
        return -1;
    }

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.Node#reset()
	 */
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		denorm.reset();
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#free()
	 */
	@Override
	public synchronized void free() {
		// TODO Auto-generated method stub
		super.free();
	}

}
