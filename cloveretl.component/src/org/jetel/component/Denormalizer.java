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
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.Node.Result;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.DynamicJavaCode;
import org.jetel.util.SynchronizeUtils;
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
 *  <tr><td><b>key</b></td><td>comma-separated list of key fields used to identify input record groups.
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
	private static final String XML_KEY_ATTRIBUTE = "key";
	private static final String XML_ORDER_ATTRIBUTE = "order";
	
	private static final int IN_PORT = 0;
	private static final int OUT_PORT = 0;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DENORMALIZER";

	private static final Pattern PATTERN_CLASS = Pattern.compile("class\\s+\\w+");
	private static final Pattern PATTERN_TL_CODE = Pattern.compile("function\\s+getOutputRecord");
	
	private static final int TRANSFORM_JAVA_SOURCE = 1;
	private static final int TRANSFORM_CLOVER_TL = 2;

	private Properties transformationParameters;

	static Log logger = LogFactory.getLog(HashJoin.class);

	private InputPort inPort;
	private OutputPort outPort;
	private RecordDenormalize denorm;
	
	private DataRecordMetadata inMetadata;
	private DataRecordMetadata outMetadata;
	
	private String xformClass;
	private String xform;
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
	public Denormalizer(String id, String xform, String xformClass, String[] key, Order order) {
		super(id);
		this.xformClass = xformClass;
		this.xform = xform;
		this.key = key;
		this.order = order;
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
		DynamicJavaCode dynCode = new DynamicJavaCode(denormCode);
        dynCode.setCaptureCompilerOutput(true);
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
		super.init();
		inPort = getInputPort(IN_PORT);
		outPort = getOutputPort(OUT_PORT);	
		inMetadata = inPort.getMetadata();
		outMetadata = outPort.getMetadata();
		recordKey = new RecordKey(key, inMetadata);
		recordKey.init();

		if (xformClass != null) {
			denorm = createDenormalizer(xformClass);
		} else {
			switch (guessTransformType(xform)) {
			case TRANSFORM_JAVA_SOURCE:
				denorm = createDenormalizerDynamic(xform);
				break;
			case TRANSFORM_CLOVER_TL:
                denorm = new RecordDenormalizeTL(logger, xform);
                break;
			default:
				throw new ComponentNotReadyException(
						"Can't determine transformation code type at component ID :" + getId());
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
		DataRecord record = new DataRecord(inMetadata);
		record.init();
		DataRecord prevRecord = null;
		DataRecord currentRecord = null;
		while (runIt) {
			currentRecord = inPort.readRecord(record);
			if (endRun(prevRecord, currentRecord)) {
				outRecord.reset();
				if (!denorm.getOutputRecord(outRecord)) {
					throw new TransformException(denorm.getMessage());
				}
				outPort.writeRecord(outRecord);
			}
			if (currentRecord == null) { // no more input data
				return;
			}
			prevRecord = currentRecord.duplicate();
			if (!denorm.addInputRecord(prevRecord)) {
				throw new TransformException(denorm.getMessage());
			}
			SynchronizeUtils.cloverYield();
		} // while
	}

	@Override
	public Result execute() throws Exception {
		processInput();
		denorm.finished();
		setEOF(OUT_PORT);
		return runIt ? Node.Result.OK : Node.Result.ABORTED;
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
        //TODO
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
		return keyList == null ? new String[]{} : keyList.split(",");
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
					xattribs.getString(XML_TRANSFORM_ATTRIBUTE, null), 
					xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
					parseKeyList(xattribs.getString(XML_KEY_ATTRIBUTE, null)),
					order
					);

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

}
