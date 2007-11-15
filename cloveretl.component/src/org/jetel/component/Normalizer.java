/*
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
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Enumeration;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.normalize.RecordNormalize;
import org.jetel.component.normalize.RecordNormalizeTL;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.compile.DynamicJavaCode;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
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
	
	private static final int IN_PORT = 0;
	private static final int OUT_PORT = 0;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "NORMALIZER";

	private static final Pattern PATTERN_CLASS = Pattern.compile("class\\s+\\w+");
	private static final Pattern PATTERN_TL_CODE = Pattern.compile("function\\s+transform");
	
	private static final int TRANSFORM_JAVA_SOURCE = 1;
	private static final int TRANSFORM_CLOVER_TL = 2;
	
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
        if (transObject instanceof RecordNormalize) {
            return (RecordNormalize)transObject;
        } else {
            throw new ComponentNotReadyException("Provided transformation class doesn't implement RecordNormalize.");
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

		if (norm == null) {
			if (xformClass != null) {
				norm = createNormalizer(xformClass);
			}else if (xform == null) {
				xform = FileUtils.getStringFromURL(getGraph().getRuntimeParameters().getProjectURL(), xformURL, charset);
			}
			if (xformClass == null) {
				switch (guessTransformType(xform)) {
				case TRANSFORM_JAVA_SOURCE:
					norm = createNormalizerDynamic(xform);
					break;
				case TRANSFORM_CLOVER_TL:
					norm = new RecordNormalizeTL(logger, xform);
					break;
				default:
					throw new ComponentNotReadyException(
							"Can't determine transformation code type at component ID :"
									+ getId());
				}
			}
		}
		if (!norm.init(transformationParameters, inMetadata, outMetadata)) {
			throw new ComponentNotReadyException("Normalizer initialization failed: " + norm.getMessage());
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
		while (runIt) {
			if (inPort.readRecord(inRecord) == null) { // no more input data
				return;
			}
			for (int idx = 0; idx < norm.count(inRecord); idx++) {
				if (norm.transform(inRecord, outRecord, idx)) {
					outPort.writeRecord(outRecord);
					outRecord.reset();
				}else{
					logger.warn(norm.getMessage());
				}
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
			norm.finished();
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
		 
		checkInputPorts(status, 1, 1);
        checkOutputPorts(status, 1, 1);

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
					xattribs.getString(XML_TRANSFORM_ATTRIBUTE, null, false), 
					xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
					xattribs.getString(XML_TRANSFORMURL_ATTRIBUTE, null));
            if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
            	norm.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
            }

			norm.setTransformationParameters(xattribs.attributes2Properties(
					new String[] { XML_ID_ATTRIBUTE,
							XML_TRANSFORM_ATTRIBUTE,
							XML_TRANSFORMCLASS_ATTRIBUTE, }));
			return norm;
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
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

}
