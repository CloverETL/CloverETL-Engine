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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.jetel.component.transform.XSLTFormatter;
import org.jetel.component.transform.XSLTMappingTransition;
import org.jetel.component.transform.XSLTransformer;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.graph.runtime.tracker.ReformatComponentTokenTracker;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.ReadableChannelIterator;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.TargetFile;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;
/**
 *  <h3>Sort Component</h3>
 *
 * <!-- Sorts the incoming records based on specified key -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Sort</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Performs XSL transformation over a String data.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0]- output records</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"XSL_TRANSFORMER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>xsltFile</b><br><i>if no xslt</i></td><td>XSLT file needed for transformation</td>
 *  <tr><td><b>xslt</b><br><i>if no xsltFile</i></td><td>XSLT needed for transformation</td></tr>
 *  <tr><td><b>mapping</b></td><td>transformation mapping separated by ; {semicolon}</td></tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="SORT_CUSTOMER" type="SORT" sortKey="Name:Address" sortOrder="A"/&gt;</pre>
 *  
 *  <pre>&lt;Node id="SORT_CUSTOMER" type="SORT" sortKey="Name:Address" sortOrder="A" useI18N="true" locale="fr"/&gt;</pre>
 *
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 * @since       May 15, 2008
 */
public class XSLDataTransformer extends Node {

	private static final String XML_XML_INPUT_FILE_ATTRIBUTE = "xmlInputFile";
	private static final String XML_XML_OUTPUT_FILE_ATTRIBUTE = "xmlOutputFile";
	private static final String XML_XSLT_FILE_ATTRIBUTE = "xsltFile";
	private static final String XML_XSLT_ATTRIBUTE = "xslt";
    private static final String XML_MAPPING_ATTRIBUTE = "mapping";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
    
	private static final String ERR_MAPPING_FILE_NOT_FOUND = "Mapping attribute or xml file attributes must be defined.";
	private static final String ERR_XSLT_NOT_FOUND = "XSL transformation attribute must be defined.";
	private static final String ERR_INPUT_PORT_NOT_FOUND = "Input port must be connected.";
	private static final String ERR_OUTPUT_PORT_NOT_FOUND = "Output port must be connected.";
	private static final String ERR_INPUT_PORT_FOUND = "Input port doesn't have to be connected.";
	private static final String ERR_OUTPUT_PORT_FOUND = "Output port doesn't have to be connected.";
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "XSL_TRANSFORMER";

	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;
	
	private String xmlInputFile;
	private String xmlOutputFile;
	private String xsltFile;
	private String xslt;
	private String mapping;
	private String charset;
	
	private XSLTMappingTransition xsltMappingTransition;
	private ReadableChannelIterator channelIterator;
	private TargetFile currentTarget;
	private XSLTransformer transformer;
	private InputStream xsltIs; //a stream used in xsltMappingTransition and transformer

	/**
	 * Constructor for the XSLTransformer object. 
	 *
	 * @param  id         Description of the Parameter
	 * @param  mapping    Transformation mapping separated by ; {semicolon}
	 * @param  xsltFile   XSLT file needed for transformation
	 * @param  xslt       XSLT needed for transformation
	 */
	public XSLDataTransformer(String id, String mapping, String xsltFile, String xslt) {
		super(id);
		this.mapping = mapping;
		this.xsltFile = xsltFile;
		this.xslt = xslt;
		charset = XSLTransformer.DEFAULT_CHARSET;
	}

	/**
	 * Constructor for the XSLTransformer object. 
	 *
	 * @param  id              Description of the Parameter
	 * @param  xmlInputFile    XML input file
	 * @param  xmlOutputFile    XML output file
	 * @param  xsltFile        XSLT file needed for transformation
	 * @param  xslt            XSLT needed for transformation
	 */
	public XSLDataTransformer(String id, String xmlInputFile, String xmlOutputFile, String xsltFile, String xslt) {
		super(id);
		this.xmlInputFile = xmlInputFile;
		this.xmlOutputFile = xmlOutputFile;
		this.xsltFile = xsltFile;
		this.xslt = xslt;
		charset = XSLTransformer.DEFAULT_CHARSET;
	}

	@Override
	//MAINTAINS RESOURCES:
	// * xsltFile (xsltIs - read),
	// * xmlInputFile (channelIterator - read),
	// * xmlOutputFile (currentTarget - write)
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		
		InputPort inputPort = getInputPort(READ_FROM_PORT);
		OutputPort outputPort = getOutputPort(WRITE_TO_PORT);

		DataRecord outRecord = null;
		if (outputPort != null) {
			outRecord = DataRecordFactory.newRecord(outputPort.getMetadata());
			outRecord.init();
		}

		xsltIs = null;
		if (xsltFile != null) {
			try {
				xsltIs = Channels.newInputStream(FileUtils.getReadableChannel(getGraph().getRuntimeContext().getContextURL(), xsltFile));
			} catch (RuntimeException e) {
				if (xslt == null || xslt.equals("")) throw e;
			} catch (IOException e) {
				throw new ComponentNotReadyException(e);
			}
		} else {
			try {
				xsltIs = new ByteArrayInputStream(xslt.getBytes(charset));
			} catch (UnsupportedEncodingException e) {
				throw new ComponentNotReadyException(e);
			}
		}

		if (mapping != null) {
			xsltMappingTransition = new XSLTMappingTransition(outRecord, mapping, xsltIs);
			xsltMappingTransition.setInMatadata(inputPort.getMetadata());
			xsltMappingTransition.setCharset(charset);
			xsltMappingTransition.init();
		} else {
			initChannelIterator();
			initTarget();
			initTransformer(xsltIs);
		}
	}



	
	@Override
	public Result execute() throws Exception {
		if (xsltMappingTransition != null) {
			// transformation for the mapping attribute
			return executeMapping();
		}
		else {
			//transformation for file attributes
			return executeFiles();
		}
	}
	
	public Result executeFiles() throws Exception {
		try {
			ReadableByteChannel readableByteChannel;
			XSLTFormatter formatter;
			WritableByteChannel writableByteChannel;
			boolean next = false;
			
			while (channelIterator.hasNext() && (readableByteChannel = channelIterator.nextChannel()) != null) {
				if (next) currentTarget.setNextOutput(); else next = true;
				formatter = (XSLTFormatter)currentTarget.getFormatter();
				writableByteChannel = formatter.getWritableByteChannel();
				
				transformer.transform(Channels.newInputStream(readableByteChannel), Channels.newOutputStream(writableByteChannel));
				readableByteChannel.close();
			}
		} catch (JetelException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		} finally {
			currentTarget.finish();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	public Result executeMapping() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord inRecord = DataRecordFactory.newRecord(inPort.getMetadata());
		inRecord.init();
		DataRecord outRecord;
		
		while (inRecord != null && runIt) {
			inRecord = inPort.readRecord(inRecord);
	        if((outRecord = xsltMappingTransition.getRecord(inRecord)) != null) {
	            writeRecordBroadcast(outRecord);
	        }
		    SynchronizeUtils.cloverYield();
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		try {
			xsltIs.close(); //closing XSLT opened in xsltMappingTransition or transformer
			//files opened by channelIterator are closed in execute()
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}

	
	@Override
	public void free() {
        if(!isInitialized()) return;
		super.free();
	}
	
	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();

		checkAttributes();
	}

    private void initChannelIterator() throws ComponentNotReadyException {
    	TransformationGraph graph = getGraph();
    	channelIterator = new ReadableChannelIterator(getInputPort(READ_FROM_PORT), graph != null ? graph.getRuntimeContext().getContextURL() : null, xmlInputFile);
    	channelIterator.setCharset(charset);
    	channelIterator.setDictionary(graph != null ? graph.getDictionary() : null);
    	channelIterator.init();
    }

    /**
     * Creates target array or target map.
     * 
     * @throws ComponentNotReadyException
     */
    private void initTarget() throws ComponentNotReadyException {
    	// prepare type of targets: lookpup/keyValue
		try {
			InputPort inputPort = getInputPort(READ_FROM_PORT);
	    	currentTarget = new TargetFile(xmlOutputFile, getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, 
	    			new XSLTFormatter(), inputPort == null ? null : inputPort.getMetadata());
			currentTarget.setAppendData(false);
			currentTarget.setUseChannel(true);
			currentTarget.setCharset(charset);
			currentTarget.setOutputPort(getOutputPort(WRITE_TO_PORT));
			currentTarget.setDictionary(getGraph().getDictionary());
			currentTarget.init();
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
    }
    
	/**
	 * Initializes xslt transition.
	 * 
	 * @throws ComponentNotReadyException
	 */
	public void initTransformer(InputStream xsltIs) throws ComponentNotReadyException {
		transformer = new XSLTransformer();
		transformer.setCharset(charset);
		try {
			transformer.init(xsltIs);
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
	}

    private void checkConfig() throws ComponentNotReadyException {
		checkAttributes();
		
		InputPort inputPort = getInputPort(READ_FROM_PORT);
		OutputPort outputPort = getOutputPort(WRITE_TO_PORT);
		
		// check for mapping attribute
		if (mapping != null) {
			if (inputPort == null) throw new ComponentNotReadyException(ERR_INPUT_PORT_NOT_FOUND);
			if (outputPort == null) throw new ComponentNotReadyException(ERR_OUTPUT_PORT_NOT_FOUND);
			
			DataRecord outRecord = DataRecordFactory.newRecord(outputPort.getMetadata());
			outRecord.init();
			
			XSLTMappingTransition xsltTransition = new XSLTMappingTransition(outRecord, mapping, null);
			xsltTransition.setInMatadata(inputPort.getMetadata());
			xsltTransition.setCharset(charset);
			xsltTransition.checkConfig();
			return;
		}
		
		// check for file attributes
		if (xmlInputFile.startsWith("port:")) {
			if (inputPort == null) throw new ComponentNotReadyException(ERR_INPUT_PORT_NOT_FOUND);
		} else {
			if (inputPort != null) throw new ComponentNotReadyException(ERR_INPUT_PORT_FOUND);
		}
		// check for file attributes
		if (xmlOutputFile.startsWith("port:")) {
			if (outputPort == null) throw new ComponentNotReadyException(ERR_OUTPUT_PORT_NOT_FOUND);
		} else {
			if (outputPort != null) throw new ComponentNotReadyException(ERR_OUTPUT_PORT_FOUND);
		}
	}

	private void checkAttributes() throws ComponentNotReadyException {
		if (xsltFile == null && (xslt == null || xslt.equals(""))) throw new ComponentNotReadyException(ERR_XSLT_NOT_FOUND);
		if (mapping == null && (xmlInputFile == null || xmlOutputFile == null)) throw new ComponentNotReadyException(ERR_MAPPING_FILE_NOT_FOUND);
	}
	
    @Override
	public void reset() throws ComponentNotReadyException {
    	super.reset();
    }

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 */
	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
        xmlElement.setAttribute(XML_MAPPING_ATTRIBUTE, mapping);
		if (xsltFile != null) {
			xmlElement.setAttribute(XML_XSLT_FILE_ATTRIBUTE, xsltFile);
		}
        if (xslt != null){
            xmlElement.setAttribute(XML_XSLT_ATTRIBUTE, xslt);
        }
        if (xmlInputFile != null){
            xmlElement.setAttribute(XML_XML_INPUT_FILE_ATTRIBUTE, xmlInputFile);
        }
        if (xmlOutputFile != null){
            xmlElement.setAttribute(XML_XML_OUTPUT_FILE_ATTRIBUTE, xmlOutputFile);
        }
        if (charset != null){
            xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charset);
        }
	}

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @throws AttributeNotFoundException 
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		XSLDataTransformer xslTransformer;
		if (xattribs.exists(XML_MAPPING_ATTRIBUTE)) {
			xslTransformer = new XSLDataTransformer(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_MAPPING_ATTRIBUTE),
					xattribs.getStringEx(XML_XSLT_FILE_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF),
					xattribs.getString(XML_XSLT_ATTRIBUTE, null));
		} else {
			xslTransformer = new XSLDataTransformer(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getStringEx(XML_XML_INPUT_FILE_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF),
					xattribs.getStringEx(XML_XML_OUTPUT_FILE_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF),
					xattribs.getStringEx(XML_XSLT_FILE_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF),
					xattribs.getString(XML_XSLT_ATTRIBUTE, null));
		}
		if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
			xslTransformer.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
		}
		return xslTransformer;
	}


	private void setCharset(String charset) {
		this.charset = charset;
	}

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
        @Override
        public ConfigurationStatus checkConfig(ConfigurationStatus status) {
    		super.checkConfig(status);
   		 
    		if(!checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 0, 1)) {
    			return status;
    		}
    		
            try {
                checkConfig();
            } catch (ComponentNotReadyException e) {
                ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
                if(!StringUtils.isEmpty(e.getAttributeName())) {
                    problem.setAttributeName(e.getAttributeName());
                }
                status.add(problem);
            } finally {
            	free();
            }
            
            return status;
       }
	
	@Override
	public String getType(){
		return COMPONENT_TYPE;
	}
    
	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new ReformatComponentTokenTracker(this);
	}
	
}

