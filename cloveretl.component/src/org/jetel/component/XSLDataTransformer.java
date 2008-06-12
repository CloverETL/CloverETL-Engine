/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;

import org.jetel.component.transform.XSLTTransition;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

import com.sun.org.apache.xml.internal.serialize.OutputFormat.Defaults;
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
 *         (c) OpenSys (www.opensys.eu)
 * @since       May 15, 2008
 */
public class XSLDataTransformer extends Node {

	private static final String XML_XSLT_FILE_ATTRIBUTE = "xsltFile";
	private static final String XML_XSLT_ATTRIBUTE = "xslt";
    private static final String XML_MAPPING_ATTRIBUTE = "mapping";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
    
	private static final String ERR_MAPPING_NOT_FOUND = "Mapping attribute must be defined.";
	private static final String ERR_XSLT_NOT_FOUND = "XSL transformation attribute must be defined.";
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "XSL_TRANSFORMER";

	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;
	private static final String DEFAULT_CHARSET = Defaults.Encoding;
	
	private String xsltFile;
	private String xslt;
	private String mapping;
	private String charset;
	
	private XSLTTransition xsltTransition;
	
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
		charset = DEFAULT_CHARSET;
	}

	@Override
	public Result execute() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		inRecord.init();
		DataRecord outRecord;
		
		while (inRecord != null && runIt) {
			inRecord = inPort.readRecord(inRecord);
	        if((outRecord = xsltTransition.getRecord(inRecord)) != null) {
	            writeRecordBroadcast(outRecord);
	        }
		    SynchronizeUtils.cloverYield();
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
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
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		DataRecord outRecord = new DataRecord(getOutputPort(WRITE_TO_PORT).getMetadata());
		outRecord.init();

		checkAttributes();
		InputStream xsltIs = null;
		if (xsltFile != null) {
			try {
				xsltIs = Channels.newInputStream(FileUtils.getReadableChannel(getGraph().getProjectURL(), xsltFile));
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

		xsltTransition = new XSLTTransition(outRecord, mapping, xsltIs);
		xsltTransition.setInMatadata(getInputPort(READ_FROM_PORT).getMetadata());
		xsltTransition.setCharset(charset);
		xsltTransition.init();
	}

	private void checkConfig() throws ComponentNotReadyException {
		checkAttributes();
		DataRecord outRecord = new DataRecord(getOutputPort(WRITE_TO_PORT).getMetadata());
		outRecord.init();

		XSLTTransition xsltTransition = new XSLTTransition(outRecord, mapping, null);
		xsltTransition.setInMatadata(getInputPort(READ_FROM_PORT).getMetadata());
		xsltTransition.setCharset(charset);
		xsltTransition.checkConfig();
	}

	private void checkAttributes() throws ComponentNotReadyException {
		if (xsltFile == null && (xslt == null || xslt.equals(""))) throw new ComponentNotReadyException(ERR_XSLT_NOT_FOUND);
		if (mapping == null) throw new ComponentNotReadyException(ERR_MAPPING_NOT_FOUND);
	}
	
    public void reset() throws ComponentNotReadyException {
    	super.reset();
    }

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
        xmlElement.setAttribute(XML_MAPPING_ATTRIBUTE, mapping);
		if (xsltFile != null) {
			xmlElement.setAttribute(XML_XSLT_FILE_ATTRIBUTE, xsltFile);
		}
        
        if (xslt != null){
            xmlElement.setAttribute(XML_XSLT_ATTRIBUTE, xslt);
        }
	}

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		XSLDataTransformer xslTransformer;
		try {
			xslTransformer = new XSLDataTransformer(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_MAPPING_ATTRIBUTE),
					xattribs.getString(XML_XSLT_FILE_ATTRIBUTE, null),
					xattribs.getString(XML_XSLT_ATTRIBUTE, null));
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				xslTransformer.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
			}
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
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
   		 
    		if(!checkInputPorts(status, 1, 1) || !checkOutputPorts(status, 1, 1)) {
    			return status;
    		}
    		
            try {
                checkConfig();
            } catch (ComponentNotReadyException e) {
                ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
                if(!StringUtils.isEmpty(e.getAttributeName())) {
                    problem.setAttributeName(e.getAttributeName());
                }
                status.add(problem);
            } finally {
            	free();
            }
            
            return status;
       }
	
	public String getType(){
		return COMPONENT_TYPE;
	}
    
}

