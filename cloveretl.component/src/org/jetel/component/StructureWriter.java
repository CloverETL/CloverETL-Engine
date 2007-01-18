
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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
import java.nio.channels.WritableByteChannel;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.StructureFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ByteBufferUtils;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>StructureWriter Component</h3>
 *
 * <!-- All records from input port [0] are formatted due to given mask and written to specified file -->
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>StructureWriter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records from input port [0] are formatted due to given mask and written to specified file.
 * Records can be preceded by some text (header) or be trailed by a text (footer)</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td>This component uses java.nio.* classes.</td></tr>
 * </table>
 *  <br>  
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"STRUCTURE_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>Output files mask.
 *  Use wildcard '#' to specify where to insert sequential number of file. Number of consecutive wildcards specifies
 *  minimal length of the number. Name without wildcard specifies only one file.</td>
 *  <tr><td><b>charset</b></td><td>character encoding of the output file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>append</b></td><td>whether to append data at the end if output file exists or replace it (values: true/false)</td>
 *  <tr><td><b>mask</b></td><td>template for formating records. Every occurrence 
 *  of $fieldName will be replaced by value of the fieldName. The rest of text will
 *  be unchanged. If not given there is used default mask:
 *  &lt; recordName field1=$field1 field2=$field2 ... fieldn=$fieldn /&gt;
 *  where field1 ,.., fieldn are record's fields from metadata</td>
 *  <tr><td><b>header</b></td><td>text to write before records</td>
 *  <tr><td><b>footer</b></td><td>text to write after records</td>
 *  </tr>
 *  </table>  
 *
 * <h4>Example:</h4>
 * <pre>&lt;Node append="true" fileURL="${WORKSPACE}/output/structured_customers.txt"
 *  id="STRUCTURE_WRITER0" type="STRUCTURE_WRITER"&gt;
 * &lt;attr name="header"&gt;dir = ${WORKSPACE}&lt;/attr&gt;
 * &lt;attr name="mask"&gt;
 * &lt;Customer id=$customer_id&gt;
 * 	&lt;last name = $lname&gt;
 *	&lt;first name = $fname&gt;
 * &lt;/Customer&gt;
 * &lt;/attr&gt;
 * &lt;attr name="footer"&gt;end of file&lt;/attr&gt;
 * &lt;/Node&gt;
 * 
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 30, 2006
 *
 */
public class StructureWriter extends Node {

	public static final String XML_APPEND_ATTRIBUTE = "append";
	public static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	public static final String XML_CHARSET_ATTRIBUTE = "charset";
	public static final String XML_MASK_ATTRIBUTE = "mask";
	public static final String XML_HEADER_ATTRIBUTE = "header";
	public static final String XML_FOOTER_ATTRIBUTE = "footer";

	private String fileURL;
	private boolean appendData;
	private StructureFormatter formatter;
	private String header = null;
	private String footer = null;
	private WritableByteChannel writer;
	private ByteBuffer buffer;
	private String charset;

	public final static String COMPONENT_TYPE = "STRUCTURE_WRITER";
	private final static int READ_FROM_PORT = 0;

	/**
	 * Constructor
	 * 
	 * @param id
	 * @param fileURL
	 * @param charset
	 * @param appendData
	 * @param mask
	 */
	public StructureWriter(String id, String fileURL, String charset, 
			boolean appendData, String mask) {
		super(id);
		this.fileURL = fileURL;
		this.appendData = appendData;
		this.charset = charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER;
		formatter = charset == null ? new StructureFormatter() : 
			new StructureFormatter(charset);
		formatter.setMask(mask);
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	@Override
	public Result execute() throws Exception {
		//write header
		if (header != null ){
			buffer.put(header.getBytes(charset));
			ByteBufferUtils.flush(buffer,writer);
		}
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		record.init();
		//write records
		try {
			while (record != null && runIt) {
				record = inPort.readRecord(record);
				if (record != null) {
					formatter.write(record);
				}
				SynchronizeUtils.cloverYield();
			}
			formatter.flush();
			//write footer
			if (footer != null ){
				buffer.clear();
				buffer.put(footer.getBytes(charset));
				ByteBufferUtils.flush(buffer,writer);
			}
		} catch (Exception e) {
			throw e;
		}finally{
			//close output
			writer.close();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public void free() {
		super.free();
		formatter.close();
	}
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
		checkInputPorts(status, 1, 1);
        checkOutputPorts(status, 0, 0);

        try {
            init();
            free();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        }
        
        return status;
    }
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		// based on file mask, create/open output file
		try {
			writer = FileUtils.getWritableChannel(getGraph().getProjectURL(), fileURL, appendData);
			buffer = ByteBuffer.allocateDirect(StringUtils.getMaxLength(header,footer));
			formatter.init(getInputPort(READ_FROM_PORT).getMetadata());
            formatter.setDataTarget(writer);
		} catch (IOException ex) {
			throw new ComponentNotReadyException(getId() + "IOError: " + ex.getMessage());
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#fromXML(org.jetel.graph.TransformationGraph, org.w3c.dom.Element)
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		StructureWriter aDataWriter = null;
		
		try{
			aDataWriter = new StructureWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
									xattribs.getString(XML_FILEURL_ATTRIBUTE),
									xattribs.getString(XML_CHARSET_ATTRIBUTE,null),
									xattribs.getBoolean(XML_APPEND_ATTRIBUTE, false),
									xattribs.getString(XML_MASK_ATTRIBUTE,null));
			if (xattribs.exists(XML_HEADER_ATTRIBUTE)){
				aDataWriter.setHeader(xattribs.getString(XML_HEADER_ATTRIBUTE));
			}
			if (xattribs.exists(XML_FOOTER_ATTRIBUTE)){
				aDataWriter.setFooter(xattribs.getString(XML_FOOTER_ATTRIBUTE));
			}
		}catch(Exception ex){
			System.err.println(COMPONENT_TYPE + ":" + xattribs.getString(Node.XML_ID_ATTRIBUTE,"unknown ID") + ":" + ex.getMessage());
			return null;
		}
		
		return aDataWriter;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	public void toXML(org.w3c.dom.Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILEURL_ATTRIBUTE,this.fileURL);
		String charSet = this.formatter.getCharsetName();
		if (charSet != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, this.formatter.getCharsetName());
		}
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(this.appendData));
		if (header != null){
			xmlElement.setAttribute(XML_HEADER_ATTRIBUTE,header);
		}
		if (footer != null){
			xmlElement.setAttribute(XML_FOOTER_ATTRIBUTE,footer);
		}
	}
	
	public void setFooter(String footer) {
		this.footer = footer;
	}

	public void setHeader(String header) {
		this.header = header;
	}
	
}
