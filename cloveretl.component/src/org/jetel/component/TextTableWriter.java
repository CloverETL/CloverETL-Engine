
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
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.TextTableFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>TextTableWriter Component</h3>
 *
 * <!-- All records from input port [0] are formatted to table and written to specified file or on screen-->
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>TextTableWriter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records from input port [0] are formatted to table and written to specified file or on screen.</td></tr>
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
 *  <tr><td><b>recordSkip</b></td><td>number of skipped records</td>
 *  <tr><td><b>recordCount</b></td><td>number of written records</td>
 *  </tr>
 *  </table>  
 *
 * <h4>Example:</h4>
 * <pre>&lt;Node append="true" fileURL="${WORKSPACE}/output/structured_customers.txt"
 *  id="TEXT_TABLE_WRITER0" type="TEXT_TABLE_WRITER"&gt;
 * &lt;/Node&gt;
 * 
 * 
 * @author ausperger; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Feb 6, 2007
 *
 */
public class TextTableWriter extends Node {

	public static final String XML_APPEND_ATTRIBUTE = "append";
	public static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	public static final String XML_CHARSET_ATTRIBUTE = "charset";
	public static final String XML_MASK_ATTRIBUTE = "mask";
	public static final String XML_HEADER_ATTRIBUTE = "header";
	public static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	public static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";

	private String fileURL;
	private boolean appendData;
	private TextTableFormatter formatter;
	private boolean header = true;
	private WritableByteChannel writer;
    private int skip;
	private int numRecords;
	
	public final static String COMPONENT_TYPE = "TEXT_TABLE_WRITER";
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
	public TextTableWriter(String id, String fileURL, String charset, 
			boolean appendData, String[] fields) {
		super(id);
		this.fileURL = fileURL;
		this.appendData = appendData;
		formatter = charset == null ? new TextTableFormatter(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER) : new TextTableFormatter(charset);
		formatter.setMask(fields);
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
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		long iRec = 0;
		int recordTo = numRecords < 0 ? Integer.MAX_VALUE : (skip <= 0 ? numRecords+1 : skip+1 + numRecords);
		record.init();
		//write records
		try {
			while (record != null && runIt) {
				iRec++;
				record = inPort.readRecord(record);
				if (skip >= iRec || recordTo <= iRec) continue;
				if (record != null) {
					formatter.write(record);
				}
				SynchronizeUtils.cloverYield();
			}
			formatter.eof();
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
			writer = fileURL == null ? Channels.newChannel(System.out) : FileUtils.getWritableChannel(getGraph().getProjectURL(), fileURL, appendData);
			formatter.init(getInputPort(READ_FROM_PORT).getMetadata());
            formatter.setDataTarget(writer);
            formatter.setHeader(header);
		} catch (IOException ex) {
			throw new ComponentNotReadyException(getId() + "IOError: " + ex.getMessage());
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#fromXML(org.jetel.graph.TransformationGraph, org.w3c.dom.Element)
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		TextTableWriter aDataWriter = null;
		
		try{
			String fields = xattribs.getString(XML_MASK_ATTRIBUTE,null);
			String[] aFields = fields == null ? null : fields.split(";");
			aDataWriter = new TextTableWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
									xattribs.getString(XML_FILEURL_ATTRIBUTE),
									xattribs.getString(XML_CHARSET_ATTRIBUTE,null),
									xattribs.getBoolean(XML_APPEND_ATTRIBUTE, false),
									aFields);
			if (xattribs.exists(XML_HEADER_ATTRIBUTE)){
				aDataWriter.setHeader(Boolean.parseBoolean(xattribs.getString(XML_HEADER_ATTRIBUTE)));
			}
			if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)){
				aDataWriter.setSkip(Integer.parseInt(xattribs.getString(XML_RECORD_SKIP_ATTRIBUTE)));
			}
			if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)){
				aDataWriter.setNumRecords(Integer.parseInt(xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE)));
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
		xmlElement.setAttribute(XML_HEADER_ATTRIBUTE,String.valueOf(header));
		if (skip != 0){
			xmlElement.setAttribute(XML_RECORD_SKIP_ATTRIBUTE, String.valueOf(skip));
		}
		if (numRecords != 0){
			xmlElement.setAttribute(XML_RECORD_COUNT_ATTRIBUTE,String.valueOf(numRecords));
		}
	}
	
	public void setHeader(boolean header) {
		this.header = header;
	}

    /**
     * Sets number of skipped records in next call of getNext() method.
     * @param skip
     */
    public void setSkip(int skip) {
        this.skip = skip;
    }

    /**
     * Sets number of written records.
     * @param numRecords
     */
    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }

}
