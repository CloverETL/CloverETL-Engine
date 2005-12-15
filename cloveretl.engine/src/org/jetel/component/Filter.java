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

import java.io.*;
import java.nio.ByteBuffer;
import org.jetel.graph.*;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordFilter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Filter Component</h3>
 *
 * <!-- All records not rejected by the filter are copied from input port:0 onto output port:0 
 *  rejected records are copied to port:1 (if connected) -->
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Filter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records for which the filterExpression evaluates TRUE are copied from input port:0 onto output port:0. Rejected records
 * are copied onto output port:1 (if it is connected).</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] - accepted records<br>
 * [1] - (optional) rejected records </td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td>It can filter on text, date, integer, numeric
 * fields with comparison <code>[&gt;, &lt;, ==, =&lt;, &gt;=, !=]</code><br>
 * Text fields can also be compared to a
 * Java regexp. using ~ (tilda) character <br>
 * A filter can be made of different parts separated by a ";".<br> 
 * If one of the parts is verified, the record pases the
 * filter (it's an OR combination of the parts, and AND can be achieved by
 * several filters).
 * Date format used for comparison is the same as defined for the field which
 * is used for filtration.  
 *
 * </td></tr>
 * </table>
 *  <br>  
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"FILTER"</td></tr>
 *  <tr><td><b>id</b></td>
 *  <td>component identification</td>
 *  </tr>
 *  <tr><td><b>filterExpression</b></td><td>Expression used for filtering records. <i>See above.</i></td></tr>
 *  </table>
 * 
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="FILTEREMPL1" type="FILTER" filterExpression="HireDate&amp;lt;1993-12-31"/&gt;</pre>
 *  <pre>&lt;Node id="FILTEREMPL1" type="FILTER" filterExpression="Name~^A.*;Age>25"/&gt;</pre>
 *
 * @author     rbauduin
 * @since    July 23, 2003
 * @see		org.jetel.graph.TransformationGraph
 * @see		org.jetel.graph.Node
 * @see 	org.jetel.graph.Edge
 */
public class Filter extends Node {

	private static final String XML_FILTEREXPRESSION_ATTRIBUTE = "filterExpression";
	public final static String COMPONENT_TYPE="FILTER";
	private final static int READ_FROM_PORT=0;
	
	
	
	private final static int WRITE_TO_PORT=0;
	private final static int REJECTED_PORT=1;
	
	private ByteBuffer recordBuffer;
	private RecordFilter recordFilter;
	
	public Filter(String id){
		super(id);
		
	}

	/**
	 *  Main processing method for the Filter object
	 *
	 * @since    July 23, 2002
	 */
	public void run() {
		InputPort inPort=getInputPort(READ_FROM_PORT);
		OutputPort outPort=getOutputPort(WRITE_TO_PORT);
		OutputPort rejectedPort=getOutputPort(REJECTED_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());

		record.init();
		boolean isData=true;
		
		while(isData && runIt){
			try{
				record=inPort.readRecord(record);
				if (record==null){
					isData = false;
					break;
				}
				if (recordFilter.accepts(record)){
					outPort.writeRecord(record);
				}else if (rejectedPort!=null){
					rejectedPort.writeRecord(record);
				}

			}catch(IOException ex){
				resultMsg=ex.getMessage();
				resultCode=Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			}catch(Exception ex){
				resultMsg=ex.getClass().getName()+" : "+ ex.getMessage();
				resultCode=Node.RESULT_FATAL_ERROR;
				return;
			}
			SynchronizeUtils.cloverYield();
		}
		broadcastEOF();
		if (runIt) resultMsg="OK"; else resultMsg="STOPPED";
		resultCode=Node.RESULT_OK;
	}	


	/**
	 *  Description of the Method
	 *
	 * @since    July 23, 2002
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port and one output
		if (inPorts.size()<1){
			throw new ComponentNotReadyException("At least one input port has to be defined!");
		}else if (outPorts.size()<1){
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		
		if (recordFilter!=null){
			recordFilter.init(getInputPort(READ_FROM_PORT).getMetadata());
		}else{
			throw new ComponentNotReadyException("RecordFilter class not defined !"); 
		}
	}
	
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     July 23, 2002
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILTEREXPRESSION_ATTRIBUTE,this.recordFilter.getFilterExpression());
	}

	public void setRecordFilter(RecordFilter rf)
	{
		recordFilter = rf;
	}

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           July 23, 2002
	 */
	public static Node fromXML(org.w3c.dom.Node nodeXML) {
		Filter filter;
		String filterExpression;
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML);
		

		try{
			filter = new Filter(xattribs.getString(Node.XML_ID_ATTRIBUTE));
			if (xattribs.exists(XML_FILTEREXPRESSION_ATTRIBUTE)){
				filterExpression=xattribs.getString(XML_FILTEREXPRESSION_ATTRIBUTE);
				filter.setRecordFilter(new RecordFilter(filterExpression));
			}
			return filter;
		}catch(Exception ex){
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
	}

	/**  Description of the Method */
    public boolean checkConfig() {
		return true;
	}
    
    public String getType(){
		return COMPONENT_TYPE;
	}

}


