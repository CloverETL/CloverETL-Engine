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
import java.nio.ByteBuffer;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Node;
import org.jetel.interpreter.FilterExpParser;
import org.jetel.interpreter.SimpleNode;
import org.jetel.interpreter.Stack;
import org.jetel.util.ComponentXMLAttributes;

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
 * <td>All records accepted by the filter (passing filter condition) are copied from input port:0 onto output port:0. Rejected records
 * are copied onto output port:1 (if it is connected).</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] - accepted records<br>
 * [1] - (optional) rejected records </td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td>It can filter on text, date, integer, numeric
 * fields with comparison <code>[>, <, ==, =<, >=, !=]</code><br>
 * Text fields can also be compared to a
 * Java regexp. using ~= (tilda,equal sign) characters<br>
 * A filter can be made of different parts separated by a logical
 * operator AND, OR. You can as well use parenthesis to give precendence<br> 
 * <b>NoteL</b>Date format used for specifying date value is yyyy-MM-dd for
 * dates only and yyyy-MM-dd HH:mm:ss for date&time.
 * When referencing particular field, you have to precede field's name with 
 * dollar [$] sign - e.g. $FirstName.
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
 * Want to filter on HireDate field. HireDate must be less than 31st of December 1993<br>
 *  <pre>&lt;Node id="FILTEREMPL1" type="FILTER" filterExpression="$HireDate &amp;lt; &quot;1993-12-31&quot;"/&gt;</pre>
 * Want to filter on Name and Age fields. Name must start with 'A' char and Age must be greater than 25<br> 
 * <pre>&lt;Node id="FILTEREMPL1" type="FILTER" filterExpression="$Name~=&quot;^A.*&quot; and $Age &amp;gt;25"/&gt;</pre>
 * <br>
 * <b>Hint:</b> If you have combination of filtering expressions connected by AND operator, it
 * is wise to write first those which can be quickly evaluated - comparing integers, numbers, dates.
 * 
 * @author   dpavlis
 * @since    Sep 01, 2004
 * @see		org.jetel.graph.TransformationGraph
 * @see		org.jetel.graph.Node
 * @see 	org.jetel.graph.Edge
 */
public class ExtFilter extends org.jetel.graph.Node {

	public final static String COMPONENT_TYPE="EXT_FILTER";
	private final static int READ_FROM_PORT=0;
	
	private final static int WRITE_TO_PORT=0;
	private final static int REJECTED_PORT=1;
	
	private ByteBuffer recordBuffer;
	private SimpleNode recordFilter;
	private String filterExpression;
	private DataRecord record;
	
	public ExtFilter(String id){
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
		boolean isData=true;
		Stack stack=recordFilter.getStack();
		
		while(isData && runIt){
			try{
				record=inPort.readRecord(record);
				if (record==null){
					isData = false;
					break;
				}
				recordFilter.interpret();
				if (((Boolean)stack.pop()).booleanValue()){
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
				resultMsg=ex.getMessage();
				resultCode=Node.RESULT_FATAL_ERROR;
				return;
			}
			yield();
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
		record = new DataRecord(getInputPort(READ_FROM_PORT).getMetadata());
		record.init();
		FilterExpParser parser=new FilterExpParser(record,
				new ByteArrayInputStream(filterExpression.getBytes()));
		if (parser!=null){
			try {
				  recordFilter = parser.Start();
				  recordFilter.init();
			}catch (Exception e) {
				throw new ComponentNotReadyException("Error when parsing expression: "+e.getMessage());
			}
		}else{
			throw new ComponentNotReadyException("Can't create filter expression parser !"); 
		}
	}
	
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     July 23, 2002
	 */
	public org.w3c.dom.Node toXML() {
		// TODO
		return null;
	}

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           July 23, 2002
	 */
	public static org.jetel.graph.Node fromXML(org.w3c.dom.Node nodeXML) {
		ExtFilter filter;
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML);
		
		try{
			filter = new ExtFilter(xattribs.getString("id"));
			if (xattribs.exists("filterExpression")){
				filter.setFilterExpression(xattribs.getString("filterExpression"));
			}else{
				filter.setFilterExpression(xattribs.getText(nodeXML));
			}
			return filter;
		}catch(Exception ex){
			System.err.println(nodeXML.getNodeName()+" : "+ex.getMessage());
			return null;
		}
	}

	/**  Description of the Method */
    public boolean checkConfig() {
		return true;
	}

	/**
	 * @param filterExpression The filterExpression to set.
	 */
	public void setFilterExpression(String filterExpression) {
		this.filterExpression = filterExpression;
	}
}


