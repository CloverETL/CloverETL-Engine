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

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
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
	
	private RecordFilterOld recordFilter;
	
	public Filter(String id){
		super(id);
		
	}

	@Override
	public Result execute() throws Exception {
		InputPort inPort=getInputPort(READ_FROM_PORT);
		OutputPort outPort=getOutputPort(WRITE_TO_PORT);
		OutputPort rejectedPort=getOutputPort(REJECTED_PORT);
		DataRecord record = DataRecordFactory.newRecord(inPort.getMetadata());

		record.init();
		boolean isData=true;
		
		while (isData && runIt) {
			record = inPort.readRecord(record);
			if (record == null) {
				isData = false;
				break;
			}
			if (recordFilter.accepts(record)) {
				outPort.writeRecord(record);
			} else if (rejectedPort != null) {
				rejectedPort.writeRecord(record);
			}

			SynchronizeUtils.cloverYield();
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}


	/**
	 *  Description of the Method
	 *
	 * @since    July 23, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
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
	
	public void setRecordFilter(RecordFilterOld rf)
	{
		recordFilter = rf;
	}

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @throws AttributeNotFoundException 
	 * @since           July 23, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		Filter filter;
		String filterExpression;
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(xmlElement, graph);
		
		filter = new Filter(xattribs.getString(XML_ID_ATTRIBUTE));
		if (xattribs.exists(XML_FILTEREXPRESSION_ATTRIBUTE)){
			filterExpression=xattribs.getString(XML_FILTEREXPRESSION_ATTRIBUTE);
			filter.setRecordFilter(new RecordFilterOld(filterExpression));
		}
		return filter;
	}

	/**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        //TODO
    	
        status.add(new ConfigurationProblem(
        		"Component is of type SORT, which is deprecated",
        		Severity.WARNING, this, Priority.NORMAL));


        return status;
    }
    
    /*
     * (non-Javadoc)
     * @see org.jetel.graph.Node#reset()
     */
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		// no implementation needed
	}

}


