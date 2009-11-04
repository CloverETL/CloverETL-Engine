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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.normalize.CTLRecordNormalize;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ITLCompiler;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPortDirect;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangExecutor;
import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.ASTnode.CLVFStartExpression;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

/**
 *  <h3>Extended Filter Component</h3>
 *
 * <!-- All records for which the filterExpression evaluates TRUE are copied from input port:0 onto output port:0. 
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
 * fields with comparison <code>[&gt;, &lt;, ==, &lt;=, &gt;=, !=]</code><br>
 * Text fields/expressions can also be compared to a
 * Java regexp. using <tt>~=</tt> (tilda,equal sign) characters<br>
 * A filter can be made of different parts separated by a logical
 * operator AND, OR. You can as well use parenthesis to give precendence<br> 
 * <b>Note:</b>Date format used for specifying date value is <tt>yyyy-MM-dd</tt> for
 * dates only and <tt>yyyy-MM-dd HH:mm:ss</tt> for date&time. These patterns correspond
 * to values specified in "defaultProperties" file.<br>
 * When referencing particular field, you have to precede field's name with 
 * dollar [$] sign - e.g. $FirstName.<br>
 * To ease the burden of converting comparison operators to XML-compatible form,
 * each operator has its textual abbreviation - <tt>[.eq. .ne. .lt. .le. .gt. .ge.]</tt><br>
 * Built-in functions you can use in expressions:
 * <ul>
 * <li>today()
 * <li>uppercase( ..str expression.. )
 * <li>lowercase( ..str expression.. )
 * <li>substring( ..str expression.. , from, length)
 * <li>trim( .. str expression.. )
 * <li>length( ..str expression.. )
 * <li>isnull( &lt;field reference&gt; )
 * <li>concat( ..str expression.., ..str expression.. , ...... )
 * <li>dateadd( ..date expression.., ..amount.. , year|month|day|hour|minute|sec )
 * <li>datediff( ..date expression.., ..date expression.. , year|month|day|hour|minute|sec )
 * <li>nvl(&lt;field reference&gt;, ..expression.. )
 * <li>replace(..str expression.., ..regex_pattern.., ..str expression.. )
 * <li>num2str(..num expression.. )
 * <li>str2num(..str expression.. )
 * <li>iif ( ..condition expression .. , ..expression.. , ..expression.. )
 * <li>print_err ( ..str expression.. )
 * <li>date2str(..date expression..,..format str expression..)
 * <li>str2date(..str expression.., .. format str expression..)
 * </ul> 
 * </td></tr>
 * </table>
 *  <br>  
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"EXT_FILTER"</td></tr>
 *  <tr><td><b>id</b></td>
 *  <td>component identification</td>
 *  </tr>
 *  <tr><td><b>filterExpression</b></td><td>Expression used for filtering records. <i>See above.</i></td></tr>
 *  </table>
 * <i>Note: you can also put the expression inside the XML Node - see examples.</i>
 *  <h4>Examples:</h4>
 * Want to filter on HireDate field. HireDate must be less than 31st of December 1993<br>
 *  <pre>&lt;Node id="FILTEREMPL1" type="EXT_FILTER" filterExpression="$HireDate &amp;lt; &quot;1993-12-31&quot;"/&gt;</pre>
 * Want to filter on Name and Age fields. Name must start with 'A' char and Age must be greater than 25<br> 
 * <pre>&lt;Node id="FILTEREMPL1" type="EXT_FILTER" filterExpression="$Name~=&quot;^A.*&quot; and $Age &amp;gt;25"/&gt;</pre>
 * <pre>&lt;Node id="FILTEREMPL1" type="EXT_FILTER"&gt;
 * $Name~="^A.*" and $Age.gt.25
 *&lt;/Node&gt;</pre>
 * More complex example showing how to use various built-in functions.<br>
 * <pre>&lt;Node id="FILTEREMPL1" type="EXT_FILTER"&gt;
 * ( trim($CustomerName)~=".*Peter.*" or $DateSale==dateadd(today(),-1,month)) and $Age*2.lt.$Weight-10
 *&lt;/Node&gt;</pre>
 * </pre>
 * Evaluating data fields with NULL values (have isNull() set) results in runtime error. To
 * circumvent such situation, use <code>isnull</code> and <code>nvl</code> functions.<br>
 * <pre>&lt;Node id="FILTEREMPL1" type="EXT_FILTER"&gt;
 * !isnull($CustomerName) && $CustomerName~=".*Peter.*" or nvl($DateSale,2001-1-1)>=$DateInvoice
 *&lt;/Node&gt;</pre>
 * </pre>
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

	private static final String XML_FILTEREXPRESSION_ATTRIBUTE = "filterExpression";
	public final static String COMPONENT_TYPE="EXT_FILTER";
	private final static int READ_FROM_PORT=0;
	
	private final static int WRITE_TO_PORT=0;
	private final static int REJECTED_PORT=1;
	
	
	private RecordFilter filter = null;
	private String filterExpression;
	
    static Log logger = LogFactory.getLog(ExtFilter.class);
    
	public ExtFilter(String id){
		super(id);
		
	}

	@Override
	public Result execute() throws Exception {
		InputPortDirect inPort=getInputPortDirect(READ_FROM_PORT);
		OutputPortDirect outPort=getOutputPortDirect(WRITE_TO_PORT);
		OutputPortDirect rejectedPort=getOutputPortDirect(REJECTED_PORT);
		boolean isData=true;
        DataRecord record = new DataRecord(getInputPort(READ_FROM_PORT).getMetadata());
        record.init();
        ByteBuffer recordBuffer=ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
        
		while(isData && runIt){
			try{
                recordBuffer.clear();
				if (!inPort.readRecordDirect(recordBuffer)){
					isData = false;
					break;
				}
                record.deserialize(recordBuffer);
				if (filter.isValid(record)){
                    recordBuffer.rewind();
					outPort.writeRecordDirect(recordBuffer);
				}else if (rejectedPort!=null){
                    recordBuffer.rewind();
					rejectedPort.writeRecordDirect(recordBuffer);
				}

			}catch(ClassCastException ex){
				logger.error("Invalid filter expression - does not evaluate to TRUE/FALSE !");
				throw new JetelException("Invalid filter expression - does not evaluate to TRUE/FALSE !",ex);
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
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		if (filterExpression.contains(org.jetel.ctl.TransformLangExecutor.CTL_TRANSFORM_CODE_ID)) {
			// new CTL initialization
			DataRecordMetadata[] inputMetadata = (DataRecordMetadata[]) getInMetadata().toArray(new DataRecordMetadata[getInMetadata().size()]);
			DataRecordMetadata[] outputMetadata = (DataRecordMetadata[]) getOutMetadata().toArray(new DataRecordMetadata[getOutMetadata().size()]);
			ITLCompiler compiler = TLCompilerFactory.createCompiler(getGraph(),inputMetadata,outputMetadata,"UTF-8");
	    	
			List<ErrorMessage> msgs = compiler.compileExpression(filterExpression, CTLRecordFilter.class, getId(),CTLRecordFilterAdapter.ISVALID_FUNCTION_NAME,boolean.class);
	    	if (compiler.errorCount() > 0) {
	    		for (ErrorMessage msg : msgs) {
	    			logger.error(msg.toString());
	    		}
	    		throw new ComponentNotReadyException("CTL code compilation finished with " + compiler.errorCount() + " errors");
	    	}
	    	if (compiler.errorCount() > 0) {
	    		for (ErrorMessage msg : msgs) {
	    			logger.error(msg.toString());
	    		}
	    		throw new ComponentNotReadyException("CTL code compilation finished with " + compiler.errorCount() + " errors");
	    	}
	    	Object ret = compiler.getCompiledCode();
	    	if (ret instanceof org.jetel.ctl.TransformLangExecutor) {
	    		// setup interpreted runtime
	    		filter = new CTLRecordFilterAdapter((org.jetel.ctl.TransformLangExecutor)ret, logger);
	    	} else if (ret instanceof CTLRecordNormalize){
	    		filter = (CTLRecordFilter)ret;
	    	} else {
	    		// this should never happen as compiler always generates correct interface
	    		throw new ComponentNotReadyException("Invalid type of record transformation");
	    	}
	    	// set graph instance to transformation (if CTL it can access lookups etc.)
	    	filter.setGraph(getGraph());
	    	
	    	// initialize transformation
	    	filter.init();
		} else {
			// old TL initialization
			TransformLangParser parser=new TransformLangParser(getInputPort(READ_FROM_PORT).getMetadata(),filterExpression);
			try {
				  final CLVFStartExpression recordFilter = parser.StartExpression();
				  filter = new RecordFilterTL(recordFilter);
				  if( getGraph() != null ){
					  filter.setGraph(getGraph());
				  }
				  filter.init();
			} catch (ParseException ex) {
                throw new ComponentNotReadyException("Parser error when parsing expression: "+ex.getMessage());
            } catch (Exception e) {
				throw new ComponentNotReadyException("Error when initializing expression: "+e.getMessage());
			}
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
		Document doc = xmlElement.getOwnerDocument();
		Element childElement = doc.createElement("attr");
		childElement.setAttribute("name", XML_FILTEREXPRESSION_ATTRIBUTE);
		// join given SQL commands
		Text textElement = doc.createTextNode(filterExpression);
		childElement.appendChild(textElement);
		xmlElement.appendChild(childElement);
	}

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           July 23, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ExtFilter filter;
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(xmlElement, graph);
		
		try{
			filter = new ExtFilter(xattribs.getString(XML_ID_ATTRIBUTE));
			if (xattribs.exists(XML_FILTEREXPRESSION_ATTRIBUTE)){
				filter.setFilterExpression(xattribs.getStringEx(XML_FILTEREXPRESSION_ATTRIBUTE,RefResFlag.SPEC_CHARACTERS_OFF));
			}else{
				try {
					filter.setFilterExpression(xattribs.getText(xmlElement, false));
				} catch (AttributeNotFoundException e) {
					throw new AttributeNotFoundException(XML_FILTEREXPRESSION_ATTRIBUTE);
				}
			}
			return filter;
		}catch(Exception ex){
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}

	/**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 1, 1)
        		|| !checkOutputPorts(status, 1, 2)) {
        	return status;
        }
        
        checkMetadata(status, getInMetadata(), getOutMetadata());

        try {
            init();
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

	/**
	 * @param filterExpression The filterExpression to set.
	 */
	public void setFilterExpression(String filterExpression) {
		this.filterExpression = filterExpression;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}
	
	public RecordFilter getRecordFilter() {
		return filter;
	}
	
	public void reset() throws ComponentNotReadyException  {
		super.reset();
		// Nothing more to reinitialize
	}
}


