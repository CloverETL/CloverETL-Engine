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

import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;

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
 *  <pre>&lt;Node id="FILTEREMPL1" type="EXT_FILTER" filterExpression="//#CTL2&#13;&#10;$HireDate &amp;lt; &quot;1993-12-31&quot;"/&gt;</pre>
 * Want to filter on Name and Age fields. Name must start with 'A' char and Age must be greater than 25<br> 
 * <pre>&lt;Node id="FILTEREMPL1" type="EXT_FILTER" filterExpression="//#CTL2&#13;&#10;$Name~=&quot;^A.*&quot; and $Age &amp;gt;25"/&gt;</pre>
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
public class ExtFilter extends ExtFilterBase {

	private static final String XML_FILTERCLASS_ATTRIBUTE = "filterClass";
	public final static String COMPONENT_TYPE = "EXT_FILTER";

	private String filterClass;

	public ExtFilter(String id) {
		super(id);
	}
	
	public ExtFilter(String id, String filterExpression){
		super(id);
		this.filterExpression = filterExpression;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			return;
		}
		super.init();
		if (filterExpression == null && filterClass == null) {
			throw new ComponentNotReadyException("Either filter expression or filter class is expected.");
		} else if (filterExpression == null && filterClass != null) {
			filter = RecordFilterFactory.createFilter(filterClass, this);
		}
	}

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException,
			AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		ExtFilter filter = new ExtFilter(xattribs.getString(XML_ID_ATTRIBUTE));

		if (xattribs.exists(XML_FILTEREXPRESSION_ATTRIBUTE)) {
			filter.setFilterExpression(xattribs.getStringEx(XML_FILTEREXPRESSION_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF));
		}
		if (xattribs.exists(XML_FILTERCLASS_ATTRIBUTE)) {
			filter.setFilterClass(xattribs.getString(XML_FILTERCLASS_ATTRIBUTE));
		}
		return filter;
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		try {
			if (filterExpression != null) {
				initFilterExpression();
			} else if (filterClass == null) {
				throw new ComponentNotReadyException("Either 'filterClass' or 'filterExpression' is required.");
			}
		} catch (ComponentNotReadyException e) {
			status.addError(this, null, e);
		} finally {
			free();
		}

		return status;
	}

	public void setFilterClass(String filterClass) {
		this.filterClass = filterClass;
	}

}
