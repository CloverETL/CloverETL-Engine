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
package org.jetel.data.parser;

import java.util.List;

import javax.xml.transform.TransformerException;

import net.sf.saxon.om.Axis;
import net.sf.saxon.om.AxisIterator;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.tinytree.TinyNodeImpl;
import net.sf.saxon.trans.XPathException;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;

public class XPathElement {

	private static final int NO_FIELD = -1;
	
	private String cloverField;
	private XPathExpression xpathExpression = null;
	private String childNodeName = null;
	private boolean trim = true;

	private List<?> nodeList;
	private int cloverFieldNumber = NO_FIELD;
	
	private NodeInfo previousContextNode = null;
	private String previousValue = null; 
	
	boolean defaultValue = false;
	
	private Object value;
	
	private AxisIterator childIterator; 
	private TinyNodeImpl item;

	/**
	 * Constructor for xpath expression.
	 * 
	 * @param xpathExpression
	 * @param cloverField
	 * @throws XPathException
	 */
	public XPathElement(XPathExpression xpathExpression, String cloverField) throws XPathException {
		this.xpathExpression = xpathExpression;
		this.cloverField = cloverField;
	}

	/**
	 * Constructor for getting value from child node.
	 * 
	 * @param childNodeName
	 * @param cloverField
	 */
	public XPathElement(String childNodeName, String cloverField) {
		this.childNodeName = childNodeName;
		this.cloverField = cloverField;
	}

	
	public XPathExpression getXPathExpression() {
		return xpathExpression;
	}
	
	public String getCloverField() {
		return cloverField;
	}
	
	public void setTrim(boolean trim) {
		this.trim = trim;
	}
	
	/**
	 * Sets value from context node to data record. 
	 * 
	 * @param record
	 * @param contextNode
	 * @return
	 * @throws TransformerException
	 */
	public DataRecord getValue(DataRecord record, NodeInfo contextNode) throws TransformerException {
		// get field index
		if (record == null) return null;
		if (contextNode == null) return record;
		if (cloverFieldNumber == NO_FIELD && record != null) {
			Integer i = (Integer)record.getMetadata().getFieldNamesMap().get(cloverField);
			if (i == null) 
				throw new TransformerException("Clover field name '" + cloverField + "' not found in metadata");
			cloverFieldNumber = i.intValue();
		}
		
		// get previous value
		if (previousContextNode == contextNode) {
			if (record != null) {
				if (defaultValue) {
					record.getField(cloverFieldNumber).setToDefaultValue();
				} else if (previousValue != null)	{
					assignValue(record.getField(cloverFieldNumber), previousValue);
				}
			}
			return record;
		}
		
		// get value (from xpath)
		if (xpathExpression != null) {
			nodeList = xpathExpression.evaluate(contextNode);
			previousContextNode = contextNode;

			switch(nodeList.size()) {
			case 0:
				defaultValue = true;
				record.getField(cloverFieldNumber).setToDefaultValue();
				break;
			case 1: 
				defaultValue = false;
				value = nodeList.get(0);
				if (value instanceof NodeInfo) {
					previousValue = trim ? ((NodeInfo)value).getStringValue().trim() : ((NodeInfo)value).getStringValue();
				} else {
					previousValue = trim ? value.toString().trim() : value.toString();
				}
				assignValue(record.getField(cloverFieldNumber), previousValue);
				break;
			default:
				throw new TransformerException("XPath for clover field'" + cloverField + "' contains two or more values!");
			}
		} else {
			childIterator = contextNode.iterateAxis(Axis.CHILD);
			previousValue = null;
			while ((item = (TinyNodeImpl)childIterator.next()) != null) {
				if (item.getDisplayName().equals(childNodeName)) {
					previousValue = trim ? item.getStringValue().trim() : item.getStringValue();
					break;
				}
			}
			if (previousValue == null) {
				defaultValue = true;
				record.getField(cloverFieldNumber).setToDefaultValue();
			} else {
				defaultValue = false;
				assignValue(record.getField(cloverFieldNumber), previousValue);
			}
		}
		return record;
	}
	
	/**
	 * The value of the current data field is assigned with the usage of data model specific conversions.  
	 * @param currentField
	 * @param value
	 * @throws TransformerException
	 */
	protected void assignValue(DataField currentField, String value) throws TransformerException {
		currentField.fromString(value);
	}
}
