package org.jetel.data.parser;

import java.util.List;

import javax.xml.transform.TransformerException;

import net.sf.saxon.om.Axis;
import net.sf.saxon.om.AxisIterator;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.tinytree.TinyNodeImpl;
import net.sf.saxon.trans.XPathException;

import org.jetel.data.DataRecord;

public class XPathElement {

	private static final int NO_FIELD = -1;
	
	private String cloverField;
	private XPathExpression xpathExpression = null;
	private String childNodeName = null;
	private boolean trim = true;

	private List nodeList;
	private int cloverFieldNumber = NO_FIELD;
	
	private NodeInfo previousContextNode = null;
	private String previousValue = null; 
	
	boolean defaultValue = false;
	
	private Object value;
	
	private AxisIterator childIterator; 
	private TinyNodeImpl item;

	private XPathElement() {}

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
			Integer i = (Integer)record.getMetadata().getFieldNames().get(cloverField);
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
					record.getField(cloverFieldNumber).fromString(previousValue);
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
				record.getField(cloverFieldNumber).fromString(previousValue);
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
				record.getField(cloverFieldNumber).fromString(previousValue);
			}
		}
		return record;
	}
}
