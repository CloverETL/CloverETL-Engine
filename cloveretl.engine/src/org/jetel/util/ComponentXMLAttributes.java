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
package org.jetel.util;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.jetel.exception.NotFoundException;
import org.w3c.dom.NamedNodeMap;
/**
 *  Helper class (wrapper) around NamedNodeMap with possibility to parse string
 *  values into integers, booleans, doubles..<br>
 *  Used in conjunction with org.jetel.Component.*<br>
 *  Converts any child nodes of form <code>&lt;attr name="xyz"&gt;abcd&lt;/attr&gt;</code> into
 *  attribute of the current node with name="xyz" and value="abcd".<br>
 *  Example:<br>
 *  <code><pre>
 *  &lt;Node id="mynode" name="xyz" append="yes"&gt;
 *  	&lt;attr name="query"&gt;
 * 			select * from my_table;
 *  	&lt;/attr&gt;
 *  	&lt;attr name="code"&gt;
 * 			a=b*10-20%50;
 *  	&lt;/attr&gt;
 *  &lt;/Node&gt;
 *  </pre></code>
 *
 *  There will be following attribute/value pairs available for getXXX() calls:
 *  <ul>
 *  <li>id - "mynode"</li>
 *  <li>name - "xyz"</li>
 *  <li>append - "yes"</li>
 *  <li>query - "select * from my_table;"</li>
 *  <li>code - "a=b*10-20%50;"</li>
 *  </ul>
 * @author      dpavlis
 * @since       July 25, 2002
 * @revision    $Revision$
 * @created     26. March 2003
 */

public class ComponentXMLAttributes {

	private NamedNodeMap attributes;
	private org.w3c.dom.Node nodeXML;
	private PropertyRefResolver refResolver;
	
	public static final String XML_ATTRIBUTE_NODE_NAME = "attr";
	public static final String XML_ATTRIBUTE_NODE_NAME_ATTRIBUTE = "name";

	//private Map    childNodes;

	/**
	 *  Constructor for the ComponentXMLAttributes object
	 *
	 * @param  nodeXML  Description of the Parameter
	 */
	public ComponentXMLAttributes(org.w3c.dom.Node nodeXML) {
	    this(nodeXML,null);
	}


	/**
	 *Constructor for the ComponentXMLAttributes object
	 *
	 * @param  nodeXML     Description of the Parameter
	 * @param  properties  Description of the Parameter
	 */
	public ComponentXMLAttributes(org.w3c.dom.Node nodeXML, Properties properties) {
	   
		this.nodeXML = nodeXML;
		refResolver=new PropertyRefResolver( properties!=null ? properties : new Properties());
		instantiateInlinedNodeAttributes(nodeXML);
		this.attributes = nodeXML.getAttributes();
	}
	
	private void instantiateInlinedNodeAttributes(org.w3c.dom.Node nodeXML){
	    org.w3c.dom.Node childNode;
	    org.w3c.dom.NodeList list;
	    NamedNodeMap childNodeAttributes;
	    String newAttributeName;
	    String newAttributeValue;
	    
		// add all "inlined" attributes in form of "attr" node as normal attributes
		if (nodeXML.hasChildNodes()) {
			list = nodeXML.getChildNodes();
			for (int i = 0; i < list.getLength(); i++) {
				childNode = list.item(i);
				if (childNode.getNodeName().equalsIgnoreCase(XML_ATTRIBUTE_NODE_NAME)) {
				    newAttributeName=childNode.getAttributes().getNamedItem(XML_ATTRIBUTE_NODE_NAME_ATTRIBUTE).getNodeValue();
				    // get text value
				    newAttributeValue=null;
				    org.w3c.dom.NodeList childList = childNode.getChildNodes();
					for (int j = 0; j < list.getLength(); j++) {
					    org.w3c.dom.Node child2Node = childList.item(j);
						if (child2Node.getNodeType() == org.w3c.dom.Node.TEXT_NODE) {
						    newAttributeValue=child2Node.getNodeValue();
						    break;
						}
					}
					// add value of child node as attribute, also create new attribute node
					if (newAttributeName!=null && newAttributeValue!=null){
					    org.w3c.dom.Attr newAttribute = nodeXML.getOwnerDocument().createAttribute(newAttributeName);
					    newAttribute.setNodeValue(newAttributeValue);
					    nodeXML.getAttributes().setNamedItem(newAttribute);
					    // remove child node as it is now included as an attribute - in attribute
						nodeXML.removeChild(childNode);
					}
					
				}
			}
		}
		
	}


	
	/**
	 *  Returns the String value of specified XML attribute
	 *
	 * @param  key  name of the attribute
	 * @return      The string value
	 */
	public String getString(String key) {
		try {
			return refResolver.resolveRef(attributes.getNamedItem(key).getNodeValue());
		} catch (Exception ex) {
			throw new NotFoundException("Attribute " + key + " not found!");
		}
	}


	/**
	 *  Returns the String value of specified XML attribute
	 *
	 * @param  key           name of the attribute
	 * @param  defaultValue  default value to be returned when attribute can't be found
	 * @return               The string value
	 */
	public String getString(String key, String defaultValue) {
		try {
			return refResolver.resolveRef(attributes.getNamedItem(key).getNodeValue());
		} catch (Exception ex) {
			return defaultValue;
		}
	}


	/**
	 *  Returns the int value of specified XML attribute
	 *
	 * @param  key  name of the attribute
	 * @return      The integer value
	 */
	public int getInteger(String key) {
		String value;
		try {
			value = refResolver.resolveRef(attributes.getNamedItem(key).getNodeValue());
		} catch (NullPointerException ex) {
			throw new NotFoundException("Attribute " + key + " not found!");
		}
		return Integer.parseInt(value);
	}


	/**
	 *  Returns the int value of specified XML attribute
	 *
	 * @param  key           name of the attribute
	 * @param  defaultValue  default value to be returned when attribute can't be found
	 * @return               The integer value
	 */
	public int getInteger(String key, int defaultValue) {
		String value;
		try {
			value = refResolver.resolveRef(attributes.getNamedItem(key).getNodeValue());
			return Integer.parseInt(value);
		} catch (Exception ex) {
			return defaultValue;
		}
	}


	/**
	 *  Returns the boolean value of specified XML attribute
	 *
	 * @param  key  name of the attribute
	 * @return      The boolean value
	 */
	public boolean getBoolean(String key) {
		String value;
		try {
			value = refResolver.resolveRef(attributes.getNamedItem(key).getNodeValue());
		} catch (NullPointerException ex) {
			throw new NotFoundException("Attribute " + key + " not found!");
		}
		return value.matches("^[tTyY].*");
	}


	/**
	 *  Returns the boolean value of specified XML attribute
	 *
	 * @param  key           name of the attribute
	 * @param  defaultValue  default value to be returned when attribute can't be found
	 * @return               The boolean value
	 */
	public boolean getBoolean(String key, boolean defaultValue) {
		String value;
		try {
			value = refResolver.resolveRef(attributes.getNamedItem(key).getNodeValue());
			return value.matches("^[tTyY].*");
		} catch (Exception ex) {
			return defaultValue;
		}

	}


	/**
	 *  Returns the double value of specified XML attribute
	 *
	 * @param  key  name of the attribute
	 * @return      The double value
	 */
	public double getDouble(String key) {
		String value;
		try {
			value = refResolver.resolveRef(attributes.getNamedItem(key).getNodeValue());
		} catch (NullPointerException ex) {
			throw new NotFoundException("Attribute " + key + " not found!");
		}
		return Double.parseDouble(value);
	}


	/**
	 *  Returns the double value of specified XML attribute
	 *
	 * @param  key           name of the attribute
	 * @param  defaultValue  default value to be returned when attribute can't be found
	 * @return               The double value
	 */
	public double getDouble(String key, double defaultValue) {
		String value;
		try {
			value = refResolver.resolveRef(attributes.getNamedItem(key).getNodeValue());
			return Double.parseDouble(value);
		} catch (Exception ex) {
			return defaultValue;
		}
	}


	/**
	 *  Checks whether specified attribute exists (XML node has such attribute defined)
	 *
	 * @param  key  name of the attribute
	 * @return      true if exists, otherwise false
	 */
	public boolean exists(String key) {
		if (attributes.getNamedItem(key) != null) {
			return true;
		} else {
			return false;
		}
	}


	/**
	 *  Returns first TEXT_NODE child under specified XML Node
	 *
	 * @param  nodeXML  XML node from which to start searching
	 * @return          The TEXT_NODE value (String) if any exist or null
	 */
	public String getText(org.w3c.dom.Node nodeXML) {
		org.w3c.dom.Node childNode;
		org.w3c.dom.NodeList list;
		if (nodeXML.hasChildNodes()) {
			list = nodeXML.getChildNodes();
			for (int i = 0; i < list.getLength(); i++) {
				childNode = list.item(i);
				if (childNode.getNodeType() == org.w3c.dom.Node.TEXT_NODE) {
					return refResolver.resolveRef(childNode.getNodeValue());
				}
			}
		}
		throw new NotFoundException("Text not found !");
	}


	/**
	 *  Searches for specific child node name under specified XML Node
	 *
	 * @param  nodeXML        XML node from which to start searching
	 * @param  childNodeName  name of the child node to be searched for
	 * @return                childNode if exist under specified name or null
	 */
	public org.w3c.dom.Node getChildNode(org.w3c.dom.Node nodeXML, String childNodeName) {
		org.w3c.dom.Node childNode;
		org.w3c.dom.NodeList list;
		if (nodeXML.hasChildNodes()) {
			list = nodeXML.getChildNodes();
			for (int i = 0; i < list.getLength(); i++) {
				childNode = list.item(i);
				if (childNodeName.equals(childNode.getNodeName())) {
					return childNode;
				} else {
					childNode = getChildNode(childNode, childNodeName);
					if (childNode != null) {
						return childNode;
					}
				}

			}
		}
		return null;
	}


	/**
	 *  Gets the childNodes attribute of the ComponentXMLAttributes object
	 *
	 * @param  nodeXML        Description of the Parameter
	 * @param  childNodeName  Description of the Parameter
	 * @return                The childNodes value
	 */
	public org.w3c.dom.Node[] getChildNodes(org.w3c.dom.Node nodeXML, String childNodeName) {
		org.w3c.dom.Node childNode;
		org.w3c.dom.NodeList list;
		List childNodesList = new LinkedList();
		if (nodeXML.hasChildNodes()) {
			list = nodeXML.getChildNodes();
			for (int i = 0; i < list.getLength(); i++) {
				childNode = list.item(i);
				if (childNodeName.equals(childNode.getNodeName())) {
					childNodesList.add(childNode);
				}
			}
		}
		return (org.w3c.dom.Node[]) childNodesList.toArray(new org.w3c.dom.Node[0]);
	}

	
	/**
	 * Converts XML Node's attributes to Properties object - hash of key-value pairs.
	 * Can omit/exclude certain attributes based on specified array of Strings - attribute
	 * names.
	 * @param exclude	array of Strings - names of attributes to be excluded (can be null)
	 * @return Properties object with pairs [attribute name]-[attribute value]
	 */
	public Properties attributes2Properties(String[] exclude){
	    Properties properties=new Properties();
	    Set exception=new HashSet();
	    String name;
	    
	    if (exclude!=null){
	        for (int i=0;i<exclude.length;i++){
	            exception.add(exclude[i]);
	        }
	    }
	    for (int i=0; i<attributes.getLength();i++){
	        name=attributes.item(i).getNodeName();
	        if (!exception.contains(name)){
	            properties.setProperty(name,
	                    refResolver.resolveRef(attributes.item(i).getNodeValue()));
	        }
	    }
	    return properties;
	}

	
	/**
	 * Replaces references to parameters in string with parameters' values.
	 * 
	 * @param input string in which references to parameters should be resolved 
	 * (substituted with parameters' values)
	 * @return String with references resolved.
	 */
	public String resloveReferences(String input){
	    return refResolver.resolveRef(input);
	}
	
}
/*
 *  End class StringUtils
 */

