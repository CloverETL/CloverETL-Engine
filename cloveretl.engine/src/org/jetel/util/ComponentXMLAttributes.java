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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.jetel.exception.AttributeNotFoundException;
import org.jetel.graph.TransformationGraph;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import org.apache.commons.logging.Log;

/**
 *  Helper class (wrapper) around NamedNodeMap with possibility to parse string
 *  values into integers, booleans, doubles..<br>
 *  Used in conjunction with org.jetel.Component.*<br>
 *  MAX_INT,MIN_INT,MAX_DOUBLE, MIN_DOUBLE constants are defined and are
 * translated to appropriate int or double values when getInteger(), getDouble()
 * methods are called.<br>
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

    private static final String STR_MAX_INT="MAX_INT";
    private static final String STR_MIN_INT="MIN_INT";
    private static final String STR_MAX_DOUBLE="MAX_DOUBLE";
    private static final String STR_MIN_DOUBLE="MIN_DOUBLE";
    
	protected NamedNodeMap attributes;
	protected Element nodeXML;
	protected PropertyRefResolver refResolver;
	
	public static final String XML_ATTRIBUTE_NODE_NAME = "attr";
	public static final String XML_ATTRIBUTE_NODE_NAME_ATTRIBUTE = "name";

	//private Map    childNodes;

    /**
     *  Constructor for the ComponentXMLAttributes object
     *
     * @param  nodeXML  Description of the Parameter
     */
    public ComponentXMLAttributes(Element nodeXML) {
        this(nodeXML, (Properties) null);
    }

    /**
	 *  Constructor for the ComponentXMLAttributes object
	 *
	 * @param  nodeXML  Description of the Parameter
	 */
	public ComponentXMLAttributes(Element nodeXML, TransformationGraph graph) {
	    this(nodeXML, graph.getGraphProperties());
	}


	/**
	 *Constructor for the ComponentXMLAttributes object
	 *
	 * @param  nodeXML     Description of the Parameter
	 * @param  properties  Description of the Parameter
	 */
	public ComponentXMLAttributes(Element nodeXML, Properties properties) {
	   
		this.nodeXML = nodeXML;
		refResolver= new PropertyRefResolver(properties);
		instantiateInlinedNodeAttributes(nodeXML);
		this.attributes = nodeXML.getAttributes();
	}

    
	private void instantiateInlinedNodeAttributes(Element _nodeXML){
	    org.w3c.dom.Node childNode;
	    org.w3c.dom.NodeList list;
	    String newAttributeName;
	    String newAttributeValue;
	    
		// add all "inlined" attributes in form of "attr" node as normal attributes
		if (_nodeXML.hasChildNodes()) {
			list = _nodeXML.getChildNodes();
			for (int i = 0; i < list.getLength(); i++) {
				childNode = list.item(i);
				if (childNode.getNodeName().equalsIgnoreCase(XML_ATTRIBUTE_NODE_NAME)) {
				    newAttributeName=childNode.getAttributes().getNamedItem(XML_ATTRIBUTE_NODE_NAME_ATTRIBUTE).getNodeValue();
				    // get text value
				    newAttributeValue=null;
				    org.w3c.dom.NodeList childList = childNode.getChildNodes();
					for (int j = 0; j < childList.getLength(); j++) {
					    org.w3c.dom.Node child2Node = childList.item(j);
						if (child2Node.getNodeType() == org.w3c.dom.Node.TEXT_NODE) {
						    newAttributeValue=child2Node.getNodeValue();
						    break;
						}
					}
					// add value of child node as attribute, also create new attribute node
					if (newAttributeName!=null && newAttributeValue!=null){
					    org.w3c.dom.Attr newAttribute = _nodeXML.getOwnerDocument().createAttribute(newAttributeName);
					    newAttribute.setNodeValue(newAttributeValue);
					    _nodeXML.getAttributes().setNamedItem(newAttribute);
					    // remove child node as it is now included as an attribute - in attribute
						_nodeXML.removeChild(childNode);
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
	public String getString(String key) throws AttributeNotFoundException {
		try {
			return refResolver.resolveRef(nodeXML.getAttribute(key));
		} catch (NullPointerException ex){
            throw new AttributeNotFoundException(key);
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
			return refResolver.resolveRef(nodeXML.getAttribute(key));
		} catch (Exception ex){
            return defaultValue;
        }  
	}

    public String getString(String key, String defaultValue, Log logger) {
        try{
            return refResolver.resolveRef(nodeXML.getAttribute(key));
        }catch(AttributeNotFoundException ex){
            logger.debug("using default value \""+defaultValue+"\" for attribute \""+key+"\" :",ex);
        }
        return defaultValue;

     }
    
    
    /**
     * Sets value of specified attribute
     * 
     * @param key   attribute name
     * @param value attribute value
     */
    public void setString(String key,String value) throws AttributeNotFoundException {
        nodeXML.setAttribute(key,String.valueOf(value));
    }
    

	/**
	 *  Returns the int value of specified XML attribute
	 *
	 * @param  key  name of the attribute
	 * @return      The integer value
	 */
	public int getInteger(String key) throws AttributeNotFoundException {
		String value;
		try {
			value = refResolver.resolveRef(nodeXML.getAttribute(key));
			if (value.equalsIgnoreCase(STR_MIN_INT)){
			    return Integer.MIN_VALUE;
			}else if (value.equalsIgnoreCase(STR_MAX_INT)){
			    return Integer.MAX_VALUE;
			}
		} catch (NullPointerException ex) {
            throw new AttributeNotFoundException(key);
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
			value = refResolver.resolveRef(nodeXML.getAttribute(key));
			if (value.equalsIgnoreCase(STR_MIN_INT)){
			    return Integer.MIN_VALUE;
			}else if (value.equalsIgnoreCase(STR_MAX_INT)){
			    return Integer.MAX_VALUE;
			}
			return Integer.parseInt(value);
		} catch (Exception ex) {
            return defaultValue;
        }
	}
    
    public int getInteger(String key, int defaultValue, Log logger) {
        String value;
        try {
            value = refResolver.resolveRef(nodeXML.getAttribute(key));
            if (value.equalsIgnoreCase(STR_MIN_INT)) {
                return Integer.MIN_VALUE;
            } else if (value.equalsIgnoreCase(STR_MAX_INT)) {
                return Integer.MAX_VALUE;
            }
            return Integer.parseInt(value);
        } catch (AttributeNotFoundException ex) {
            logger.debug("using default value \"" + defaultValue
                    + "\" for attribute \"" + key + "\" :", ex);
        } catch (Exception ex) {
        }
        return defaultValue;
    }
    
    /**
     * Sets value of specified attribute
     * 
     * @param key   attribute name
     * @param value value to be set
     */
    public void setInteger(String key,int value) throws AttributeNotFoundException{
        nodeXML.setAttribute(key,String.valueOf(value));
    }


	/**
	 *  Returns the boolean value of specified XML attribute
	 *
	 * @param  key  name of the attribute
	 * @return      The boolean value
	 */
	public boolean getBoolean(String key) throws AttributeNotFoundException {
		String value;
		try {
			value = refResolver.resolveRef(nodeXML.getAttribute(key));
		} catch (NullPointerException ex) {
			throw new AttributeNotFoundException(key);
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
			value = refResolver.resolveRef(nodeXML.getAttribute(key));
			return value.matches("^[tTyY].*");
		} catch (Exception ex) {
			return defaultValue;
		}

	}
    
    public boolean getBoolean(String key, boolean defaultValue, Log logger) {
        String value;
        try {
            value = refResolver.resolveRef(nodeXML.getAttribute(key));
            return value.matches("^[tTyY].*");
        }catch(AttributeNotFoundException ex){
            logger.debug("using default value \""+defaultValue+"\" for attribute \""+key+"\" :",ex);
        }catch (Exception ex) {
        }
       return defaultValue;

    }

    /**
     * Sets value of specified attribute
     * 
     * @param key   attribute name
     * @param value value to be set
     */
    public void setBoolean(String key,boolean value) throws AttributeNotFoundException {
        nodeXML.setAttribute(key,String.valueOf(value));
    }


	/**
	 *  Returns the double value of specified XML attribute
	 *
	 * @param  key  name of the attribute
	 * @return      The double value
	 */
	public double getDouble(String key) throws AttributeNotFoundException {
		String value;
		try {
			value = refResolver.resolveRef(nodeXML.getAttribute(key));
			if (value.equalsIgnoreCase(STR_MIN_DOUBLE)){
			    return Double.MIN_VALUE;
			}else if (value.equalsIgnoreCase(STR_MAX_DOUBLE)){
			    return Double.MAX_VALUE;
			}
		} catch (NullPointerException ex) {
			throw new AttributeNotFoundException(key);
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
	public double getDouble(String key, double defaultValue){
		String value;
		try {
			value = refResolver.resolveRef(nodeXML.getAttribute(key));
			if (value.equalsIgnoreCase(STR_MIN_DOUBLE)){
			    return Double.MIN_VALUE;
			}else if (value.equalsIgnoreCase(STR_MAX_DOUBLE)){
			    return Double.MAX_VALUE;
			}
			return Double.parseDouble(value);
		} catch (Exception ex) {
			return defaultValue;
		} 
	}

    public double getDouble(String key, double defaultValue, Log logger) {
        String value;
        try {
            value = refResolver.resolveRef(nodeXML.getAttribute(key));
            if (value.equalsIgnoreCase(STR_MIN_DOUBLE)) {
                return Double.MIN_VALUE;
            } else if (value.equalsIgnoreCase(STR_MAX_DOUBLE)) {
                return Double.MAX_VALUE;
            }
        } catch (AttributeNotFoundException ex) {
            logger.debug("using default value \"" + defaultValue
                    + "\" for attribute \"" + key + "\" :", ex);
        } catch (Exception ex) {

        }
        return defaultValue;

    }
    
    
    /**
     * Sets value of specified attribute
     * 
     * @param key   attribute name
     * @param value value to be set
     */
    public void setDouble(String key, double value) {
             nodeXML.setAttribute(key,String.valueOf(value));
    }

    
    
    
    
	/**
     * Checks whether specified attribute exists (XML node has such attribute
     * defined)
     * 
     * @param key
     *            name of the attribute
     * @return true if exists, otherwise false
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
	public String getText(org.w3c.dom.Node nodeXML) throws AttributeNotFoundException{
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
		throw new AttributeNotFoundException("TEXT_NODE not found within node \""+nodeXML.getNodeName()+"\"");
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
	public Properties attributes2Properties(String[] exclude) throws AttributeNotFoundException {
	    Properties properties=new Properties();
	    Set exception=new HashSet();
	    String name;
	    
	    Collections.addAll(exception,exclude);
       
	    for (int i=0; i<attributes.getLength();i++){
	        name=attributes.item(i).getNodeName();
	        if (!exception.contains(name)){
	            properties.setProperty(name,
	                    refResolver.resolveRef(attributes.item(i).getNodeValue()));
	        }
	    }
	    return properties;
	}

    public void Properties2Attributes(Properties properties){
        org.w3c.dom.Node node;
        for (Iterator iter=properties.entrySet().iterator();iter.hasNext();){
            Map.Entry entry=(Map.Entry)iter.next();
            // check whether attribute of certain name already exists
            if ((node=attributes.getNamedItem((String)entry.getKey())) != null){
               node.setNodeValue((String)entry.getValue()); // just set the value
            }else{
                // create new attribute
                org.w3c.dom.Attr attr=node.getOwnerDocument().createAttribute((String)entry.getKey());
                attr.setValue((String)entry.getValue());
                node.appendChild(attr);
            }
        }
    }
	
	/**
	 * Replaces references to parameters in string with parameters' values.
	 * 
	 * @param input string in which references to parameters should be resolved 
	 * (substituted with parameters' values)
	 * @return String with references resolved.
	 */
	public String resloveReferences(String input) throws AttributeNotFoundException{
	    return refResolver.resolveRef(input);
	}

	/**
	 * Determines whether resolving references is enabled/disabled.
	 * 
	 * @return	true if resolving references is enables.
	 */
	public boolean isResolveReferences(){
	    return this.refResolver.isResolve();
	}
	
	/**
	 * Enables/disables resolving references within string values to Properties.<br>
	 * Default behaviour is to resolve.
	 * 
	 * @param resolve	true to resolve references
	 */
	public void setResolveReferences(boolean resolve){
	    this.refResolver.setResolve(resolve);
	}
	
}
/*
 *  End class StringUtils
 */

