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
package org.jetel.util.property;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.xml.namespace.QName;

import org.jetel.exception.AttributeNotFoundException;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.formatter.TimeIntervalUtils;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

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
 * @created     26. March 2003
 */

public class ComponentXMLAttributes {

/**
	 * 
	 */
	private static final String BOOLEAN_STRING_MATCH_PATTERN = "^[tTyY].*";
//    unused private static final String STR_MAX_SHORT="MAX_SHORT";
//    unused private static final String STR_MIN_SHORT="MIN_SHORT";
    public static final String STR_MAX_INT="MAX_INT";
    public static final String STR_MIN_INT="MIN_INT";
    public static final String STR_MAX_LONG="MAX_LONG";
    public static final String STR_MIN_LONG="MIN_LONG";
   public static final String STR_MAX_DOUBLE="MAX_DOUBLE";
    public static final String STR_MIN_DOUBLE="MIN_DOUBLE";
    
	protected NamedNodeMap attributes;
	final protected Element nodeXML;
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
	    this(nodeXML, graph != null ? graph.getGraphProperties() : null);
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
	    ArrayList<Node> toRemove = new ArrayList<Node>();
		if (_nodeXML!=null && _nodeXML.hasChildNodes()) {
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
						if (child2Node.getNodeType() == org.w3c.dom.Node.TEXT_NODE || child2Node.getNodeType() == org.w3c.dom.Node.CDATA_SECTION_NODE) {
						    newAttributeValue = newAttributeValue == null ? child2Node.getNodeValue() : newAttributeValue + child2Node.getNodeValue();
						    //break;
						}
					}
					// add value of child node as attribute, also create new attribute node
					if (newAttributeName!=null && newAttributeValue!=null){
					    org.w3c.dom.Attr newAttribute = _nodeXML.getOwnerDocument().createAttribute(newAttributeName);
					    newAttribute.setNodeValue(newAttributeValue);
					    _nodeXML.getAttributes().setNamedItem(newAttribute);
					    // remove child node as it is now included as an attribute - in attribute
//						_nodeXML.removeChild(childNode);
					    toRemove.add(childNode);
					}
					
				}
			}
		}
		for (Node node : toRemove) {
			nodeXML.removeChild(node);
		}
		
	}
	
    /**
     * Returns the String value of specified XML attribute
     * 
     * @param key   name of the attribute
     * @param resolveSpecChars if true, all special characters will be resolved; @see StringUtils.stringToSpecChar()
     * @return  The string value
     * @throws AttributeNotFoundException   if attribute does not exist or if can not resolve
     * reference to global parameter/property included in atribute's textual/string value
     */
	public String getString(String key) throws AttributeNotFoundException {
    	if (!nodeXML.hasAttribute(key)) {
            throw new AttributeNotFoundException(key);
    	}
        String value = nodeXML.getAttribute(key);
        return refResolver.resolveRef(value);
    }

	/**
     * Returns the String value of specified XML attribute. If the attribute is not found returns
     * default value.
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public String getString(String key, String defaultValue) {
		return getStringEx(key, defaultValue, null);
	}

	/**
     * Returns the String value of specified XML attribute. If the attribute is not found returns
     * default value. Reference resolving is done according the given flag.
	 * @param key
	 * @param defaultValue
	 * @param options
	 * @return
	 */
	public String getStringEx(String key, String defaultValue, RefResFlag flag) {
		if (!nodeXML.hasAttribute(key)) {
            return defaultValue;
		}
        String value=nodeXML.getAttribute(key);
        return refResolver.resolveRef(value, flag);
	}


	/**
     * Returns the String value of specified XML attribute. Reference resolving is done according the given flag.
	 * @param key
	 * @param flag
	 * @return
	 * @throws AttributeNotFoundException
	 */
	public String getStringEx(String key, RefResFlag flag) throws AttributeNotFoundException {
    	if (!nodeXML.hasAttribute(key)) {
            throw new AttributeNotFoundException(key);
    	}
        String value = nodeXML.getAttribute(key);
        return refResolver.resolveRef(value, flag);
    }


    /**
     * Returns the String value of specified XML attribute
     * 
     * @param key   name of the attribute
     * @param resolveSpecChars if true, all special characters will be resolved; @see StringUtils.stringToSpecChar()
     * @return  The string value
     * @throws AttributeNotFoundException   if attribute does not exist or if can not resolve
     * reference to global parameter/property included in atribute's textual/string value
     * @deprecated call getString(String, RefResFlag) instead
     */
	@Deprecated
    public String getString(String key, boolean resolveSpecChars) throws AttributeNotFoundException {
    	if (!nodeXML.hasAttribute(key)) {
            throw new AttributeNotFoundException(key);
    	}
        String value=nodeXML.getAttribute(key);
        return refResolver.resolveRef(value, resolveSpecChars);
    }


	/**
	 *  Returns the String value of specified XML attribute
	 *
	 * @param  key           name of the attribute
	 * @param  defaultValue  default value to be returned when attribute can't be found
	 * @return               The string value
	 * @deprecated call getString(String, String, RefResFlag)
	 */
	@Deprecated
	public String getString(String key, String defaultValue, boolean resolveSpecChars) {
		if (!nodeXML.hasAttribute(key)) {
            return defaultValue;
		}
        String value=nodeXML.getAttribute(key);
        return refResolver.resolveRef(value, resolveSpecChars);
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
     * @throws AttributeNotFoundException   if attribute does not exist or if can not resolve
     * reference to global parameter/property included in atribute's textual/string value
	 */
	public int getInteger(String key) throws AttributeNotFoundException {
        String value = getString(key);
        if (value.equalsIgnoreCase(STR_MIN_INT)){
            return Integer.MIN_VALUE;
        }else if (value.equalsIgnoreCase(STR_MAX_INT)){
            return Integer.MAX_VALUE;
        }
        try{
        	return Integer.parseInt(value);
        }catch(NumberFormatException ex){
        	throw new NumberFormatException(String.format("Parse error when converting value \"%s\" of property \"%s\" to integer.",value,key)); 
        }
	}

	/**
	 *  Returns the int value of specified XML attribute
	 *
	 * @param  key           name of the attribute
	 * @param  defaultValue  default value to be returned when attribute can't be found
	 * @return               The integer value
	 */
	public int getInteger(String key, int defaultValue) {
	    try{
	        String value = getString(key);
	        if (value.equalsIgnoreCase(STR_MIN_INT)){
	            return Integer.MIN_VALUE;
	        }else if (value.equalsIgnoreCase(STR_MAX_INT)){
	            return Integer.MAX_VALUE;
	        }
	        return Integer.parseInt(value);
	    } catch (NumberFormatException ex) {
	        return defaultValue;
	    } catch (AttributeNotFoundException e) {
	        return defaultValue;
		}
	}
    
	public short getShort(String key) throws AttributeNotFoundException {
        String value = getString(key);
        if (value.equalsIgnoreCase(STR_MIN_INT)){
            return Short.MIN_VALUE;
        }else if (value.equalsIgnoreCase(STR_MAX_INT)){
            return Short.MAX_VALUE;
        }
        try{
        	return Short.parseShort(value);
        }catch (NumberFormatException ex){
        	throw new NumberFormatException(String.format("Parse error when converting value \"%s\" of property \"%s\" to short.",value,key)); 
        }
	}

	public short getShort(String key, short defaultValue) {
	    try{
	        String value = getString(key);
	        value = refResolver.resolveRef(value);
	        if (value.equalsIgnoreCase(STR_MIN_INT)){
	            return Short.MIN_VALUE;
	        }else if (value.equalsIgnoreCase(STR_MAX_INT)){
	            return Short.MAX_VALUE;
	        }
			return Short.parseShort(value);
	    } catch (NumberFormatException ex) {
	        return defaultValue;
	    } catch (AttributeNotFoundException e) {
	        return defaultValue;
		}
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
	 *  Returns the int value of specified XML attribute
	 *
	 * @param  key  name of the attribute
	 * @return      The long value
     * @throws AttributeNotFoundException   if attribute does not exist or if can not resolve
     * reference to global parameter/property included in atribute's textual/string value
	 */
	public long getLong(String key) throws AttributeNotFoundException {
        String value = getString(key);
        if (value.equalsIgnoreCase(STR_MIN_LONG)){
            return Long.MIN_VALUE;
        }else if (value.equalsIgnoreCase(STR_MAX_LONG)){
            return Long.MAX_VALUE;
        }
        try{
        	return Long.parseLong(value);
        }catch(NumberFormatException ex){
        	throw new NumberFormatException(String.format("Parse error when converting value \"%s\" of property \"%s\" to long.",value,key)); 
        }
	}



	/**
	 *  Returns the int value of specified XML attribute
	 *
	 * @param  key           name of the attribute
	 * @param  defaultValue  default value to be returned when attribute can't be found
	 * @return               The long value
	 */
	public long getLong(String key, long defaultValue) {
	    try{
	        String value = getString(key);
	        if (value.equalsIgnoreCase(STR_MIN_LONG)){
	            return Long.MIN_VALUE;
	        }else if (value.equalsIgnoreCase(STR_MAX_LONG)){
	            return Long.MAX_VALUE;
	        }
	        return Long.parseLong(value);
	    } catch (NumberFormatException ex) {
	        return defaultValue;
	    } catch (AttributeNotFoundException e) {
	        return defaultValue;
		}
	}

    /**
     * Sets value of specified attribute
     * 
     * @param key   attribute name
     * @param value value to be set
     */
    public void setLong(String key,long value) throws AttributeNotFoundException{
        nodeXML.setAttribute(key,String.valueOf(value));
    }

    /**
	 *  Returns the boolean value of specified XML attribute
	 *
	 * @param  key  name of the attribute
	 * @return      The boolean value
     * @throws AttributeNotFoundException   if attribute does not exist or if can not resolve
     * reference to global parameter/property included in atribute's textual/string value
	 */
    public boolean getBoolean(String key) throws AttributeNotFoundException {
        String value = getString(key);
        return value.matches(BOOLEAN_STRING_MATCH_PATTERN);
	}


	/**
	 *  Returns the boolean value of specified XML attribute
	 *
	 * @param  key           name of the attribute
	 * @param  defaultValue  default value to be returned when attribute can't be found
	 * @return               The boolean value
	 */
	public boolean getBoolean(String key, boolean defaultValue) {
		try {
	        String value = getString(key);
			return value.matches(BOOLEAN_STRING_MATCH_PATTERN);
		} catch (AttributeNotFoundException ex) {
			return defaultValue;
		}
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
     * @throws AttributeNotFoundException   if attribute does not exist or if can not resolve
     * reference to global parameter/property included in atribute's textual/string value
	 */
	public double getDouble(String key) throws AttributeNotFoundException {
        String value = getString(key);
	    if (value.equalsIgnoreCase(STR_MIN_DOUBLE)){
	        return Double.MIN_VALUE;
	    }else if (value.equalsIgnoreCase(STR_MAX_DOUBLE)){
	        return Double.MAX_VALUE;
	    }
	    try{
	    	return Double.parseDouble(value);
	    }catch(NumberFormatException ex){
	    	throw new NumberFormatException(String.format("Parse error when converting value \"%s\" of property \"%s\" to double.",value,key)); 
	    }
	}


	/**
	 *  Returns the double value of specified XML attribute
	 *
	 * @param  key           name of the attribute
	 * @param  defaultValue  default value to be returned when attribute can't be found
	 * @return               The double value
	 */
	public double getDouble(String key, double defaultValue){
		try {
	        String value = getString(key);
			if (value.equalsIgnoreCase(STR_MIN_DOUBLE)){
			    return Double.MIN_VALUE;
			}else if (value.equalsIgnoreCase(STR_MAX_DOUBLE)){
			    return Double.MAX_VALUE;
			}
			return Double.parseDouble(value);
		} catch (NumberFormatException ex) {
			return defaultValue;
		} catch (AttributeNotFoundException e) {
			return defaultValue;
		} 
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
		} 
		return false;
	}

	/**
	 *  Returns first TEXT_NODE child under specified XML Node
	 *
	 * @param  nodeXML  XML node from which to start searching
     * @param resolveSpecChars if true, all special characters will be resolved; @see StringUtils.stringToSpecChar()
	 * @return          The TEXT_NODE value (String) if any exist or null
     * @throws AttributeNotFoundException   if attribute does not exist or if can not resolve
     * reference to global parameter/property included in atribute's textual/string value
     */
	@Deprecated
	public String getText(org.w3c.dom.Node nodeXML, boolean resolveSpecChars) throws AttributeNotFoundException{
		org.w3c.dom.Node childNode;
		org.w3c.dom.NodeList list;
		if (nodeXML.hasChildNodes()) {
			list = nodeXML.getChildNodes();
			for (int i = 0; i < list.getLength(); i++) {
				childNode = list.item(i);
				if (childNode.getNodeType() == org.w3c.dom.Node.TEXT_NODE) {
					return refResolver.resolveRef(childNode.getNodeValue(), resolveSpecChars);
				}
			}
		}
		throw new AttributeNotFoundException("TEXT_NODE not found within node \""+nodeXML.getNodeName()+"\"");
	}

    /**
     *  Returns first TEXT_NODE child under specified XML Node
     * 
     * @param nodeXML  XML node from which to start searching
     * @return  The TEXT_NODE value (String) if any exist or null
     * * @throws AttributeNotFoundException   if attribute does not exist or if can not resolve
     * reference to global parameter/property included in atribute's textual/string value
     */
	@Deprecated
    public String getText(org.w3c.dom.Node nodeXML) throws AttributeNotFoundException{
        return getText(nodeXML,true);
    }
    
	/**
	 *  Searches for specific child node name under specified XML Node
	 *
	 * @param  nodeXML        XML node from which to start searching
	 * @param  childNodeName  name of the child node to be searched for
	 * @return                childNode if exist under specified name or null
	 */
	@Deprecated
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
	 * Converts XML Node's attributes to Properties object - hash of key-value pairs.
	 * Can omit/exclude certain attributes based on specified array of Strings - attribute
	 * names.
	 * @param exclude	array of Strings - names of attributes to be excluded (can be null)
	 * @param resolveSpecChars specifies whether special characters (\n, etc) should be resolved. 
	 * {@link StringUtils#stringToSpecChar(CharSequence)}
	 * @return Properties object with pairs [attribute name]-[attribute value]
	 */
	@Deprecated
	public Properties attributes2Properties(String[] exclude, boolean resolveSpecChars) {
	    Properties properties=new Properties();
	    Set<String> exception=new HashSet<String>();
	    String name;
	    
	    if(exclude != null) Collections.addAll(exception,exclude);
       
	    for (int i=0; i<attributes.getLength();i++){
	        name=attributes.item(i).getNodeName();
	        if (!exception.contains(name)){
	            properties.setProperty(name,
	                    refResolver.resolveRef(attributes.item(i).getNodeValue(), resolveSpecChars));
	        }
	    }
	    return properties;
	}

	public Properties attributes2Properties(String[] exclude) {
		return attributes2Properties(exclude, null);
	}

	/**
	 * Converts XML Node's attributes to Properties object - hash of key-value pairs.
	 * Can omit/exclude certain attributes based on specified array of Strings - attribute
	 * names.
	 * @param exclude	array of Strings - names of attributes to be excluded (can be null)
	 * @param flag		flag for property resolving
	 * @return Properties object with pairs [attribute name]-[attribute value]
	 */
	public Properties attributes2Properties(String[] exclude, RefResFlag flag) {
	    Properties properties = new Properties();
	    Set<String> exceptions = new HashSet<String>();
	    String name;
	    
	    if (exclude != null) {
	    	Collections.addAll(exceptions, exclude);
	    }
       
	    for (int i = 0; i < attributes.getLength(); i++) {
	        name = attributes.item(i).getNodeName();
	        if (!exceptions.contains(name)) {
	            properties.setProperty(name, refResolver.resolveRef(attributes.item(i).getNodeValue(), flag));
	        }
	    }
	    
	    return properties;
	}

    public void properties2Attributes(Properties properties){
        org.w3c.dom.Node node;
        for (Iterator iter=properties.entrySet().iterator();iter.hasNext();){
            Map.Entry entry=(Map.Entry)iter.next();
            // check whether attribute of certain name already exists
            node=attributes.getNamedItem((String)entry.getKey());
            if (node!= null){
               node.setNodeValue((String)entry.getValue()); // just set the value
            }else{
                // create new attribute
                org.w3c.dom.Attr attr=nodeXML.getOwnerDocument().createAttribute((String)entry.getKey());
                attr.setValue((String)entry.getValue());
                nodeXML.appendChild(attr);
            }
        }
    }

	public String resolveReferences(String input) throws AttributeNotFoundException {
		return resolveReferences(input, null);
	}

	/**
	 * Replaces references to parameters in string with parameters' values.
	 * 
	 * @param input string in which references to parameters should be resolved 
	 * (substituted with parameters' values)
	 * @return String with references resolved.
	 */
	public String resolveReferences(String input, RefResFlag flag) throws AttributeNotFoundException{
	    return refResolver.resolveRef(input, flag);
	}

	/**
	 * Resolve references with explicit resolveSpecChars
	 * @param input
	 * @param resolveSpecChars
	 * @return
	 * @throws AttributeNotFoundException
	 */
	@Deprecated
	public String resolveReferences(String input, boolean resolveSpecChars) throws AttributeNotFoundException{
	    return refResolver.resolveRef(input, resolveSpecChars);
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
	
	/**
	 * Returns the qualified name value of specified XML attribute
	 * 
	 * @param key
	 *            name of the attribute
	 * @return The qualified name value
	 * @throws AttributeNotFoundException
	 *             if attribute does not exist or if can not resolve reference to global parameter/property included in
	 *             atribute's textual/string value
	 */
	public QName getQName(String key) throws AttributeNotFoundException {
		String value = nodeXML.getAttribute(key);
		if (value.length() == 0) {
			throw new AttributeNotFoundException(key);
		}

		value = refResolver.resolveRef(value);

		return QName.valueOf(value);
	}

	/**
	 * Returns the qualified name value of specified XML attribute
	 * 
	 * @param key
	 *            name of the attribute
	 * @param defaultValue
	 *            default value to be returned when attribute can't be found
	 * @return The qualified name value
	 */
	public QName getQName(String key, QName defaultValue) {
		String value = nodeXML.getAttribute(key);
		if (value.length() == 0) {
			return defaultValue;
		}

		value = refResolver.resolveRef(value);

		return new QName(value);
	}

	/**
	 * Sets the qualified name as value of specified attribute
	 * 
	 * @param key
	 *            attribute name
	 * @param value
	 *            attribute value
	 */
	public void setQName(String key, QName value) throws AttributeNotFoundException {
		nodeXML.setAttribute(key, value.toString());
	}
	
	/**
	 * Returns the URL location value of specified XML attribute
	 * 
	 * @param key
	 *            name of the attribute
	 * @return The qualified name value
	 * @throws AttributeNotFoundException
	 *             if attribute does not exist or if can not resolve reference to global parameter/property included in
	 *             atribute's textual/string value
	 */
	public URL getURL(String key) throws AttributeNotFoundException {
		String value = nodeXML.getAttribute(key);
		if (value.length() == 0) {
			throw new AttributeNotFoundException(key);
		}

		value = refResolver.resolveRef(value);

		try {
			return new URL(value);
		} catch (MalformedURLException urle) {
			throw new AttributeNotFoundException(key, urle.getMessage());
		}
	}

	/**
	 * Returns the URL location value of specified XML attribute
	 * 
	 * @param key
	 *            name of the attribute
	 * @param defaultValue
	 *            default value to be returned when attribute can't be found
	 * @return The URL location value
	 */
	public URL getURL(String key, URL defaultValue) {
		String value = nodeXML.getAttribute(key);
		if (value.length() == 0) {
			return defaultValue;
		}

		value = refResolver.resolveRef(value);

		try {
			return new URL(value);
		} catch (MalformedURLException urle) {
			return defaultValue;
		}
	}
	
	/**
	 * Sets the URL location as value of specified attribute
	 * 
	 * @param key
	 *            attribute name
	 * @param value
	 *            attribute value
	 */
	public void setURL(String key, URL value) throws AttributeNotFoundException {
		nodeXML.setAttribute(key, value.toString());
	}
	
	/**
	 * Returns the time interval duration from the specified XML attribute.
	 * Numbers without a unit are regarded as milliseconds.
	 * 
	 * @param key
	 *            name of the attribute
	 * @return The time interval value, default unit is millisecond
	 * @throws AttributeNotFoundException
	 */
	public long getTimeInterval(String key) throws AttributeNotFoundException {
        String value = getString(key);
		if (value.equalsIgnoreCase(STR_MIN_LONG)) {
			return Long.MIN_VALUE;
		} else if (value.equalsIgnoreCase(STR_MAX_LONG)) {
			return Long.MAX_VALUE;
		}
		
		if (value.length() == 0) {
			throw new AttributeNotFoundException(key);
		}

		try {
			return TimeIntervalUtils.parseInterval(value);
		} catch (IllegalArgumentException ex) {
			throw new NumberFormatException(String.format("Parse error when converting value \"%s\" of property \"%s\" to a time interval.", value, key));
		}
	}
	
	/**
	 * Returns the time interval duration from the specified XML attribute.
	 * Numbers without a unit are regarded as milliseconds.
	 * 
	 * If the attribute is not found or the conversion fails,
	 * the <code>defaultValue</code> is returned. 
	 * 
	 * @param key
	 *            name of the attribute
	 * @param defaultValue
	 *            default value to be returned when attribute can't be found
	 *            
	 * @return The time interval value, default unit is millisecond
	 */
	public long getTimeInterval(String key, long defaultValue) {
		try {
			return getTimeInterval(key);
		} catch (NumberFormatException ex) {
			return defaultValue;
		} catch (AttributeNotFoundException e) {
			return defaultValue;
		}
	}
	
}
/*
 *  End class StringUtils
 */

