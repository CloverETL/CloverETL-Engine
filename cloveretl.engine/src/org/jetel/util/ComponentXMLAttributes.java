/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.util;

import org.w3c.dom.NamedNodeMap;
import org.jetel.exception.NotFoundException;
/**
 *  Helper class (wrapper) around NamedNodeMap with possibility to parse string
 *  values into integers, booleans, doubles..<br>
 *  Used in conjunction with org.jetel.Component.*
 *
 *@author     dpavlis
 *@created    26. March 2003
 *@since      July 25, 2002
 */

public class ComponentXMLAttributes {

	private NamedNodeMap attributes;
	private org.w3c.dom.Node nodeXML;

	//private Map    childNodes;


	/**
	 *  Constructor for the ComponentXMLAttributes object
	 *
	 *@param  nodeXML  Description of the Parameter
	 */
	public ComponentXMLAttributes(org.w3c.dom.Node nodeXML) {
		attributes = nodeXML.getAttributes();
		this.nodeXML = nodeXML;
	}


	/**
	 *  Gets the string attribute of the ComponentXMLAttributes object
	 *
	 *@param  key  Description of the Parameter
	 *@return      The string value
	 */
	public String getString(String key) {
		try {
			return attributes.getNamedItem(key).getNodeValue();
		} catch (Exception ex) {
			throw new NotFoundException(key);
		}
	}


	/**
	 *  Gets the integer attribute of the ComponentXMLAttributes object
	 *
	 *@param  key  Description of the Parameter
	 *@return      The integer value
	 */
	public int getInteger(String key) {
		String value;
		try {
			value = attributes.getNamedItem(key).getNodeValue();
		} catch (Exception ex) {
			throw new NotFoundException(key);
		}
		return Integer.parseInt(value);
	}


	/**
	 *  Gets the boolean attribute of the ComponentXMLAttributes object
	 *
	 *@param  key  Description of the Parameter
	 *@return      The boolean value
	 */
	public boolean getBoolean(String key) {
		String value;
		try {
			value = attributes.getNamedItem(key).getNodeValue();
		} catch (Exception ex) {
			throw new NotFoundException(key);
		}
		return value.matches("^[tTyY].*");
	}


	/**
	 *  Gets the double attribute of the ComponentXMLAttributes object
	 *
	 *@param  key  Description of the Parameter
	 *@return      The double value
	 */
	public double getDouble(String key) {
		String value;
		try {
			value = attributes.getNamedItem(key).getNodeValue();
		} catch (Exception ex) {
			throw new NotFoundException(key);
		}
		return Double.parseDouble(value);
	}


	/**
	 *  Description of the Method
	 *
	 *@param  key  Description of the Parameter
	 *@return      Description of the Return Value
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
	 *@param  nodeXML  Description of the Parameter
	 *@return          The TEXT_NODE value (String) if any exist or null
	 */
	public String getText(org.w3c.dom.Node nodeXML) {
		org.w3c.dom.Node childNode;
		org.w3c.dom.NodeList list;
		if (nodeXML.hasChildNodes()) {
			list = nodeXML.getChildNodes();
			for (int i = 0; i < list.getLength(); i++) {
				childNode = list.item(i);
				if (childNode.getNodeType() == org.w3c.dom.Node.TEXT_NODE) {
					return childNode.getNodeValue();
				}
			}
		}
		return null;
	}


	/**
	 *  Searches for specific child node name under specified XML Node
	 *
	 *@param  nodeXML        Description of the Parameter
	 *@param  childNodeName  Description of the Parameter
	 *@return                childNode if exist under specified name or null
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

}
/*
 *  End class StringUtils
 */

