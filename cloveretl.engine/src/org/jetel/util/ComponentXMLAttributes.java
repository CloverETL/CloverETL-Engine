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
	//private Map    childNodes;

	
	/**
	 *  Constructor for the ComponentXMLAttributes object
	 *
	 *@param  nodeXML  Description of the Parameter
	 */
	public ComponentXMLAttributes(org.w3c.dom.Node nodeXML) {
		attributes = nodeXML.getAttributes();
		
		/*
		NodeList childern=nodeXML.getChildNodes();
		childNodes=new HashMap();
		for(int i=0;i<childern.getLength();i++){
			childNodes.put(childern.item(i), );
		}
		check how is CDATA or TEXT present
		
		*/ 
	}


	/**
	 *  Gets the string attribute of the ComponentXMLAttributes object
	 *
	 *@param  key  Description of the Parameter
	 *@return      The string value
	 */
	String getString(String key) {
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
	int getInteger(String key) {
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
	boolean getBoolean(String key) {
		String value;
		try {
			value = attributes.getNamedItem(key).getNodeValue();
		} catch (Exception ex) {
			throw new NotFoundException(key);
		}
		return value.matches("^[tTyY]");
	}


	/**
	 *  Gets the double attribute of the ComponentXMLAttributes object
	 *
	 *@param  key  Description of the Parameter
	 *@return      The double value
	 */
	double getDouble(String key) {
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
	boolean exists(String key) {
		if (attributes.getNamedItem(key) != null) {
			return true;
		} else {
			return false;
		}
	}
}
/*
 *  End class StringUtils
 */

