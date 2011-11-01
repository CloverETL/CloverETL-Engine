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
package org.jetel.component.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.jetel.util.string.StringUtils;

/**
 * Class for creating command for load utility from string pieces and parameters.
 * Each parameter is one field in array.
 * 
 * @author Miroslav Haupt (Mirek.Haupt@javlin.cz) 
 * (c) Javlin a.s. (www.javlin.cz)
 * @since 10.2.2009
 */
public class CommandBuilder {
	private final static String DEFAULT_EQUAL_MARK = "=";
	private final static int UNUSED_INT = -1;
	private final static String DEFAULT_SWITCH_MARK = "";
	private final static String LINE_SEPARATOR = System.getProperty("line.separator");

	private Properties params;
	private List<String> cmdList;
	private String switchMark = DEFAULT_SWITCH_MARK;
	private String equalMark = DEFAULT_EQUAL_MARK;

	public CommandBuilder(Properties properties) {
		this.params = properties;
		cmdList = new ArrayList<String>();
	}
	
	public CommandBuilder(Properties properties, String switchMark) {
		this(properties);
		this.switchMark = switchMark;
	}
	
	public CommandBuilder(Properties properties, char equalChar) {
		this(properties);
		this.equalMark = String.valueOf(equalChar);
	}
	
	public CommandBuilder(Properties properties, String switchMark, String equalMark) {
		this(properties, switchMark);
		this.equalMark = equalMark;
	}

	/**
	 * NOTE: This method was introduced due to hot fix of issue CL-1932.
	 * Special characters (StringUtils.specCharToString()) are not converted to string
	 * in comparison with former implementation (CommandBuilder.addAttribute()).
	 * Method should be removed once the issue "CL-1930 Escape sequences interpretation in CommandBuilder" is solved.
	 * This method can be invoked only by MsSqlDataWriter.createCommandLineForLoadUtility() method 
	 * for 'serverName' attribute.
	 * 
	 * DO NOT CALL THIS METHOD IN OTHER CASES!!! comprehensive re-factorization is necessary first
	 *  
	 *  Adds attribute and it's value:
	 *  attrName=attrValue or attrName='attrValue'
	 *  for exmaple:  host=localhost | host='localhost' | --host=localhost 
	 * 
	 * @param attrName
	 * @param attrValue
	 * @param singleQuoted decides if attrValue will be single quoted
	 * @deprecated see the note above
	 */
	public void addAttributeDirect(String attrName, String attrValue) {
		if (StringUtils.isEmpty(attrValue)) {
			return;
		}

		cmdList.add(switchMark + attrName + equalMark + attrValue);
	}
	
	/**
	 *  Adds attribute and it's value:
	 *  attrName=attrValue or attrName='attrValue'
	 *  for exmaple:  host=localhost | host='localhost' | --host=localhost 
	 * 
	 * @param attrName
	 * @param attrValue
	 * @param singleQuoted decides if attrValue will be single quoted
	 */
	public void addAttribute(String attrName, String attrValue, boolean singleQuoted) {
		if (StringUtils.isEmpty(attrValue)) {
			return;
		}

		if (singleQuoted) {
			cmdList.add(switchMark + attrName + equalMark + "'" + StringUtils.specCharToString(attrValue) + "'");
		} else {
			cmdList.add(switchMark + attrName + equalMark + StringUtils.specCharToString(attrValue));
		}
	}
	
	/**
	 *  Adds attribute and it's value (no quoted):
	 *  attrName=attrValue
	 *  for exmaple:  host=localhost | --host=localhost 
	 * 
	 * @param attrName
	 * @param attrValue
	 */
	public void addAttribute(String attrName, String attrValue) {
		addAttribute(attrName, attrValue, false);
	}
	
	/**
	 *  Adds attribute and it's value (no quoted):
	 *  attrName=attrValue | --attrName=attrValue
	 *  for exmaple:  port=123
	 * 
	 * @param attrName
	 * @param attrValue
	 */
	public void addAttribute(String attrName, int attrValue) {
		if (attrValue == UNUSED_INT) {
			return;
		}
		addAttribute(attrName, String.valueOf(attrValue), false);
	}
	
	/**
	 *  Adds both attribute and it's value (no quoted) separately:
	 *  --attrName
	 *  attrValue
	 *  for exmaple:  -host, localhost | --host, localhost 
	 * 
	 * @param attrName
	 * @param attrValue
	 */
	public void addAttributeAsTwoAttributes(String attrName, String attrValue) {
		if (StringUtils.isEmpty(attrValue)) {
			return;
		}

		cmdList.add(switchMark + attrName);
		cmdList.add(attrValue);
	}
	
	/**
	 *  Adds both attribute and it's value (no quoted) separately:
	 *  --attrName
	 *  attrValue
	 *  for exmaple:  -host, localhost | --host, localhost 
	 * 
	 * @param attrName
	 * @param attrValue
	 */
	public void addAttributeAsTwoAttributes(String attrName, int attrValue) {
		if (attrValue == UNUSED_INT) {
			return;
		}
		addAttributeAsTwoAttributes(attrName, String.valueOf(attrValue));
	}
	
	/**
	 *  If attrValue doesn't equal "false" then attrName is added to command.
	 *  "<i><b>switchMark</b>attrName</i>"<br>
	 *  for exmaple:  --compress
	 * 
	 * @param attrName
	 * @param attrValue
	 */
	public void addBooleanAttribute(String attrName, boolean attrValue) {
		if (attrValue) {
			cmdList.add(switchMark + attrName);
		}
	}
	
	/**
	 *  Adds param and it's value - value is get from properties:
	 *  paramKeyword=paramValue or paramKeyword='paramValue'
	 *  for exmaple:  host=localhost | host='localhost' | -host=localhost
	 * 
	 * @param paramName name of parameter used in properties
	 * @param paramKeyword name of parameter used in load utility command
	 * @param singleQuoted decides if attrValue will be single quoted
	 */
	public boolean addParam(String paramName, String paramKeyword, boolean singleQuoted) {
		if (paramName == null || !params.containsKey(paramName)) {
			return false;
		}
		addAttribute(paramKeyword, params.getProperty(paramName), singleQuoted);
		return true;
	}
	
	/**
	 *  Adds param and it's value (no quoted) - value is get from properties:
	 *  paramKeyword=paramValue
	 *  for exmaple:  host=localhost | -host=localhost
	 * 
	 * @param paramName name of parameter used in properties
	 * @param paramKeyword name of parameter used in load utility command
	 */
	public void addParam(String paramName, String paramKeyword) {
		addParam(paramName, paramKeyword, false);
	}
	
	/**
	 *  If paramName is contained in properties and it's value 
	 *  doesn't equal "false" then param is added to command.
	 *  "<i><b>switchMark</b>switchString</i>"<br>
	 *  for exmaple:  --compress
	 * 
	 * @param paramName name of param in properties
	 * @param switchString name of param in command
	 */
	public boolean addBooleanParam(String paramName, String switchString) {
		if (params.containsKey(paramName) && !"false".equalsIgnoreCase(params.getProperty(paramName))) {
			cmdList.add(switchMark + switchString);
			return true;
		}
		return false;
	}
	
	/**
	 *  If paramName is contained in properties and it's value 
	 *  doesn't equal "false" then param is added to command.
	 *  "<i><b>switchMark</b>switchString</i>"<br>
	 *  When paramName isn't in properties and defaultValue==true then it is added too.
	 *  for exmaple:  --compress
	 * 
	 * @param paramName
	 * @param switchString
	 * @param defaultValue
	 */
	public void addBooleanParam(String paramName, String switchString, boolean defaultValue) {
		if (params.containsKey(paramName)) {
			addBooleanParam(paramName, switchString);
			return;
		}

		if (defaultValue) {
			cmdList.add(switchMark + switchString);
		}
	}
	
	/**
	 * Appends given value to the end of command.
	 * 
	 * @param value
	 */
	public void add(String value) {
		cmdList.add(value);
	}
	
	public String[] getCommand() {
		return cmdList.toArray(new String[cmdList.size()]);
	}
	
	/**
	 * Return command as String, add space between each field.
	 * @return command as String
	 */
	public String getCommandAsString() {
		StringBuilder sb = new StringBuilder();
		String lastStr = null;
		for (String str : cmdList) {
			if (sb.length() == 0 || str.equals(LINE_SEPARATOR) || 
					(lastStr != null && lastStr.endsWith(LINE_SEPARATOR))) {
				sb.append(str);
			} else {
				sb.append(" " + str);
			}
			lastStr = str;
		}
		return sb.toString();
	}
}
