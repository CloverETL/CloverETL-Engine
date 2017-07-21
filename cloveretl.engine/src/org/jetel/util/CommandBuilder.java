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
package org.jetel.util;

import java.util.Properties;

import org.jetel.util.string.StringUtils;

/**
 * Class for creating command from string pieces and parameters
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Aug 14, 2007
 *
 */
public class CommandBuilder {
	
	private final static char EQUAL_CHAR = '=';
	private final static char DEFAULT_PARAMETER_DELIMITER = ' ';
	private final static char DEFAULT_END_CHARACTER = '\n';
	private final static String DEFAULT_SWITCH_MARK = "-";
	private final static String TRUE = "true";
	
	StringBuilder command;
	char parameterDelimiter;
	char endCharacter;
	String switchMark;
	Properties params;

	/**
	 * Creates command object with default parameter delimiter (' '), end char ('\n') and switch mark ("-")
	 * 
	 * @param command char, which will be used as
	 */
	public CommandBuilder(String command){
		this.command = new StringBuilder(command);
		this.parameterDelimiter = DEFAULT_PARAMETER_DELIMITER;
		this.endCharacter = DEFAULT_END_CHARACTER;
		this.switchMark = DEFAULT_SWITCH_MARK;
	}
	
	/**
	 * @return parameters
	 */
	public Properties getParams() {
		return params;
	}

	/**
	 * Sets parameters
	 * 
	 * @param params
	 */
	public void setParams(Properties params) {
		this.params = params;
	}
	
	/**
	 * @return complete command 
	 */
	public String getCommand(){
		return command.toString() + endCharacter;
	}
	
	/**
	 * if paramName is in properties adds to the end of command:
	 * 	" paramName=paramValue"
	 * 
	 * @param paramName
	 */
	public void addParameter(String paramName) {
		if (params.containsKey(paramName)) {
			command.append(parameterDelimiter);
			command.append(paramName);
			command.append(EQUAL_CHAR);
			command.append(params.getProperty(paramName));
		}
	}
	
	/**
	 * if paramName is in properties adds to the end of command: 
	 * 	"[ prefix] paramName=paramValue"
	 * 
	 * @param prefix string to write before paramName
	 * @param addPrefix indicates if write prefix or not
	 * @param paramName 
	 * @return true if prefix was written (parmName is among parameters)
	 */
	public boolean addParameterWithPrefixClauseConditionally(String prefix, boolean addPrefix, 
			String paramName){
		if (params.containsKey(paramName)) {
			if (addPrefix) {
				command.append(parameterDelimiter);
				command.append(prefix);
			}
			command.append(parameterDelimiter);
			command.append(paramName);
			command.append(EQUAL_CHAR);
			command.append(params.getProperty(paramName));
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * if paramName is in properties adds to the end of command: 
	 *	"[ prefix] paramName<i>equalChar</i>paramValue"	 
	 *
	 * @param prefix string to write before paramName
	 * @param addPrefix indicates if write prefix or not
	 * @param paramName
	 * @param equalChar string, which will be written between paramName and paramValue
	 * @return true if prefix was written (parmName is among parameters)
	 */
	public boolean addParameterSpecialWithPrefixClauseConditionally(String prefix, boolean addPrefix, 
			String paramName, String equalChar){
		if (params.containsKey(paramName)) {
			if (addPrefix) {
				command.append(parameterDelimiter);
				command.append(prefix);
			}
			command.append(parameterDelimiter);
			command.append(paramName);
			command.append(equalChar);
			command.append(params.getProperty(paramName));
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 *  if paramName is in properties adds to the end of command: 
	 *   "[ prefix] paramName="paramValue""
	 * 
	 * @param prefix  string to write before paramName
	 * @param addPrefix indicates if write prefix or not
	 * @param paramName
	 * @return true if prefix was written (parmName is among parameters)
	 */
	public boolean addAndQuoteParameterWithPrefixClauseConditionally(String prefix, boolean addPrefix, 
			String paramName){
		if (params.containsKey(paramName)) {
			if (addPrefix) {
				command.append(parameterDelimiter);
				command.append(prefix);
			}
			command.append(parameterDelimiter);
			command.append(paramName);
			command.append(EQUAL_CHAR);
			String tmp = params.getProperty(paramName);
			if (StringUtils.isQuoted(tmp)) {
				command.append(tmp);
			}else{
				command.append(StringUtils.quote(tmp));
			}
			return true;
		}else{
			return false;
		}
	}

	/**
	 * if paramName is in properties and has TRUE value adds to the end of command: 
	 *  "[ prefix] paramName"
	 * 
	 * @param prefix string to write before paramName
	 * @param addPrefix indicates if write prefix or not
	 * @param paramName
	 * @return true if prefix was written (= paramName found and has "true" value)
	 */
	public boolean addBooleanParameterWithPrefixClauseConditionally(String prefix, boolean addPrefix, 
			String paramName){
		if (params.containsKey(paramName) && params.getProperty(paramName).equalsIgnoreCase(TRUE)) {
			if (addPrefix) {
				command.append(parameterDelimiter);
				command.append(prefix);
			}
			command.append(parameterDelimiter);
			command.append(paramName);
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 *  if paramName is in properties adds to the end of command: 
	 *   " prefix<i>paramValue</i>
	 * 
	 * @param prefix
	 * @param paramName
	 */
	public void addParameterWithPrefix(String prefix, String paramName){
		if (params.containsKey(paramName)) {
			command.append(parameterDelimiter);
			command.append(prefix);
			command.append(params.getProperty(paramName));
		}
	}
	
	/**
	 * if paramName is in properties and has TRUE value adds to the end of command: 
	 *  " paramName"
	 * 
	 * @param paramName
	 */
	public void addBooleanParameter(String paramName){
		if (params.containsKey(paramName) && params.getProperty(paramName).equalsIgnoreCase(TRUE)) {
			command.append(parameterDelimiter);
			command.append(paramName);
		}
	}
	
	/**
	 * if paramName is in properties adds to the end of command: 
	 *  " paramName<i>equalChar</i>paramValue"
	 * 
	 * @param paramName
	 * @param equalChar
	 */
	public void addParameterSpecial(String paramName, String equalChar){
		if (params.containsKey(paramName)) {
			command.append(parameterDelimiter);
			command.append(paramName);
			command.append(equalChar);
			command.append(params.getProperty(paramName));
		}
	}

	/**
	 * appends given string to the end of command
	 * 
	 * @param str
	 */
	public void append(String str){
		command.append(str);
	}
	
	@Override
	public String toString() {
		return command.toString();
	}
}
