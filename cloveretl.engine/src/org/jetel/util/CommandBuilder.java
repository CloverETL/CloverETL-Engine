
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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

import java.util.Properties;

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
	
	private final char EQUAL_CHAR = '=';
	private final char DEFAULT_PARAMETER_DELIMITER = ' ';
	private final char DEFAULT_END_CHARACTER = '\n';
	private final static String DEFAULT_SWITCH_MARK = "-";
	private final String TRUE = "true";
	
	StringBuilder command;
	char parameterDelimiter;
	char endCharacter;
	String switchMark;
	Properties params;
	
	/**
	 * Creates command object
	 * 
	 * @param command input command as string
	 * @param parameterDelimiter char, which will be used as parameter delimiter
	 * @param endCharacter char, which will be used as last char
	 */
	public CommandBuilder(String command, char parameterDelimiter, char endCharacter){
		this.command = new StringBuilder(command);
		this.parameterDelimiter = parameterDelimiter;
		this.endCharacter = endCharacter;
		this.switchMark = DEFAULT_SWITCH_MARK;
	}

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
	 * Creates command object
	 * 
	 * @param command input command as string
	 * @param switchMark String, which will be used as switchMark; for example "-" or "--"
	 */
	public CommandBuilder(String command, String switchMark){
		this(command);
		this.switchMark = switchMark;
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
	 * 	" paramName=paramValue"
	 * if doesn't exist:
	 * 	" paramName=defaultValue"
	 * 
	 * @param paramName
	 * @param defaultValue
	 */
	public void addParameter(String paramName, String defaultValue){
		command.append(parameterDelimiter);
		command.append(paramName);
		command.append(EQUAL_CHAR);
		command.append(params.getProperty(paramName, defaultValue));
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
	 * if paramName is in properties adds to the end of command: 
	 *  " <i><b>switchMark</b>switchChar</i>paramValue"<br>
	 *  for exmaple:  -P"password"
	 * 
	 * @param paramName
	 * @param switchChar
	 */
	public void addParameterSwitch(String paramName, char switchChar) {
		if (params.containsKey(paramName)) {
			command.append(parameterDelimiter);
			command.append(switchMark);
			command.append(switchChar);
			command.append(StringUtils.quote(params.getProperty(paramName)));
		}
	}
	
	/**
	 * if value isn;t null adds to the end of command: 
	 *  " <i><b>switchMark</b>switchChar</i>value"<br>
	 *  for exmaple:  -P"password"
	 *  
	 *  
	 * @param paramName
	 * @param switchChar
	 */
	public void addSwitch(char switchChar, String value) {
		if (value != null) {
			command.append(parameterDelimiter);
			command.append(switchMark);
			command.append(switchChar);
			command.append(StringUtils.quote(value));
		}
	}
	
	/**
	 * if paramName is in properties adds to the end of command: 
	 *  " <i><b>switchMark</b>switchChar</i>"<br>
	 *  for exmaple:  -P
	 * 
	 * @param paramName
	 * @param switchChar
	 */
	public void addParameterBooleanSwitch(String paramName, char switchChar) {
		addParameterBooleanSwitch(paramName, String.valueOf(switchChar));
	}
	
	/**
	 * if paramValue isn't null or paramName is in properties adds to the end of command:
	 *  " <i><b>switchMark</b>switchChar</i>paramValue"<br>
	 *  for exmaple:  --host="localhost"
	 * 
	 * @param paramName
	 * @param switchString
	 */
	public void addParameterSwitchWithEqualChar(String paramName, String switchString, String paramValue) {
		if (paramValue == null && (paramName == null || !params.containsKey(paramName))) {
			return;
		}
		
		command.append(parameterDelimiter);
		command.append(switchMark);
		command.append(switchString);
		command.append(EQUAL_CHAR);
		if (paramValue != null) {
			command.append(StringUtils.specCharToString(paramValue));
		} else {
			command.append(StringUtils.specCharToString(params.getProperty(paramName)));
		}
	}
	
	/**
	 * if paramName is in properties adds to the end of command: 
	 *  " <i><b>switchMark</b>switchChar</i>"<br>
	 *  for exmaple:  --compress
	 * 
	 * @param paramName
	 * @param switchString
	 */
	public void addParameterBooleanSwitch(String paramName, String switchString) {
		if (params.containsKey(paramName) && !"false".equalsIgnoreCase(params.getProperty(paramName))) {
			command.append(parameterDelimiter);
			command.append(switchMark);
			command.append(switchString);
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
