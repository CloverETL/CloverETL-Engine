
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
	private final String TRUE = "true";
	
	StringBuilder command;
	char parameterDelimiter;
	char endCharacter;
	Properties params;
	
	public CommandBuilder(String command, char parameterDelimiter, char endCharacter){
		this.command = new StringBuilder(command);
		this.parameterDelimiter = parameterDelimiter;
		this.endCharacter = endCharacter;
	}

	public CommandBuilder(String command){
		this.command = new StringBuilder(command);
		this.parameterDelimiter = DEFAULT_PARAMETER_DELIMITER;
		this.endCharacter = DEFAULT_END_CHARACTER;
	}

	public Properties getParams() {
		return params;
	}

	public void setParams(Properties params) {
		this.params = params;
	}
	
	public String getCommand(){
		return command.toString() + endCharacter;
	}
	
	public void addParameter(String paramName) {
		if (params.containsKey(paramName)) {
			command.append(parameterDelimiter);
			command.append(paramName);
			command.append(EQUAL_CHAR);
			command.append(params.getProperty(paramName));
		}
	}
	
	public void addParameter(String paramName, String defaultValue){
		command.append(parameterDelimiter);
		command.append(paramName);
		command.append(EQUAL_CHAR);
		command.append(params.getProperty(paramName, defaultValue));
	}
	
	/**
	 * adds: [prefix] paramName=paramValue
	 * 
	 * @param prefix
	 * @param addPrefix
	 * @param paramName
	 */
	public boolean addParameterWithPrefixClauseConditionaly(String prefix, boolean addPrefix, 
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
	 * adds: [prefix] paramNameparamValue	 
	 *
	 * @param prefix
	 * @param addPrefix
	 * @param paramName
	 * @return
	 */
	public boolean addParameterSpecialWithPrefixClauseConditionaly(String prefix, boolean addPrefix, 
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
	 * adds: [prefix] paramName="paramValue"
	 * 
	 * @param prefix
	 * @param addPrefix
	 * @param paramName
	 * @return
	 */
	public boolean addAndQuoteParameterWithPrefixClauseConditionaly(String prefix, boolean addPrefix, 
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
	 * adds: [prefix] paramName
	 * 
	 * @param prefix
	 * @param addPrefix
	 * @param paramName
	 * @return true if prefix was written (= paramName found and has "true" value)
	 */
	public boolean addBooleanParameterWithPrefixClauseConditionaly(String prefix, boolean addPrefix, 
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
	 * adds: prefixparamValue
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
	
	public void addBooleanParameter(String paramName){
		if (params.containsKey(paramName) && params.getProperty(paramName).equalsIgnoreCase(TRUE)) {
			command.append(parameterDelimiter);
			command.append(paramName);
		}
	}
	
	public void addParameterSpecial(String paramName, String equalChar){
		if (params.containsKey(paramName)) {
			command.append(parameterDelimiter);
			command.append(paramName);
			command.append(equalChar);
			command.append(params.getProperty(paramName));
		}
	}
	
	public void append(String str){
		command.append(str);
	}
	
	@Override
	public String toString() {
		return command.toString();
	}
}
