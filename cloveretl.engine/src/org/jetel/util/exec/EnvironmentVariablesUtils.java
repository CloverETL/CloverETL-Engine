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
package org.jetel.util.exec;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.util.string.StringUtils;

/**
 * A class for handling environment variable property of components SystemExecute and
 * ExecuteShellScript
 * 
 * @author Pavel Simecek (pavel.simecek@javlin.eu)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.7.2012
 */
public class EnvironmentVariablesUtils {

	public static class EnvironmentVariableUpdate {
		public final String value;
		public final AssignmentType assignmentType;
		
		public EnvironmentVariableUpdate(String value, AssignmentType assignmentType) {
			this.value = value;
			this.assignmentType = assignmentType;
		}
	}
	
	public enum AssignmentType {
		NORMAL,
		APPENDING;
		
		private static final String NORMAL_ENV_VARIABLES_ASSIGNMENT = "=";
		private static final String APPENDING_ENV_VARIABLES_ASSIGNMENT = "+=";
		
		public static AssignmentType parseAssignment(String assignmentString) {
			if (APPENDING_ENV_VARIABLES_ASSIGNMENT.equals(assignmentString)) {
				return APPENDING;
			} else if (NORMAL_ENV_VARIABLES_ASSIGNMENT.equals(assignmentString)) {
				return NORMAL;
			} else {
				return null;
			}
		}
		
		@Override
		public String toString() {
			if (this==NORMAL) {
				return NORMAL_ENV_VARIABLES_ASSIGNMENT;
			} else {
				return APPENDING_ENV_VARIABLES_ASSIGNMENT;
			}
		}
	}
	
	public static final Pattern ENV_VARIABLES_ASSIGNMENT_PATTERN = Pattern.compile(Pattern.quote(AssignmentType.APPENDING.toString()) + "|" + Pattern.quote(AssignmentType.NORMAL.toString()));

	public static class EnvironmentVariable {
		public final String name;
		public final String value;
		public final Boolean appending;
		
		public EnvironmentVariable(String name, String value, Boolean appending) {
			this.name = name;
			this.value = value;
			this.appending = appending;
		}
	}
	
	public static class InvalidEnvironmentVariableDefinition extends Exception {
		private static final long serialVersionUID = 3280227189687406634L;
		public static enum ErrorSource { KEY, VALUE, UNKNOWN};
		private final ErrorSource errorSource;
		private final String name;

		public InvalidEnvironmentVariableDefinition(ErrorSource errorSource, String name) {
			super();
			this.errorSource = errorSource;
			this.name = name;
		}
		
		public InvalidEnvironmentVariableDefinition() {
			super();
			errorSource = ErrorSource.UNKNOWN;
			name = null;
		}

		/**
		 * @return the errorSource
		 */
		public ErrorSource getErrorSource() {
			return errorSource;
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

	}
	
	public static class ParsedAssignment {
		public final String variableName;
		public final AssignmentType assignmentType;
		public final String variableValue;
		
		public ParsedAssignment(String variableName, AssignmentType assignmentType, String variableValue) {
			this.variableName = variableName;
			this.assignmentType = assignmentType;
			this.variableValue = variableValue;
		}
	}
	
	public static ParsedAssignment parseVariableAssignment(String assignmentString) {
		Matcher assignmentMatcher = ENV_VARIABLES_ASSIGNMENT_PATTERN.matcher(assignmentString);
		if (assignmentMatcher.find()) {
			String variableName = assignmentString.substring(0,assignmentMatcher.start()).trim();
			String assignmentTypeString = assignmentString.substring(assignmentMatcher.start(), assignmentMatcher.end()).trim();
			AssignmentType assignmentType = AssignmentType.parseAssignment(assignmentTypeString);
			String variableValue = StringUtils.unquote(assignmentString.substring(assignmentMatcher.end()).trim());
			return new ParsedAssignment(variableName, assignmentType, variableValue);
		} else {
			return null;
		}
	}
	
	public static void setEnvironmentVariables(Map<String, EnvironmentVariableUpdate> environmentVariables, String string) throws InvalidEnvironmentVariableDefinition {
		String[] env = StringUtils.split(string);
		for (int i = 0; i < env.length; i++) {
			ParsedAssignment parsedAssignment = parseVariableAssignment(env[i]);
			if (parsedAssignment!=null) {
				environmentVariables.put(parsedAssignment.variableName, new EnvironmentVariableUpdate(parsedAssignment.variableValue, parsedAssignment.assignmentType));
			} else {
				throw new InvalidEnvironmentVariableDefinition();
			}
		}
	}
	
	

}
