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
package org.jetel.component.validator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetel.component.validator.params.ValidationParamNode;

/**
 * Accumulator for errors gathered during {@link ValidationNode#isReady(org.jetel.metadata.DataRecordMetadata, ReadynessErrorAcumulator, GraphWrapper)}
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 1.3.2013
 * @see ValidationNode#isReady(org.jetel.metadata.DataRecordMetadata, ReadynessErrorAcumulator, GraphWrapper)
 */
public class ReadynessErrorAcumulator {
	private Map<ValidationParamNode, List<String>> errors = new HashMap<ValidationParamNode, List<String>>();
	private Map<ValidationParamNode, ValidationNode> parents = new HashMap<ValidationParamNode, ValidationNode>();
	
	/**
	 * Adds error
	 * @param node Validation param node which contains error
	 * @param parent Parent validation node of param node
	 * @param message Message to be shown to the user
	 */
	public void addError(ValidationParamNode node, ValidationNode parent, String message) {
		if(!hasErrors(node)) {
			errors.put(node, new ArrayList<String>());
		}
		if(!parents.containsKey(node)) {
			parents.put(node, parent);
		}
		errors.get(node).add(message);
	}
	
	/**
	 * Checks wheter this accumulator has any error for given validation param node
	 * @param node Validation node for which the check is done
	 * @return True if there are any errors, false otherwise
	 */
	public boolean hasErrors(ValidationParamNode node) {
		return errors.containsKey(node);
	}
	
	/**
	 * Returns all errors for given validation param node
	 * @param node Validation param node for which errors are wanted
	 * @return List of all errors for some validation param node
	 */
	public List<String> getErrors(ValidationParamNode node) {
		return errors.get(node);
	}
	
	/**
	 * List of all errors associated with their param nodes
	 * @return Map of params nodes and theirs errors
	 */
	public Map<ValidationParamNode, List<String>> getErrors() {
		return errors;
	}
	
	/**
	 * Return parent validation node of given param node
	 * @param node Param node to find its parent
	 * @return Parent validation node
	 */
	public ValidationNode getParentRule(ValidationParamNode node) {
		return parents.get(node);
	}
	
	/**
	 * Return all errors in strings delimited by newline
	 * @return All errors
	 */
	public String getAllErrorsInString() {
		StringBuilder output = new StringBuilder();
		for(List<String> temp : errors.values()) {
			for(String temp2 : temp) {
				output.append(temp2);
				output.append("\n");
			}
		}
		return output.toString().trim();
	}
	
	/**
	 * Clean all errors
	 */
	public void reset() {
		errors.clear();
		parents.clear();
	}
}
