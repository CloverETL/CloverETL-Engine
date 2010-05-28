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
package org.jetel.ctl;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.ASTnode.CLVFVariableDeclaration;
import org.jetel.ctl.data.Scope;

/**
 * 
 * Class for management of symbols and symbol tables.
 * Stores information about symbols declared in specific scope as well as information
 * about local declarations of functions.
 * 
 * @author David Pavlis <david.pavlis@javlin.cz>
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 * @since 28.5.2006
 * 
 */
public class ParserHelper {

	private Scope currentScope;
	private TreeMap<String,List<CLVFFunctionDeclaration>> declaredFunctions = new TreeMap<String,List<CLVFFunctionDeclaration>>();
	private int blockOffset = 0; 
	private int variableOffset = 0;

	public CLVFVariableDeclaration addVariable(CLVFVariableDeclaration var) {
		var.setVariableOffset(variableOffset++);
		var.setBlockOffset(currentScope.getBlockOffset());
		return currentScope.put(var.getName(), var);
	}

	
	/**
	 * Registers function as a new code-block and creates a separate scope for it.
	 * Stores newly allocated scope in the function node.
	 * 
	 * @param node	function node to register
	 * @return false if duplicate declaration; true otherwise
	 */
	public boolean enteredBlock(CLVFFunctionDeclaration node) {
		currentScope = new Scope(currentScope,nextBlockOffset());
		node.setScope(currentScope);

		List<CLVFFunctionDeclaration> value = declaredFunctions.get(node.getName());
		if (value == null) {
			value = new LinkedList<CLVFFunctionDeclaration>();
			declaredFunctions.put(node.getName(),value);
		}
		
		value.add(node);
		
		return true;
	}
	
	/**
	 * Registers an anonymous code-block (if/else/loop body) and allocates a separate
	 * scope for it
	 * @return allocated scope
	 */
	public Scope enteredBlock() {
		currentScope = new Scope(currentScope,nextBlockOffset());
		return currentScope;
	}
	

	public void exitedBlock() {
		// renew the previous scope
		currentScope = currentScope.getParent();
		// the global scope has no parent - so it is null
		variableOffset = currentScope != null ? currentScope.size() : 0; 
		blockOffset--;
	}

	public CLVFVariableDeclaration getVariable(String name) {
		for (Scope scope = currentScope; scope != null; scope = scope.getParent()) {
			CLVFVariableDeclaration decl = scope.get(name);
			if (decl != null) {
				return decl;
			}
		}
		
		return null;
	}
	
	public boolean getFunction(String name) {
		return declaredFunctions.containsKey(name);
	}
	
	public Map<String,List<CLVFFunctionDeclaration>> getFunctions() {
		return declaredFunctions;
	}
	
	public void reset() {
		currentScope = null;
		declaredFunctions.clear();
		blockOffset = 0;
		variableOffset = 0;
	}

	/**
	 * @return Current block offset (offset used by current scope)
	 */
	public int getCurrentBlockOffset() {
		return currentScope.getBlockOffset();
	}
	
	/**
	 * @return block index (depth) starting from 0 for global scope (>0 for any local scope)
	 */
	private final int nextBlockOffset() {
		variableOffset = 0;
		return blockOffset++;
	}
	
	
	
}
