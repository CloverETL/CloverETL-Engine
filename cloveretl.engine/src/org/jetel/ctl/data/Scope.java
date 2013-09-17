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
/**
 * 
 */
package org.jetel.ctl.data;

import java.util.HashMap;
import java.util.Map;

import org.jetel.ctl.ASTnode.CLVFVariableDeclaration;

public class Scope {

	static final int INITIAL_HASH_MAP_SIZE = 16;
	
	private final int blockIndex;
	private Scope parent;
	private Map<String,CLVFVariableDeclaration> symbols;

	public Scope(Scope parent, int blockIndex) {
		this.blockIndex = blockIndex;
		this.parent = parent;
		this.symbols = new HashMap<String,CLVFVariableDeclaration>(INITIAL_HASH_MAP_SIZE);
	}
	
	public CLVFVariableDeclaration put(String name, CLVFVariableDeclaration var) {
		return symbols.put(name,var);
	}
	
	public CLVFVariableDeclaration get(String name) {
		return symbols.get(name);
	}
	
	public Map<String,CLVFVariableDeclaration> getSymbols() {
		return symbols;
	}
	
	public Scope getParent() {
		return parent;
	}
	
	public int getBlockOffset() {
		return blockIndex;
	}
	
	public int size() {
		return symbols.size();
	}
	
}