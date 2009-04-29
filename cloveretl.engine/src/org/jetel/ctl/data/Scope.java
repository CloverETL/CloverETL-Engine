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