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
package org.jetel.component.aggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Registry of aggregation functions.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Apr 18, 2007
 */
public class FunctionRegistry {
	// registry of available aggregate functions, key is the function name (lowercase)
	private Map<String, Class<? extends AggregateFunction>> functions = 
		new HashMap<String, Class<? extends AggregateFunction>>();

	/**
	 * 
	 * Allocates a new <tt>FunctionRegistry</tt> object.
	 *
	 */
	public FunctionRegistry() {
		registerFunction(new Count());
		registerFunction(new Min());
		registerFunction(new Max());
		registerFunction(new Sum());
		registerFunction(new First());
		registerFunction(new Last());
		registerFunction(new Avg());
		registerFunction(new StdDev());
		registerFunction(new CRC32());
		registerFunction(new MD5());
		registerFunction(new FirstNonNull());
		registerFunction(new LastNonNull());
	}

	/**
	 * 
	 * @param name name of the aggregation function, case-insensitive.
	 * @return aggregation function from the registry.
	 */
	public Class<? extends AggregateFunction> getFunction(String name) {
		return functions.get(name.toLowerCase());
	}
	
	/**
	 * 
	 * @return names of all registered aggregation functions.
	 */
	public List<String> getFunctionNames() {
		return new ArrayList<String>(functions.keySet());
	}

	/**
	 * Registers an aggregation function, i.e. makes it available for use during aggregation.
	 * @param f
	 */
	private void registerFunction(AggregateFunction f) {
		if (getFunction(f.getName()) != null) {
			throw new IllegalArgumentException("Aggregate function already registered: " + f.getName());
		}
		addFunction(f.getName(), f.getClass());
	}
	
	/**
	 * Adds an aggregation function to the registry.
	 * 
	 * @param name name
	 * @param f
	 */
	private void addFunction(String name, Class<? extends AggregateFunction> f) {
		functions.put(name.toLowerCase(), f);
	}
}
