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
package org.jetel.util.property;

/**
 * This object is used to distinguish various type of property referecence resolvinng 
 * in the PorpertyRefResolver.
 * Currently are available just one boolean switches 
 * - resolve special characters (\n, \r, \t, ...)
 * 
 * @see PropertyRefResolver
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 22.9.2009
 */
public class RefResFlag {

	/**
	 * Regular resolving type - special characters and ctl expressions are turned on, secure parameters are turned off.
	 * Regular parameters are resolved all the time.
	 */
	public static final RefResFlag REGULAR = new RefResFlag(true);

	/**
	 * Everything is turned off - only parameters are resolved.
	 */
	public static final RefResFlag ALL_OFF = new RefResFlag(false);

	/**
	 * Everything is turned off - only ctl expressions are turned on.
	 */
	public static final RefResFlag SPEC_CHARACTERS_OFF = new RefResFlag(false);

	/**
	 * Special flag for URL resolution. 
	 */
	public static final RefResFlag URL = SPEC_CHARACTERS_OFF;

	/**
	 * Special flag for passwords resolution.
	 */
	public static final RefResFlag PASSWORD = SPEC_CHARACTERS_OFF;

	/** Should be special characters resolved? */
	private boolean specCharacters;
	
	private RefResFlag(boolean specCharacters) {
		this.specCharacters = specCharacters;
	}

	private RefResFlag(RefResFlag flag) {
		this.specCharacters = flag.specCharacters;
	}
	
	/** Should be special characters resolved? */
	public boolean resolveSpecCharacters() {
		return specCharacters;
	}

	public RefResFlag resolveSpecCharacters(boolean specCharacters) {
		RefResFlag result = new RefResFlag(this);
		result.specCharacters = specCharacters;
		return result;
	}
	
}
