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
 * Currently are available just two boolean switches 
 * - resolve special characters (\n, \r, \t, ...)
 * - resolve ctl statements (`date2str(today(),"dd-MM-yyyy")`)
 * 
 * @see PropertyRefResolver
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 22.9.2009
 */
public enum RefResFlag {

	/**
	 * Regular resolving type - everything is turned on
	 */
	REGULAR,
	
	/**
	 * Special characters resolving is turned off.
	 */
	SPEC_CHARACTERS_OFF(false, true),
	
	/**
	 * CTL statements resolving is turned off.
	 */
	CTL_EXPRESSIONS_OFF(true, false),
	
	/**
	 * Both special characters and CTL statements resolving is turned off.
	 */
	ALL_OFF(false, false);

	private final static boolean DEFAULT_SPEC_CHARACTERS = true;
	private final static boolean DEFAULT_CTL_STATEMENTS = true;
	
	private boolean specCharacters;
	private boolean ctlStatements;
	
	private RefResFlag() {
		this(DEFAULT_SPEC_CHARACTERS, DEFAULT_CTL_STATEMENTS);
	}

	private RefResFlag(boolean specCharacters, boolean ctlStatements) {
		this.specCharacters = specCharacters;
		this.ctlStatements = ctlStatements;
	}
	
	public boolean resolveSpecCharacters() {
		return specCharacters;
	}
	
	public boolean resolveCTLstatements() {
		return ctlStatements;
	}
	
}
