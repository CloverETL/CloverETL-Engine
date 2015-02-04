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
public class RefResFlag {

	/**
	 * Regular resolving type - special characters and ctl expressions are turned on, secure parameters are turned off.
	 * Regular parameters are resolved all the time.
	 */
	public static final RefResFlag REGULAR = new RefResFlag(true, true, false);

	/**
	 * Everything is turned off - only parameters are resolved.
	 */
	public static final RefResFlag ALL_OFF = new RefResFlag(false, false, false);

	/**
	 * Everything is turned off - only ctl expressions are turned on.
	 * Regular parameters are resolved all the time.
	 */
	public static final RefResFlag SPEC_CHARACTERS_OFF = new RefResFlag(false, true, false);

	/**
	 * Everything is turned off - only special characters are turned on.
	 * Regular parameters are resolved all the time.
	 */
	public static final RefResFlag CTL_EXPRESSIONS_OFF = new RefResFlag(true, false, false);

	/**
	 * Everything is turned off - only secure parameters are turned on.
	 * Regular parameters are resolved all the time.
	 */
	public static final RefResFlag SECURE_PARAMATERS = SPEC_CHARACTERS_OFF.resolveSecureParameters(true);

	/**
	 * Special flag for URL resolution. Only ctl expressions and secure parameters are resolved. 
	 * Regular parameters are resolved all the time.
	 */
	public static final RefResFlag URL = SPEC_CHARACTERS_OFF.resolveSecureParameters(true);

	/** Should be special characters resolved? */
	private boolean specCharacters;
	/** Should be ctl expressions resolved? */
	private boolean ctlStatements;
	/** Should be secure parameters resolved? */
	private boolean secureParameters;
	/** Should be unresolved secure parameters reported as an error?
	 * If secureParameters=false, but a reference to a secure parameter
	 * is found in resolved text, an exception is thrown by resolver.
	 * This flag could avoid this default behaviour. */
	private boolean forceSecureParameters;
	
	private RefResFlag(boolean specCharacters, boolean ctlStatements, boolean secureParamaters) {
		this.specCharacters = specCharacters;
		this.ctlStatements = ctlStatements;
		this.secureParameters = secureParamaters;
		this.forceSecureParameters = true;
	}
	private RefResFlag(RefResFlag flag) {
		this.specCharacters = flag.specCharacters;
		this.ctlStatements = flag.ctlStatements;
		this.secureParameters = flag.secureParameters;
		this.forceSecureParameters = flag.forceSecureParameters;
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
	
	/** Should be ctl expressions resolved? */
	public boolean resolveCTLStatements() {
		return ctlStatements;
	}

	/** Should be ctl expressions resolved? */
	public RefResFlag resolveCTLStatements(boolean ctlStatements) {
		RefResFlag result = new RefResFlag(this);
		result.ctlStatements = ctlStatements;
		return result;
	}

	/** Should be secure parameters resolved? */
	public boolean resolveSecureParameters() {
		return secureParameters;
	}

	/** Should be secure parameters resolved? */
	public RefResFlag resolveSecureParameters(boolean secureParameters) {
		RefResFlag result = new RefResFlag(this);
		result.secureParameters = secureParameters;
		return result;
	}

	/** Should be unresolved secure parameters reported as an error? */
	public boolean forceSecureParameters() {
		return forceSecureParameters;
	}

	/** Should be unresolved secure parameters reported as an error? */
	public RefResFlag forceSecureParameters(boolean forceSecureParameters) {
		RefResFlag result = new RefResFlag(this);
		result.forceSecureParameters = forceSecureParameters;
		return result;
	}

}
