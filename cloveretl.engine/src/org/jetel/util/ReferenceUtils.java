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
package org.jetel.util;

import org.jetel.exception.JetelRuntimeException;

/**
 * This class is preliminary implementation of graph element referencing.
 * Some kind of URLs in clover world.
 * 
 * Current implementation supports only relative reference inside single graph
 * and can be changed in the future.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21. 8. 2014
 */
public class ReferenceUtils {

	private static final String RELATIVE_REFERENCE_PREFIX = "#//";
	
	/**
	 * @return string "#//" + elementId
	 */
	public static String getRelativeReference(String elementId) {
		return RELATIVE_REFERENCE_PREFIX + elementId;
	}

	/**
	 * Expects relative reference in format "#//elementId".
	 * The elementId string is returned
	 */
	public static String getElementID(String relativeReference) {
		if (relativeReference.startsWith(RELATIVE_REFERENCE_PREFIX)) {
			return relativeReference.substring(RELATIVE_REFERENCE_PREFIX.length());
		} else {
			throw new JetelRuntimeException("Invalid element reference '" + relativeReference + "'");
		}
	}
	
}
