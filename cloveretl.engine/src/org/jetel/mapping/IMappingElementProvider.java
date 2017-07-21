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
package org.jetel.mapping;


/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 12.3.2008
 */
public interface IMappingElementProvider {

	/**
	 * Creates source mapping element.
	 * 
	 * @param rawSource - source string
	 * @return source mapping element instance
	 * @throws MappingException
	 */
	public MappingSource createMappingSource(String rawSource) throws MappingException;

	/**
	 * Creates target mapping element.
	 * 
	 * @param rawTarget - target string
	 * @return target mapping element instance
	 * @throws MappingException
	 */
	public MappingTarget createMappingTarget(String rawTarget) throws MappingException;

}
