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
package org.jetel.exception;

/** Exception which occures when name of graph element (Node, Edge, Metadata etc.)
 * violates following pattern [A-Za-z0-9_].
 * 
 * @see Exception
 * @author D.Pavlis
 */
public class InvalidGraphObjectNameException extends RuntimeException {

	private static final long serialVersionUID = -3542689167346956113L;

	// Attributes

	// Associations

	// Operations
	
	public InvalidGraphObjectNameException(String objectName, String objectType) {
		super("Graph object " + objectType + " named \"" + objectName + "\" violates naming pattern [A-Za-z0-9_] !");
	}

} /* end class InvalidGraphObjectNameException */
