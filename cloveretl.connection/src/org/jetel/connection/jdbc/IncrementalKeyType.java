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
package org.jetel.connection.jdbc;

/**
 * Types of database incremental key
 * 
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Jul 24, 2008
 */
public enum IncrementalKeyType {
	FIRST,//first record from result set
	LAST,//last record from result set
	MAX,//maximal value from result set
	MIN;//minimal value from result set
	
	/**
	 * @return pattern for parsing key definition (all values delimited by | )
	 */
	public static String getKeyTypePattern(){
		StringBuilder pattern = new StringBuilder();
		for (IncrementalKeyType type : IncrementalKeyType.values()) {
			pattern.append(type);
			pattern.append('|');
		}
		return pattern.substring(0, pattern.length() -1);
	}
}
