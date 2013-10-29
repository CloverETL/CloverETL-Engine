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
package org.jetel.database.sql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * The aim of this interface together with delegating classes {@link ResultSetMetaData} and
 * {@link ParameterDbMetadata} is to have common interface for ResultSetMetaData and ParameterMetaData classes.
 */
public interface DbMetadata {
	/** Retrieves the designated SQL type of column/parameter */
	public int getType(int index) throws SQLException;
	/** Gets the designated column/parameter's number of digits to right of the decimal point */
	public int getScale(int index) throws SQLException;
	/** Get the designated column/parameter's specified column size. */
	public int getPrecision(int index) throws SQLException;
	/** Indicates the nullability of values in the designated column/parameter */
	public int isNullable(int index) throws SQLException;
}