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
package org.jetel.connection.jdbc.specific.impl;

import java.sql.Connection;

import org.jetel.connection.jdbc.specific.conn.RedshiftConnection;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;

/**
 * CLO-7037:
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 10. 8. 2015
 */
public class RedshiftSpecific extends PostgreSpecific {

	private static final RedshiftSpecific INSTANCE = new RedshiftSpecific();
	
	public static RedshiftSpecific getInstance() {
		return INSTANCE;
	}

	@Override
	public SqlConnection createSQLConnection(DBConnection dbConnection, Connection connection,
			OperationType operationType) throws JetelException {
		return new RedshiftConnection(dbConnection, connection, operationType);
	}

	@Override
	public int jetelType2sql(DataFieldMetadata field) {
		switch (field.getDataType()) {
		case BYTE:
		case CBYTE:
			throw new IllegalArgumentException("Can't handle Clover's data type: " + field.getDataType().getName());
		default:
			return super.jetelType2sql(field);
		}
	}
	
	@Override
	public ClassLoader getDriverClassLoaderParent() {
		//this parent classloader is necessary for correct reporting of issues from redshift driver
		return Thread.currentThread().getContextClassLoader();
	}
	
}
