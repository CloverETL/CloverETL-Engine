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

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.specific.conn.DefaultConnection;
import org.jetel.connection.jdbc.specific.conn.MSSQLConnection;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;

/**
 * MS SQL 2005 specific behaviour.
 * 
 * This specific works on SQL Server 2005 and older primarily,
 * although it should work also on newer versions
 * 
 * @author Pavel Najvar (pavel.najvar@javlin.eu)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Mar 19, 2009
 */
public class MSSQLSpecific2005 extends AbstractMSSQLSpecific {

	private static final MSSQLSpecific2005 INSTANCE = new MSSQLSpecific2005();

	public static MSSQLSpecific2005 getInstance() {
		return INSTANCE;
	}
	
	protected MSSQLSpecific2005() {
		super();
	}

	@Override
	protected DefaultConnection prepareSQLConnection(DBConnection dbConnection, OperationType operationType) throws JetelException {
		return new MSSQLConnection(dbConnection, operationType);
	}

	@Override
	public int jetelType2sql(DataFieldMetadata field) {
		switch (field.getType()) {
		case DataFieldMetadata.DATE_FIELD:
			return Types.TIMESTAMP;
		case DataFieldMetadata.BOOLEAN_FIELD:
			return Types.BIT;
		case DataFieldMetadata.NUMERIC_FIELD:
			return Types.DOUBLE;
		default:
	return super.jetelType2sql(field);
		}
	}

    @Override
    public ArrayList<String> getSchemas(java.sql.Connection connection)
        throws SQLException {
      ArrayList <String> currentCatalog = new ArrayList<String>();
      currentCatalog.add(connection.getCatalog());
      return currentCatalog;
    }
}
