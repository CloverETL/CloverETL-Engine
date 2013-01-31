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

import java.sql.ResultSet;

import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.JetelException;

/**
 * The implementation of JdbcSpecific, that isn't set nearly anything.
 *  
 * @author Miroslav Haupt (hauptm@javlin.cz)
 *         (c) Javlin a.s. (www.javlin.cz)
 *
 * @created 20.4.2008
 */
public class DefaultConservativeJdbcSpecific extends AbstractJdbcSpecific {

	private static final DefaultConservativeJdbcSpecific INSTANCE = new DefaultConservativeJdbcSpecific();

	public static DefaultConservativeJdbcSpecific getInstance() {
		return INSTANCE;
	}

	protected DefaultConservativeJdbcSpecific() {
		super();
	}

	@Override
	protected SqlConnection prepareSQLConnection(DBConnection dbConnection, OperationType operationType) throws JetelException {
		SqlConnection connection = super.prepareSQLConnection(dbConnection, operationType);
		connection.setConservative(true);
		return connection;
	}

	@Override
	public void optimizeResultSet(ResultSet resultSet,
			OperationType operationType) {
		// EMPTY
	}
	
	

}
