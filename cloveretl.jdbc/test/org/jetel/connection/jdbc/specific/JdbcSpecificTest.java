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
package org.jetel.connection.jdbc.specific;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.jetel.connection.jdbc.DBConnectionImpl;
import org.jetel.connection.jdbc.driver.JdbcDriverFactory;
import org.jetel.connection.jdbc.specific.conn.MySQLConnection;
import org.jetel.database.sql.JdbcDriver;
import org.jetel.database.sql.JdbcSpecific;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19 Apr 2012
 */
public class JdbcSpecificTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}

	public void testWrapSQLConnection() throws ComponentNotReadyException, SQLException, JetelException {
		Properties mysqlLogin = new Properties();
		mysqlLogin.setProperty("user", "test");
		mysqlLogin.setProperty("password", "test");
		JdbcDriver mysqlDriver = JdbcDriverFactory.createInstance(JdbcDriverFactory.getJdbcDriverDescriptor("MYSQL"));
		Connection mysqlConnection = mysqlDriver.getDriver().connect("jdbc:mysql://koule:3306/test", mysqlLogin);
		
		JdbcSpecific mysqlSpecific = JdbcSpecificFactory.getJdbcSpecificDescription("MYSQL").getJdbcSpecific();
		
		Connection connection = mysqlSpecific.createSQLConnection(new DBConnectionImpl("id", mysqlLogin), mysqlConnection, OperationType.READ);
		assertTrue(connection instanceof MySQLConnection);
		MySQLConnection wrapperMysqlConnection = (MySQLConnection) connection;
		
		boolean autoCommit = mysqlConnection.getAutoCommit();
		assertEquals(autoCommit, wrapperMysqlConnection.getAutoCommit());
		
		mysqlConnection.setAutoCommit(!autoCommit);
		assertEquals(!autoCommit, wrapperMysqlConnection.getAutoCommit());
	}
	
}
