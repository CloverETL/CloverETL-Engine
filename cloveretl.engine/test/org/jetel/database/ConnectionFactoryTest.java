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
package org.jetel.database;

import java.sql.SQLException;
import java.util.Properties;

import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.IGraphElement;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 31.1.2013
 */
public class ConnectionFactoryTest extends CloverTestCase {

    public static final String XML_NAME_ATTRIBUTE = "name";
    public static final String XML_USER_ATTRIBUTE = "user";
    public static final String XML_PASSWORD_ATTRIBUTE = "password";
    public final static String XML_JDBC_SPECIFIC_ATTRIBUTE = "jdbcSpecific";
    public static final String XML_DATABASE_ATTRIBUTE = "database"; // database type - used to lookup in build-in JDBC drivers
    public static final String XML_DBURL_ATTRIBUTE = "dbURL";

	public void testCreateConnection() throws ComponentNotReadyException, JetelException, SQLException {
		Properties properties = new Properties();
		properties.setProperty(IGraphElement.XML_ID_ATTRIBUTE, "connection1");
		properties.setProperty(XML_USER_ATTRIBUTE, "test");
		properties.setProperty(XML_PASSWORD_ATTRIBUTE, "test");
		properties.setProperty(XML_JDBC_SPECIFIC_ATTRIBUTE, "MYSQL");
		properties.setProperty(XML_DATABASE_ATTRIBUTE, "MYSQL");
		properties.setProperty(XML_DBURL_ATTRIBUTE, "jdbc:mysql://koule:3306/test");
		
		DBConnection connection = (DBConnection) ConnectionFactory.createConnection(null, "JDBC", properties);
		connection.init();
		
		assertEquals("test", connection.getUser());
		assertEquals("test", connection.getPassword());
		assertEquals("MYSQL", connection.getJdbcSpecific().getId());
		assertEquals("MYSQL", connection.getDatabase());
		assertEquals("jdbc:mysql://koule:3306/test", connection.getDbUrl());
		
		SqlConnection sqlConnection = connection.getConnection("xxx");
		assertTrue(sqlConnection.getSchemas().size() > 1);
	}
	
}
