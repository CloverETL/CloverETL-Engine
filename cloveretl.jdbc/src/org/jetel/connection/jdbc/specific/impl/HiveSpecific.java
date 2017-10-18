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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Properties;

import org.jetel.connection.jdbc.specific.conn.HiveConnection;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.JetelException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.plugin.Plugins;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.string.StringUtils;

/**
 * JDBC Specific for Apache Hive -- the data warehouse system for Hadoop.
 * 
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26.10.2012
 */
public class HiveSpecific extends AbstractJdbcSpecific {

	private static final HiveSpecific INSTANCE = new HiveSpecific();
	
	private static final String HADOOP_PLUGIN_ID = "org.jetel.hadoop";
	private static final String HADOOP_PROVIDER_JAR = "./lib/cloveretl.hadoop.provider.jar";
	private static final String KERBEROS_UTILS_CLASS = "org.jetel.hadoop.provider.utils.KerberosUtils";
	private static final String KERBEROS_HELPER_METHOD = "doAs";
	private static final String JDBC_USER_PROPERTY = "user";
	
	private static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";

	public static HiveSpecific getInstance() {
		return INSTANCE;
	}

	protected HiveSpecific() {
		super();
	}
	
	private Connection doConnect(Driver driver, String url, Properties properties) throws SQLException {
		ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(driver.getClass().getClassLoader()); // prevent "Illegal Hadoop Version: Unknown (expected A.B.* format)"
			return super.connect(driver, url, properties);
		} finally {
			Thread.currentThread().setContextClassLoader(originalClassLoader);
		}
	}
	
	private boolean isKerberosAuthentication(String url) {
		return !StringUtils.isEmpty(url) && (url.contains(";principal=") || url.toLowerCase().contains("auth=kerberos"));
	}
	
	@Override
	public Connection connect(Driver driver, String url, Properties jdbcProperties) throws SQLException {
		if (isKerberosAuthentication(url)) {
			return connectUsingKerberos(driver, url, jdbcProperties);
		} else { // without Kerberos
			return doConnect(driver, url, jdbcProperties);
		}
	}

	private Connection connectUsingKerberos(final Driver driver, final String url, final Properties jdbcProperties) throws SQLException {
		Method helperMethod;
		try {
			helperMethod = getKerberosHelperMethod(driver);
		} catch (Exception e) {
			throw new SQLException("Failed to load helper classes for Kerberos authentication", e);
		}

		PrivilegedExceptionAction<Connection> connectAction = new PrivilegedExceptionAction<Connection>() {

			@Override
			public Connection run() throws Exception {
				return doConnect(driver, url, jdbcProperties);
			}
		}; 
		String user = jdbcProperties.getProperty(JDBC_USER_PROPERTY);
		Properties kerberosProperties = new Properties();
		kerberosProperties.putAll(jdbcProperties); // copy JDBC properties
		kerberosProperties.setProperty(HADOOP_SECURITY_AUTHENTICATION, "Kerberos"); // force enable Kerberos authentication
		
		try {
			return (Connection) helperMethod.invoke(null, connectAction, user, kerberosProperties); // invoke static helper method
		} catch (Exception e) {
			if (ExceptionUtils.instanceOf(e, SQLException.class)) { // unwrap SQLException if possible to get nice error messages
				List<SQLException> exceptions = ExceptionUtils.getAllExceptions(e, SQLException.class);
				if (!exceptions.isEmpty()) {
					throw exceptions.get(0);
				}
			}
			Throwable cause = (e instanceof InvocationTargetException) ? e.getCause() : e; // unwrap
			throw new SQLException(ExceptionUtils.getMessage("Hive connection with Kerberos authenticaton failed", cause), cause); // fallback
		}
	}

	private Method getKerberosHelperMethod(Driver driver) throws MalformedURLException, ClassNotFoundException, NoSuchMethodException {
		ClassLoader parent = driver.getClass().getClassLoader();
		URL hadoopProviderJar = Plugins.getPluginDescriptor(HADOOP_PLUGIN_ID).getURL(HADOOP_PROVIDER_JAR);
		URL[] urls = new URL[] { hadoopProviderJar };
		IAuthorityProxy authorityProxy = ContextProvider.getAuthorityProxy();
		ClassLoader cl = authorityProxy.getClassLoader(urls, parent, false);
		Class<?> kerberosUtilsClass = cl.loadClass(KERBEROS_UTILS_CLASS);
		Method doAsMethod = kerberosUtilsClass.getMethod(KERBEROS_HELPER_METHOD, PrivilegedExceptionAction.class, String.class, Properties.class);
		return doAsMethod;
	}

	@Override
	public SqlConnection createSQLConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		return new HiveConnection(dbConnection, connection, operationType);	
	}
	
	@Override
	public String sqlType2str(int sqlType) {
		switch (sqlType) {
		case Types.INTEGER:
			return "INT";
		case Types.VARCHAR:
			return "STRING";
		}

		return super.sqlType2str(sqlType);
	}
	
	@Override
	public String jetelType2sqlDDL(DataFieldMetadata field) {
		// Table column size cannot be specified in Hive (as is done in AbstractJdbcSpecific)
		return sqlType2str(jetelType2sql(field));
	}
	
	@Override
	public int jetelType2sql(DataFieldMetadata field) {
		switch (field.getDataType()) {
		case BYTE:
		case CBYTE:
			return Types.BINARY;
		case NUMBER:
			return Types.DOUBLE;
		default:
			return super.jetelType2sql(field);
		}
	}
	
	@Override
	public String getCreateTableSuffix(DataRecordMetadata metadata) {
		String delimiter = metadata.getFieldDelimiter();
		StringBuilder sb = new StringBuilder();
		sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ");
		sb.append("'");
		sb.append(delimiter);
		sb.append("'\n");
		sb.append("STORED AS TEXTFILE\n");
		return sb.toString();
	}
	
	@Override
	public ClassLoader getDriverClassLoaderParent() {
		/*
		 * Hive drivers depend on log4j & commons-logging, that are part of the clover classpath,
		 * so return class that has access to that classpath
		 */
		return Thread.currentThread().getContextClassLoader();
	}
	
	@Override
	public boolean supportsTerminatingSemicolons() {
		return false;
	}
}
