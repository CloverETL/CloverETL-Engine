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
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Properties;

import org.jetel.connection.jdbc.specific.conn.MSAccessPureJavaConnection;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.graph.Node;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Specific for pure java driver for ms access
 * @author salamonp (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 15. 7. 2015
 */
public class MSAccessPureJavaSpecific extends AbstractJdbcSpecific {
	
	private static final MSAccessPureJavaSpecific INSTANCE = new MSAccessPureJavaSpecific();

	private static final String CONVERT_STRING = "Convert the field to another type or use another matching field type.";
	
	private static final String SHOW_SCHEMA_PROPERTY = "showschema";

	protected MSAccessPureJavaSpecific() {
		super();
	}

	public static MSAccessPureJavaSpecific getInstance() {
		return INSTANCE;
	}
	
	@Override
	public Connection connect(Driver driver, String url, Properties info) throws SQLException {
		boolean schemaPropertyPresent = url.toLowerCase().replaceAll("\\s", "").contains(SHOW_SCHEMA_PROPERTY + "=");
		
		if (!schemaPropertyPresent) {
			// search the custom properties
			for (Object key : info.keySet()) {
				if (key.toString().toLowerCase().equals(SHOW_SCHEMA_PROPERTY)) {
					schemaPropertyPresent = true;
					break;
				}
			}
		}
		if (!schemaPropertyPresent) {
			info.put(SHOW_SCHEMA_PROPERTY, "true"); // needed for browsing, metadata extraction
		}
		
		return super.connect(driver, url, info);
	}
	
	@Override
	public ClassLoader getDriverClassLoaderParent() {
		// ucanaccess needs commons-logging that is on clover classpath
		return Thread.currentThread().getContextClassLoader();
	}

	@Override
	public SqlConnection createSQLConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		return new MSAccessPureJavaConnection(dbConnection, connection, operationType);
	}

	@Override
	public String getDbFieldPattern() {
		// allows white spaces
		return "([\\s\\p{Alnum}\\._]+)|([\"\'][\\s\\p{Alnum}\\._ ]+[\"\'])";
	}

	@Override
	public String quoteString(String string) {
		return quoteIdentifier(string);
	}

	@Override
	public String quoteIdentifier(String identifier) {
		return ('[' + identifier + ']');
	}

	@Override
	public ConfigurationStatus checkMetadata(ConfigurationStatus status, Collection<DataRecordMetadata> metadata, Node node) {
		for (DataRecordMetadata dataRecordMetadata : metadata) {
			for (DataFieldMetadata dataField : dataRecordMetadata.getFields()) {
				switch (dataField.getDataType()) {
				case LONG:
					status.add(new ConfigurationProblem("Metadata on input port must not use field of type long " + "because of restrictions of used driver." + CONVERT_STRING, ConfigurationStatus.Severity.ERROR, node, ConfigurationStatus.Priority.NORMAL));
					break;
				default:
					break;
				}
			}
		}
		return status;
	}

	@Override
	public String sqlType2str(int sqlType) {
		switch (sqlType) {
		case Types.TIMESTAMP:
			return "DATETIME";
		}
		return super.sqlType2str(sqlType);
	}

	@Override
	public int jetelType2sql(DataFieldMetadata field) {
		switch (field.getDataType()) {
		case NUMBER:
			return Types.DOUBLE;
		default:
			return super.jetelType2sql(field);
		}
	}

	@Override
	public char sqlType2jetel(int sqlType) {
		switch (sqlType) {
		case Types.BIT:
			return DataFieldType.BOOLEAN.getShortName();
		default:
			return super.sqlType2jetel(sqlType);
		}
	}

	@Override
	public String getTablePrefix(String schema, String owner, boolean quoteIdentifiers) {
		String tablePrefix;
		String notNullOwner = (owner == null) ? "" : owner;
		if (quoteIdentifiers) {
			tablePrefix = quoteIdentifier(schema);
			// in case when owner is empty or null skip adding
			if (!notNullOwner.isEmpty()) {
				tablePrefix += quoteIdentifier(notNullOwner);
			}
		} else {
			tablePrefix = notNullOwner.isEmpty() ? schema : (schema + "." + notNullOwner);
		}
		return tablePrefix;
	}

}