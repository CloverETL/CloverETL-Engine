/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.database;

import java.sql.Types;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.util.List;
import java.util.LinkedList;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Various utilities for working with Databases
 *
 *@author     dpavlis
 *@created    January 24, 2003
 *@since      September 25, 2002
 */
public class SQLUtil {

	/**
	 *  Gets the FieldTypes attribute of the SQLUtil class
	 *
	 *@param  metadata          Description of Parameter
	 *@param  tableName         Description of Parameter
	 *@return                   The FieldTypes value
	 *@exception  SQLException  Description of Exception
	 *@since                    October 4, 2002
	 */
	public static List getFieldTypes(DatabaseMetaData metadata, String tableName) throws SQLException {
		ResultSet rs = metadata.getColumns(null, null, tableName, null);
		List fieldTypes = new LinkedList();
		while (rs.next()) {
			// get DATA TYPE - fifth column in result set from Database metadata
			fieldTypes.add(new Integer(rs.getInt(5)));
		}
		return fieldTypes;
	}

	public static List getFieldTypes(ParameterMetaData metadata) throws SQLException {
		List fieldTypes = new LinkedList();
		for(int i=1;i<=metadata.getParameterCount();i++){
			fieldTypes.add(new Integer(metadata.getParameterType(i)));
		}
		return fieldTypes;
	}

	/**
	 *  Converts SQL data type into Jetel data type
	 *
	 *@param  sqlType  JDBC SQL data type
	 *@return          corresponding Jetel data type
	 *@since           September 25, 2002
	 */
	public static char sqlType2jetel(int sqlType) {
		switch (sqlType) {
			case Types.INTEGER:
			case Types.SMALLINT:
				return DataFieldMetadata.INTEGER_FIELD;
			case Types.BIGINT:
			case Types.DECIMAL:
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.NUMERIC:
			case Types.REAL:
				return DataFieldMetadata.NUMERIC_FIELD;
			//------------------
			case Types.CHAR:
			case Types.LONGVARCHAR:
			case Types.VARCHAR:
				return DataFieldMetadata.STRING_FIELD;
			//------------------
			case Types.DATE:
			case Types.TIME:
			case Types.TIMESTAMP:
				return DataFieldMetadata.DATE_FIELD;
			default:
				return (char) -1;
			// unknown or not possible to translate
		}
	}


	/**
	 *  Converts Jetel/Clover datatype into String
	 *
	 *@param  fieldType  Jetel datatype
	 *@return            Corresponding string name
	 */
	public static String jetelType2Str(char fieldType) {
		switch (fieldType) {
			case DataFieldMetadata.NUMERIC_FIELD:
				return "numeric";
			case DataFieldMetadata.INTEGER_FIELD:
				return "integer";
			case DataFieldMetadata.STRING_FIELD:
				return "string";
			case DataFieldMetadata.DATE_FIELD:
				return "date";
			default:
				throw new RuntimeException("Unsupported data type " + fieldType);
		}
	}



	/**
	 *  Creates SQL insert statement based on metadata describing data flow and
	 *  supplied table name
	 *
	 *@param  metadata   Metadata describing data flow from which to feed database
	 *@param  tableName  Table name into which insert data
	 *@return            string containing SQL insert statement
	 *@since             October 2, 2002
	 */
	public static String assembleInsertSQLStatement(DataRecordMetadata metadata, String tableName) {
		StringBuffer strBuf = new StringBuffer();
		StringBuffer strBuf2 = new StringBuffer();
		
		strBuf.append(") values(");
		for (int i = 0; i < metadata.getNumFields(); i++) {
			strBuf2.append(metadata.getField(i).getName());
			strBuf.append("?");
			if (i < metadata.getNumFields() - 1) {
				strBuf.append(",");
				strBuf2.append(",");
			}
		}
		strBuf.insert(0,strBuf2.toString());
		strBuf.insert(0," (");
		strBuf.insert(0,tableName);
		strBuf.insert(0,"insert into ");
		strBuf.append(")");
		System.out.println(strBuf.toString());
		return strBuf.toString();
	}
}

