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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Result set which allows to call get method multiple times. NOTE: get methods with single integer arguments shall be
 * called. The other get methods can throw exception when called multiple times.
 * 
 * @author Martin Slama (martin.slama@javlin.eu) (c) Javlin, a.s (http://www.javlin.eu)
 * 
 * @created January 25, 2012
 */
public class OdbcResultSet implements ResultSet {

	private static final Log logger = LogFactory.getLog(OdbcResultSet.class);

	private final ResultSet resultSet;
	private final Map<Integer, Object> cache = new HashMap<Integer, Object>();

	private static final Class<?>[] SINGLE_INTEGER = new Class[1];

	static {
		SINGLE_INTEGER[0] = Integer.TYPE;
	}

	/**
	 * Constructor.
	 * 
	 * @param resultSet
	 *            Calls of methods will be delegated to this result set.
	 */
	public OdbcResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	private Object getValueTryCache(int key, String method) {
		Object result = cache.get(key);
		if (result == null) {
			Object[] args = new Object[1];
			args[0] = key;
			try {
				result = resultSet.getClass().getMethod(method, SINGLE_INTEGER).invoke(resultSet, args);
			} catch (Exception e) {
				logger.error("Failed to invoke get method on result set: " + method, e);
			}
			cache.put(key, result);
		}
		return result;
	}

	/**
	 * @param row
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#absolute(int)
	 */
	@Override
	public boolean absolute(int row) throws SQLException {
		return resultSet.absolute(row);
	}

	/**
	 * @throws SQLException
	 * @see java.sql.ResultSet#afterLast()
	 */
	@Override
	public void afterLast() throws SQLException {
		resultSet.afterLast();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.ResultSet#beforeFirst()
	 */
	@Override
	public void beforeFirst() throws SQLException {
		resultSet.beforeFirst();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.ResultSet#cancelRowUpdates()
	 */
	@Override
	public void cancelRowUpdates() throws SQLException {
		resultSet.cancelRowUpdates();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.ResultSet#clearWarnings()
	 */
	@Override
	public void clearWarnings() throws SQLException {
		resultSet.clearWarnings();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.ResultSet#close()
	 */
	@Override
	public void close() throws SQLException {
		resultSet.close();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.ResultSet#deleteRow()
	 */
	@Override
	public void deleteRow() throws SQLException {
		resultSet.deleteRow();
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#findColumn(java.lang.String)
	 */
	@Override
	public int findColumn(String columnLabel) throws SQLException {
		return resultSet.findColumn(columnLabel);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#first()
	 */
	@Override
	public boolean first() throws SQLException {
		return resultSet.first();
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getArray(int)
	 */
	@Override
	public Array getArray(int columnIndex) throws SQLException {
		return (Array) getValueTryCache(columnIndex, "getArray");
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getArray(java.lang.String)
	 */
	@Override
	public Array getArray(String columnLabel) throws SQLException {
		return resultSet.getArray(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getAsciiStream(int)
	 */
	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return resultSet.getAsciiStream(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getAsciiStream(java.lang.String)
	 */
	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return resultSet.getAsciiStream(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @param scale
	 * @return
	 * @throws SQLException
	 * @deprecated
	 * @see java.sql.ResultSet#getBigDecimal(int, int)
	 */
	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		return resultSet.getBigDecimal(columnIndex, scale);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getBigDecimal(int)
	 */
	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return (BigDecimal) getValueTryCache(columnIndex, "getBigDecimal");
	}

	/**
	 * @param columnLabel
	 * @param scale
	 * @return
	 * @throws SQLException
	 * @deprecated
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String, int)
	 */
	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return resultSet.getBigDecimal(columnLabel, scale);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String)
	 */
	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return resultSet.getBigDecimal(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getBinaryStream(int)
	 */
	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return resultSet.getBinaryStream(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getBinaryStream(java.lang.String)
	 */
	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return resultSet.getBinaryStream(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getBlob(int)
	 */
	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		return (Blob) getValueTryCache(columnIndex, "getBlob");
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getBlob(java.lang.String)
	 */
	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		return resultSet.getBlob(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getBoolean(int)
	 */
	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return (Boolean) getValueTryCache(columnIndex, "getBoolean");
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getBoolean(java.lang.String)
	 */
	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return resultSet.getBoolean(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getByte(int)
	 */
	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return resultSet.getByte(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getByte(java.lang.String)
	 */
	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return resultSet.getByte(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getBytes(int)
	 */
	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		return (byte[]) getValueTryCache(columnIndex, "getBytes");
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getBytes(java.lang.String)
	 */
	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		return resultSet.getBytes(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getCharacterStream(int)
	 */
	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		return resultSet.getCharacterStream(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getCharacterStream(java.lang.String)
	 */
	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		return resultSet.getCharacterStream(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getClob(int)
	 */
	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		return resultSet.getClob(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getClob(java.lang.String)
	 */
	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		return resultSet.getClob(columnLabel);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getConcurrency()
	 */
	@Override
	public int getConcurrency() throws SQLException {
		return resultSet.getConcurrency();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getCursorName()
	 */
	@Override
	public String getCursorName() throws SQLException {
		return resultSet.getCursorName();
	}

	/**
	 * @param columnIndex
	 * @param cal
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getDate(int, java.util.Calendar)
	 */
	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		return resultSet.getDate(columnIndex, cal);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getDate(int)
	 */
	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return (Date) getValueTryCache(columnIndex, "getDate");
	}

	/**
	 * @param columnLabel
	 * @param cal
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getDate(java.lang.String, java.util.Calendar)
	 */
	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		return resultSet.getDate(columnLabel, cal);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getDate(java.lang.String)
	 */
	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return resultSet.getDate(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getDouble(int)
	 */
	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return (Double) getValueTryCache(columnIndex, "getDouble");
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getDouble(java.lang.String)
	 */
	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return resultSet.getDouble(columnLabel);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getFetchDirection()
	 */
	@Override
	public int getFetchDirection() throws SQLException {
		return resultSet.getFetchDirection();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getFetchSize()
	 */
	@Override
	public int getFetchSize() throws SQLException {
		return resultSet.getFetchSize();
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getFloat(int)
	 */
	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return resultSet.getFloat(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getFloat(java.lang.String)
	 */
	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return resultSet.getFloat(columnLabel);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getHoldability()
	 */
	@Override
	public int getHoldability() throws SQLException {
		return resultSet.getHoldability();
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getInt(int)
	 */
	@Override
	public int getInt(int columnIndex) throws SQLException {
		return (Integer) getValueTryCache(columnIndex, "getInt");
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getInt(java.lang.String)
	 */
	@Override
	public int getInt(String columnLabel) throws SQLException {
		return resultSet.getInt(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getLong(int)
	 */
	@Override
	public long getLong(int columnIndex) throws SQLException {
		return (Long) getValueTryCache(columnIndex, "getLong");
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getLong(java.lang.String)
	 */
	@Override
	public long getLong(String columnLabel) throws SQLException {
		return resultSet.getLong(columnLabel);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getMetaData()
	 */
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return resultSet.getMetaData();
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getNCharacterStream(int)
	 */
	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		return resultSet.getNCharacterStream(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getNCharacterStream(java.lang.String)
	 */
	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		return resultSet.getNCharacterStream(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getNClob(int)
	 */
	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		return resultSet.getNClob(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getNClob(java.lang.String)
	 */
	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		return resultSet.getNClob(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getNString(int)
	 */
	@Override
	public String getNString(int columnIndex) throws SQLException {
		return resultSet.getNString(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getNString(java.lang.String)
	 */
	@Override
	public String getNString(String columnLabel) throws SQLException {
		return resultSet.getNString(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @param map
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getObject(int, java.util.Map)
	 */
	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		return resultSet.getObject(columnIndex, map);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getObject(int)
	 */
	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return getValueTryCache(columnIndex, "getObject");
	}

	/**
	 * @param columnLabel
	 * @param map
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getObject(java.lang.String, java.util.Map)
	 */
	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		return resultSet.getObject(columnLabel, map);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getObject(java.lang.String)
	 */
	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return resultSet.getObject(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getRef(int)
	 */
	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		return resultSet.getRef(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getRef(java.lang.String)
	 */
	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		return resultSet.getRef(columnLabel);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getRow()
	 */
	@Override
	public int getRow() throws SQLException {
		return resultSet.getRow();
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getRowId(int)
	 */
	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		return resultSet.getRowId(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getRowId(java.lang.String)
	 */
	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		return resultSet.getRowId(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getSQLXML(int)
	 */
	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		return resultSet.getSQLXML(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getSQLXML(java.lang.String)
	 */
	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		return resultSet.getSQLXML(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getShort(int)
	 */
	@Override
	public short getShort(int columnIndex) throws SQLException {
		return resultSet.getShort(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getShort(java.lang.String)
	 */
	@Override
	public short getShort(String columnLabel) throws SQLException {
		return resultSet.getShort(columnLabel);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getStatement()
	 */
	@Override
	public Statement getStatement() throws SQLException {
		return resultSet.getStatement();
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getString(int)
	 */
	@Override
	public String getString(int columnIndex) throws SQLException {
		return (String) getValueTryCache(columnIndex, "getString");
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getString(java.lang.String)
	 */
	@Override
	public String getString(String columnLabel) throws SQLException {
		return resultSet.getString(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @param cal
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getTime(int, java.util.Calendar)
	 */
	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		return resultSet.getTime(columnIndex, cal);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getTime(int)
	 */
	@Override
	public Time getTime(int columnIndex) throws SQLException {
		return (Time) getValueTryCache(columnIndex, "getTime");
	}

	/**
	 * @param columnLabel
	 * @param cal
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getTime(java.lang.String, java.util.Calendar)
	 */
	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		return resultSet.getTime(columnLabel, cal);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getTime(java.lang.String)
	 */
	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return resultSet.getTime(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @param cal
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getTimestamp(int, java.util.Calendar)
	 */
	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		return resultSet.getTimestamp(columnIndex, cal);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getTimestamp(int)
	 */
	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return (Timestamp) getValueTryCache(columnIndex, "getTimestamp");
	}

	/**
	 * @param columnLabel
	 * @param cal
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String, java.util.Calendar)
	 */
	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		return resultSet.getTimestamp(columnLabel, cal);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String)
	 */
	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return resultSet.getTimestamp(columnLabel);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getType()
	 */
	@Override
	public int getType() throws SQLException {
		return resultSet.getType();
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getURL(int)
	 */
	@Override
	public URL getURL(int columnIndex) throws SQLException {
		return resultSet.getURL(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getURL(java.lang.String)
	 */
	@Override
	public URL getURL(String columnLabel) throws SQLException {
		return resultSet.getURL(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 * @deprecated
	 * @see java.sql.ResultSet#getUnicodeStream(int)
	 */
	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return resultSet.getUnicodeStream(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @return
	 * @throws SQLException
	 * @deprecated
	 * @see java.sql.ResultSet#getUnicodeStream(java.lang.String)
	 */
	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return resultSet.getUnicodeStream(columnLabel);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#getWarnings()
	 */
	@Override
	public SQLWarning getWarnings() throws SQLException {
		return resultSet.getWarnings();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.ResultSet#insertRow()
	 */
	@Override
	public void insertRow() throws SQLException {
		resultSet.insertRow();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#isAfterLast()
	 */
	@Override
	public boolean isAfterLast() throws SQLException {
		return resultSet.isAfterLast();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#isBeforeFirst()
	 */
	@Override
	public boolean isBeforeFirst() throws SQLException {
		return resultSet.isBeforeFirst();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#isClosed()
	 */
	@Override
	public boolean isClosed() throws SQLException {
		return resultSet.isClosed();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#isFirst()
	 */
	@Override
	public boolean isFirst() throws SQLException {
		return resultSet.isFirst();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#isLast()
	 */
	@Override
	public boolean isLast() throws SQLException {
		return resultSet.isLast();
	}

	/**
	 * @param iface
	 * @return
	 * @throws SQLException
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return resultSet.isWrapperFor(iface);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#last()
	 */
	@Override
	public boolean last() throws SQLException {
		return resultSet.last();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.ResultSet#moveToCurrentRow()
	 */
	@Override
	public void moveToCurrentRow() throws SQLException {
		resultSet.moveToCurrentRow();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.ResultSet#moveToInsertRow()
	 */
	@Override
	public void moveToInsertRow() throws SQLException {
		resultSet.moveToInsertRow();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#next()
	 */
	@Override
	public boolean next() throws SQLException {
		cache.clear();
		return resultSet.next();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#previous()
	 */
	@Override
	public boolean previous() throws SQLException {
		return resultSet.previous();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.ResultSet#refreshRow()
	 */
	@Override
	public void refreshRow() throws SQLException {
		resultSet.refreshRow();
	}

	/**
	 * @param rows
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#relative(int)
	 */
	@Override
	public boolean relative(int rows) throws SQLException {
		return resultSet.relative(rows);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#rowDeleted()
	 */
	@Override
	public boolean rowDeleted() throws SQLException {
		return resultSet.rowDeleted();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#rowInserted()
	 */
	@Override
	public boolean rowInserted() throws SQLException {
		return resultSet.rowInserted();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#rowUpdated()
	 */
	@Override
	public boolean rowUpdated() throws SQLException {
		return resultSet.rowUpdated();
	}

	/**
	 * @param direction
	 * @throws SQLException
	 * @see java.sql.ResultSet#setFetchDirection(int)
	 */
	@Override
	public void setFetchDirection(int direction) throws SQLException {
		resultSet.setFetchDirection(direction);
	}

	/**
	 * @param rows
	 * @throws SQLException
	 * @see java.sql.ResultSet#setFetchSize(int)
	 */
	@Override
	public void setFetchSize(int rows) throws SQLException {
		resultSet.setFetchSize(rows);
	}

	/**
	 * @param <T>
	 * @param iface
	 * @return
	 * @throws SQLException
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return resultSet.unwrap(iface);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateArray(int, java.sql.Array)
	 */
	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		resultSet.updateArray(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateArray(java.lang.String, java.sql.Array)
	 */
	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		resultSet.updateArray(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, int)
	 */
	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		resultSet.updateAsciiStream(columnIndex, x, length);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, long)
	 */
	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		resultSet.updateAsciiStream(columnIndex, x, length);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream)
	 */
	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		resultSet.updateAsciiStream(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, int)
	 */
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		resultSet.updateAsciiStream(columnLabel, x, length);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, long)
	 */
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		resultSet.updateAsciiStream(columnLabel, x, length);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream)
	 */
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		resultSet.updateAsciiStream(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBigDecimal(int, java.math.BigDecimal)
	 */
	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		resultSet.updateBigDecimal(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBigDecimal(java.lang.String, java.math.BigDecimal)
	 */
	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		resultSet.updateBigDecimal(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, int)
	 */
	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		resultSet.updateBinaryStream(columnIndex, x, length);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, long)
	 */
	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		resultSet.updateBinaryStream(columnIndex, x, length);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream)
	 */
	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		resultSet.updateBinaryStream(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, int)
	 */
	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		resultSet.updateBinaryStream(columnLabel, x, length);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, long)
	 */
	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		resultSet.updateBinaryStream(columnLabel, x, length);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream)
	 */
	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		resultSet.updateBinaryStream(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBlob(int, java.sql.Blob)
	 */
	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		resultSet.updateBlob(columnIndex, x);
	}

	/**
	 * @param columnIndex
	 * @param inputStream
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream, long)
	 */
	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		resultSet.updateBlob(columnIndex, inputStream, length);
	}

	/**
	 * @param columnIndex
	 * @param inputStream
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream)
	 */
	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		resultSet.updateBlob(columnIndex, inputStream);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBlob(java.lang.String, java.sql.Blob)
	 */
	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		resultSet.updateBlob(columnLabel, x);
	}

	/**
	 * @param columnLabel
	 * @param inputStream
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBlob(java.lang.String, java.io.InputStream, long)
	 */
	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		resultSet.updateBlob(columnLabel, inputStream, length);
	}

	/**
	 * @param columnLabel
	 * @param inputStream
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBlob(java.lang.String, java.io.InputStream)
	 */
	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		resultSet.updateBlob(columnLabel, inputStream);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBoolean(int, boolean)
	 */
	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		resultSet.updateBoolean(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBoolean(java.lang.String, boolean)
	 */
	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		resultSet.updateBoolean(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateByte(int, byte)
	 */
	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		resultSet.updateByte(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateByte(java.lang.String, byte)
	 */
	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		resultSet.updateByte(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBytes(int, byte[])
	 */
	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		resultSet.updateBytes(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateBytes(java.lang.String, byte[])
	 */
	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		resultSet.updateBytes(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, int)
	 */
	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		resultSet.updateCharacterStream(columnIndex, x, length);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, long)
	 */
	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		resultSet.updateCharacterStream(columnIndex, x, length);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader)
	 */
	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		resultSet.updateCharacterStream(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param reader
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, int)
	 */
	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		resultSet.updateCharacterStream(columnLabel, reader, length);
	}

	/**
	 * @param columnLabel
	 * @param reader
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, long)
	 */
	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		resultSet.updateCharacterStream(columnLabel, reader, length);
	}

	/**
	 * @param columnLabel
	 * @param reader
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader)
	 */
	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		resultSet.updateCharacterStream(columnLabel, reader);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateClob(int, java.sql.Clob)
	 */
	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		resultSet.updateClob(columnIndex, x);
	}

	/**
	 * @param columnIndex
	 * @param reader
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateClob(int, java.io.Reader, long)
	 */
	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		resultSet.updateClob(columnIndex, reader, length);
	}

	/**
	 * @param columnIndex
	 * @param reader
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateClob(int, java.io.Reader)
	 */
	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		resultSet.updateClob(columnIndex, reader);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateClob(java.lang.String, java.sql.Clob)
	 */
	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		resultSet.updateClob(columnLabel, x);
	}

	/**
	 * @param columnLabel
	 * @param reader
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateClob(java.lang.String, java.io.Reader, long)
	 */
	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		resultSet.updateClob(columnLabel, reader, length);
	}

	/**
	 * @param columnLabel
	 * @param reader
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateClob(java.lang.String, java.io.Reader)
	 */
	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		resultSet.updateClob(columnLabel, reader);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateDate(int, java.sql.Date)
	 */
	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		resultSet.updateDate(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateDate(java.lang.String, java.sql.Date)
	 */
	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		resultSet.updateDate(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateDouble(int, double)
	 */
	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		resultSet.updateDouble(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateDouble(java.lang.String, double)
	 */
	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		resultSet.updateDouble(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateFloat(int, float)
	 */
	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		resultSet.updateFloat(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateFloat(java.lang.String, float)
	 */
	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		resultSet.updateFloat(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateInt(int, int)
	 */
	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		resultSet.updateInt(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateInt(java.lang.String, int)
	 */
	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		resultSet.updateInt(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateLong(int, long)
	 */
	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		resultSet.updateLong(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateLong(java.lang.String, long)
	 */
	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		resultSet.updateLong(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader, long)
	 */
	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		resultSet.updateNCharacterStream(columnIndex, x, length);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader)
	 */
	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		resultSet.updateNCharacterStream(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param reader
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNCharacterStream(java.lang.String, java.io.Reader, long)
	 */
	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		resultSet.updateNCharacterStream(columnLabel, reader, length);
	}

	/**
	 * @param columnLabel
	 * @param reader
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNCharacterStream(java.lang.String, java.io.Reader)
	 */
	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		resultSet.updateNCharacterStream(columnLabel, reader);
	}

	/**
	 * @param columnIndex
	 * @param nClob
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNClob(int, java.sql.NClob)
	 */
	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		resultSet.updateNClob(columnIndex, nClob);
	}

	/**
	 * @param columnIndex
	 * @param reader
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNClob(int, java.io.Reader, long)
	 */
	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		resultSet.updateNClob(columnIndex, reader, length);
	}

	/**
	 * @param columnIndex
	 * @param reader
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNClob(int, java.io.Reader)
	 */
	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		resultSet.updateNClob(columnIndex, reader);
	}

	/**
	 * @param columnLabel
	 * @param nClob
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNClob(java.lang.String, java.sql.NClob)
	 */
	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		resultSet.updateNClob(columnLabel, nClob);
	}

	/**
	 * @param columnLabel
	 * @param reader
	 * @param length
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNClob(java.lang.String, java.io.Reader, long)
	 */
	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		resultSet.updateNClob(columnLabel, reader, length);
	}

	/**
	 * @param columnLabel
	 * @param reader
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNClob(java.lang.String, java.io.Reader)
	 */
	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		resultSet.updateNClob(columnLabel, reader);
	}

	/**
	 * @param columnIndex
	 * @param nString
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNString(int, java.lang.String)
	 */
	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		resultSet.updateNString(columnIndex, nString);
	}

	/**
	 * @param columnLabel
	 * @param nString
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNString(java.lang.String, java.lang.String)
	 */
	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException {
		resultSet.updateNString(columnLabel, nString);
	}

	/**
	 * @param columnIndex
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNull(int)
	 */
	@Override
	public void updateNull(int columnIndex) throws SQLException {
		resultSet.updateNull(columnIndex);
	}

	/**
	 * @param columnLabel
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateNull(java.lang.String)
	 */
	@Override
	public void updateNull(String columnLabel) throws SQLException {
		resultSet.updateNull(columnLabel);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @param scaleOrLength
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object, int)
	 */
	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		resultSet.updateObject(columnIndex, x, scaleOrLength);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object)
	 */
	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		resultSet.updateObject(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @param scaleOrLength
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object, int)
	 */
	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		resultSet.updateObject(columnLabel, x, scaleOrLength);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object)
	 */
	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		resultSet.updateObject(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateRef(int, java.sql.Ref)
	 */
	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		resultSet.updateRef(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateRef(java.lang.String, java.sql.Ref)
	 */
	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		resultSet.updateRef(columnLabel, x);
	}

	/**
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateRow()
	 */
	@Override
	public void updateRow() throws SQLException {
		resultSet.updateRow();
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateRowId(int, java.sql.RowId)
	 */
	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		resultSet.updateRowId(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateRowId(java.lang.String, java.sql.RowId)
	 */
	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		resultSet.updateRowId(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param xmlObject
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateSQLXML(int, java.sql.SQLXML)
	 */
	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		resultSet.updateSQLXML(columnIndex, xmlObject);
	}

	/**
	 * @param columnLabel
	 * @param xmlObject
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateSQLXML(java.lang.String, java.sql.SQLXML)
	 */
	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		resultSet.updateSQLXML(columnLabel, xmlObject);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateShort(int, short)
	 */
	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		resultSet.updateShort(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateShort(java.lang.String, short)
	 */
	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		resultSet.updateShort(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateString(int, java.lang.String)
	 */
	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		resultSet.updateString(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateString(java.lang.String, java.lang.String)
	 */
	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		resultSet.updateString(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateTime(int, java.sql.Time)
	 */
	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		resultSet.updateTime(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateTime(java.lang.String, java.sql.Time)
	 */
	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		resultSet.updateTime(columnLabel, x);
	}

	/**
	 * @param columnIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateTimestamp(int, java.sql.Timestamp)
	 */
	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		resultSet.updateTimestamp(columnIndex, x);
	}

	/**
	 * @param columnLabel
	 * @param x
	 * @throws SQLException
	 * @see java.sql.ResultSet#updateTimestamp(java.lang.String, java.sql.Timestamp)
	 */
	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		resultSet.updateTimestamp(columnLabel, x);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.ResultSet#wasNull()
	 */
	@Override
	public boolean wasNull() throws SQLException {
		return resultSet.wasNull();
	}
	
	/* JDBC 4.1 */

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		return resultSet.getObject(columnIndex, type);
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return resultSet.getObject(columnLabel, type);
	}
}
