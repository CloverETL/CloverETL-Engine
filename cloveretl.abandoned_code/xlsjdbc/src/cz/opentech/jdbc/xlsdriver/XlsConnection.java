/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package cz.opentech.jdbc.xlsdriver;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Map;

/**
 * JDBC connection.
 * 
 * @see XlsDriver#connect(String, Properties)
 * @see java.sql.Connection
 * 
 * @author vitaz
 */
public class XlsConnection implements Connection {

    private final Connection conn;
    
    /**
     * 
     * @param info
     */
    public XlsConnection(Connection conn) {
        this.conn = conn;
    }
    
    /**
     * @see java.sql.Connection#createStatement()
     */
    public Statement createStatement() throws SQLException {
        return conn.createStatement();
    }

    /**
     * @see java.sql.Connection#prepareStatement(java.lang.String)
     */
    public PreparedStatement prepareStatement(String sql)
    		throws SQLException {
        return conn.prepareStatement(sql);
    }

    /**
     * @see java.sql.Connection#prepareCall(java.lang.String)
     */
    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql);
    }

    /**
     * @see java.sql.Connection#nativeSQL(java.lang.String)
     */
    public String nativeSQL(String sql) throws SQLException {
        return conn.nativeSQL(sql);
    }

    /**
     * @see java.sql.Connection#setAutoCommit(boolean)
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
	        conn.setAutoCommit(autoCommit);
    }

    /**
     * @see java.sql.Connection#getAutoCommit()
     */
    public boolean getAutoCommit() throws SQLException {
        return conn.getAutoCommit();
    }

    /**
     * @see java.sql.Connection#commit()
     */
    public void commit() throws SQLException {
        conn.commit();
    }

    /**
     * @see java.sql.Connection#rollback()
     */
    public void rollback() throws SQLException {
        conn.rollback();
    }

    /**
     * @see java.sql.Connection#close()
     */
    public void close() throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("SHUTDOWN");
        } finally {
            stmt.close();
            conn.close();
        }
    }

    /**
     * @see java.sql.Connection#isClosed()
     */
    public boolean isClosed() throws SQLException {
        return conn.isClosed();
    }

    /**
     * @see java.sql.Connection#getMetaData()
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        return conn.getMetaData();
    }

    /**
     * @see java.sql.Connection#setReadOnly(boolean)
     */
    public void setReadOnly(boolean readOnly) throws SQLException {
        conn.setReadOnly(readOnly);
    }

    /**
     * @see java.sql.Connection#isReadOnly()
     */
    public boolean isReadOnly() throws SQLException {
        return conn.isReadOnly();
    }

    /**
     * @see java.sql.Connection#setCatalog(java.lang.String)
     */
    public void setCatalog(String cat) throws SQLException {
        conn.setCatalog(cat);
    }

    /**
     * @see java.sql.Connection#getCatalog()
     */
    public String getCatalog() throws SQLException {
        return conn.getCatalog();
    }

    /**
     * @see java.sql.Connection#setTransactionIsolation(int)
     */
    public void setTransactionIsolation(int level)
    		throws SQLException {
        conn.setTransactionIsolation(level);
    }

    /**
     * @see java.sql.Connection#getTransactionIsolation()
     */
    public int getTransactionIsolation() throws SQLException {
        return conn.getTransactionIsolation();
    }

    /**
     * @see java.sql.Connection#getWarnings()
     */
    public SQLWarning getWarnings() throws SQLException {
        return conn.getWarnings();
    }

    /**
     * @see java.sql.Connection#clearWarnings()
     */
    public void clearWarnings() throws SQLException {
        conn.clearWarnings();
    }

    /**
     * @see java.sql.Connection#createStatement(int, int)
     */
    public Statement createStatement(int rsType, int rsCon)
    		throws SQLException {
        return conn.createStatement(rsType, rsCon);
    }

    /**
     * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
     */
    public PreparedStatement prepareStatement(String sql, int rsType, int rsCon)
            throws SQLException {
        return conn.prepareStatement(sql, rsType, rsCon);
    }

    /**
     * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
     */
    public CallableStatement prepareCall(String sql, int rsType, int rsCon)
            throws SQLException {
        return conn.prepareCall(sql, rsType, rsCon);
    }

    /**
     * @see java.sql.Connection#getTypeMap()
     */
    public Map getTypeMap() throws SQLException {
        return conn.getTypeMap();
    }

    /**
     * @see java.sql.Connection#setTypeMap(java.util.Map)
     */
    public void setTypeMap(Map typeMap) throws SQLException {
        conn.setTypeMap(typeMap);
    }
    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @return
     * @throws java.sql.SQLException
     */
    public Statement createStatement(int arg0, int arg1, int arg2)
            throws SQLException {
        return conn.createStatement(arg0, arg1, arg2);
    }
    /**
     * @return
     * @throws java.sql.SQLException
     */
    public int getHoldability() throws SQLException {
        return conn.getHoldability();
    }
    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @param arg3
     * @return
     * @throws java.sql.SQLException
     */
    public CallableStatement prepareCall(String arg0, int arg1, int arg2,
            int arg3) throws SQLException {
        return conn.prepareCall(arg0, arg1, arg2, arg3);
    }
    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws java.sql.SQLException
     */
    public PreparedStatement prepareStatement(String arg0, int arg1)
            throws SQLException {
        return conn.prepareStatement(arg0, arg1);
    }
    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @param arg3
     * @return
     * @throws java.sql.SQLException
     */
    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2,
            int arg3) throws SQLException {
        return conn.prepareStatement(arg0, arg1, arg2, arg3);
    }
    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws java.sql.SQLException
     */
    public PreparedStatement prepareStatement(String arg0, int[] arg1)
            throws SQLException {
        return conn.prepareStatement(arg0, arg1);
    }
    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws java.sql.SQLException
     */
    public PreparedStatement prepareStatement(String arg0, String[] arg1)
            throws SQLException {
        return conn.prepareStatement(arg0, arg1);
    }
    /**
     * @param arg0
     * @throws java.sql.SQLException
     */
    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        conn.releaseSavepoint(arg0);
    }
    /**
     * @param arg0
     * @throws java.sql.SQLException
     */
    public void rollback(Savepoint arg0) throws SQLException {
        conn.rollback(arg0);
    }
    /**
     * @param arg0
     * @throws java.sql.SQLException
     */
    public void setHoldability(int arg0) throws SQLException {
        conn.setHoldability(arg0);
    }
    /**
     * @return
     * @throws java.sql.SQLException
     */
    public Savepoint setSavepoint() throws SQLException {
        return conn.setSavepoint();
    }
    /**
     * @param arg0
     * @return
     * @throws java.sql.SQLException
     */
    public Savepoint setSavepoint(String arg0) throws SQLException {
        return conn.setSavepoint(arg0);
    }
}
