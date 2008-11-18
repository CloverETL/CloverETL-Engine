/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package cz.opentech.jdbc.xlsdriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;

import cz.opentech.jdbc.xlsdriver.db.XlsDB;
import cz.opentech.jdbc.xlsdriver.metadata.ConnectionInfo;

/**
 * <p>
 * Connection URL has the following format:<br/>
 *     <center><code>"jdbc:opentech:xls:" [ file_path ] ( property "=" value )*</code></center>
 * <br/>
 * where at least one of <code>file_path</code> or <code>file</code> property must be provided.
 * Even you can specify more then one file. The workbook is then mapped as a schema and
 * sheets as tables.
 * </p>
 * 
 * <p>
 * <b>Properties:</b><br/>
 * <table><tr><th>Name</th><th>Value</th></tr>
 * 
 * file=xlsfile
 * sign=signature
 * head=1,3-4
 * rblock=1-
 * cblock=3-4,5-9, 10-
 * 
 * 
 * </table>
 * 
 * @author vitek
 */
public class XlsDriver implements Driver {
    
    public static final String URL_PREFIX = "jdbc:opentech:xls:";
    public static final String FILE_PROPERTY = "file";
    public static final String CBLOCKS_PROPERTY = "cblocks";
    public static final String RBLOCKS_PROPERTY = "rblocks";
    public static final String HEAD_PROPERTY = "head";

    // auto registration
    static {
        try {
            DriverManager.registerDriver(new XlsDriver());
        } catch (SQLException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }
    
    /**
     * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
     */
    public Connection connect(String url, Properties props) throws SQLException {
        url = url.trim();
        if (!acceptsURL(url)) {
            throw new SQLException("Cannot understand url:" + url);
        }
        url = url.substring(URL_PREFIX.length());
        ConnectionInfo info = ConnectionInfo.parseInfo(extendUrl(url, props));
        XlsDB db = new XlsDB(info);
        return db.createConnection();
    }
    private static String extendUrl(String url, Properties props)
    		throws SQLException {
        StringBuffer sb = new StringBuffer(url.trim());
        for (Iterator it = props.keySet().iterator(); it.hasNext();) {
            String key = (String) it.next();
            String value = props.getProperty(key);
            sb.append(";").append(key).append("=").append(value);
        }
        return sb.toString();
    }

    /**
     * @see java.sql.Driver#acceptsURL(java.lang.String)
     */
    public boolean acceptsURL(String url) throws SQLException {
        url = url.trim().toLowerCase();
        return url.startsWith(URL_PREFIX);
    }

    /**
     * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
     */
    public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1)
            throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * @see java.sql.Driver#getMajorVersion()
     */
    public int getMajorVersion() {
        return Version.MAJOR_VERSION;
    }

    /**
     * @see java.sql.Driver#getMinorVersion()
     */
    public int getMinorVersion() {
        return Version.MINOR_VERSION;
    }

    /**
     * @see java.sql.Driver#jdbcCompliant()
     */
    public boolean jdbcCompliant() {
        return true;
    }
}
