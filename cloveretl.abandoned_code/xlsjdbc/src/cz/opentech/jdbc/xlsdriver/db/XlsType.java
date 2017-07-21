/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package cz.opentech.jdbc.xlsdriver.db;

import java.sql.SQLException;
import java.sql.Types;

/**
 * @author vitek
 */
public final class XlsType {

    /**
     * 
     */
    public static final XlsType NUMERIC = new XlsType("numeric", Types.NUMERIC, "NUMERIC");
    
    /**
     * 
     */
    public static final XlsType TEXT = new XlsType("text", Types.LONGVARCHAR, "LONGVARCHAR");
    
    /**
     * 
     */
    public static final XlsType DATE = new XlsType("date", Types.DATE, "DATE");
    
    /**
     * 
     */
    public static final XlsType BOOL = new XlsType("bool", Types.BIT, "BIT");
    
    /**
     * 
     */
    public static final XlsType ERROR = new XlsType("error", Types.VARCHAR, "VARCHAR");
    
    /**
     * XLSDriver name.
     */
    public final String name;
    
    /**
     * Mapping to JDBC code (see {@link java.sql.Types}).
     */
    public final int jdbcCode;
    
    /**
     * Mapping to HSQLDB types.
     */
    public final String hsqldbType;
    
    /**
     * 
     * @param name
     * @param jdbcCode
     * @param hsqldbType
     */
    public XlsType(String name, int jdbcCode, String hsqldbType) {
        this.name = name;
        this.jdbcCode = jdbcCode;
        this.hsqldbType = hsqldbType;
    }
    
    /**
     * Returns the name of the type.
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return name;
    }
    
    public static XlsType getType(String name) throws SQLException {
        name = name.toLowerCase();
        if (NUMERIC.name.equals(name)) {
            return NUMERIC;
        } else if (TEXT.name.equals(name)) {
            return TEXT;
        } else if (DATE.name.equals(name)) {
            return DATE;
        } else if (BOOL.name.equals(name)) {
            return BOOL;
        } else throw new SQLException("Unknown type:" + name);
    }
}
