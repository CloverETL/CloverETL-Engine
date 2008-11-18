/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package cz.opentech.jdbc.xlsdriver.metadata;

import cz.opentech.jdbc.xlsdriver.db.XlsType;

/**
 * @author vitek
 */
public class ColumnMetadata {

    private final TableMetadata table;
    private String name;
    private XlsType type;
    private String format;
    
    /**
     * 
     * @param table
     */
    public ColumnMetadata(TableMetadata table) {
        this.table = table;
    }
    
    /**
     * @return the name.
     */
    public String getName() {
        return name;
    }
    /**
     * @return the type.
     */
    public XlsType getType() {
        return type;
    }
    /**
     * @param name the name to set.
     */
    public void setName(String name) {
        this.name = name;
    }
    /**
     * @param type the type to set.
     */
    public void setType(XlsType type) {
        this.type = type;
    }
    /**
     * @return the format.
     */
    public String getFormat() {
        if (format == null && table != null && type == XlsType.DATE) {
            return table.getSchema().getDateFormat();
        }
        return format;
    }
    /**
     * @param format the format to set.
     */
    public void setFormat(String format) {
        this.format = format;
    }
    /**
     * @return the table.
     */
    public TableMetadata getTable() {
        return table;
    }
}
