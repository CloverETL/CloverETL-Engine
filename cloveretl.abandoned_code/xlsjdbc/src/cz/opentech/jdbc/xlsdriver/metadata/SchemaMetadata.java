/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package cz.opentech.jdbc.xlsdriver.metadata;

import java.sql.SQLException;
import java.util.ArrayList;

import cz.opentech.jdbc.xlsdriver.db.util.Blocks;

/**
 * @author vitek
 */
public class SchemaMetadata {
    
    private final ConnectionInfo info;
    private String name;
    private String file;
    private String dateFormat;
    private final ArrayList tables = new ArrayList();
    private Blocks rows = new Blocks();
    private Blocks cols = new Blocks();
    private Blocks head = new Blocks();

    /**
     *
     */
    SchemaMetadata(ConnectionInfo info) {
        this.info = info;
    }
        
    /**
     * @return the name.
     */
    public String getName() {
        return name;
    }
    
    /**
     * @param name the name.
     */
    public void setName(String name) {
        this.name = name;
    }
    
    /**
     * @return the path.
     */
    public String getFile() {
        return file;
    }
    
    /**
     * @param file the path to set.
     */
    public void setFile(String file) {
        this.file = file;
    }
    
    /**
     * @return the cols.
     */
    public Blocks getCols() {
        return cols;
    }
    /**
     * @return the head.
     */
    public Blocks getHead() {
        return head;
    }
    /**
     * @return the rows.
     */
    public Blocks getRows() {
        return rows;
    }

    /**
     * @return the tables.
     */
    public TableMetadata[] getTables() {
        return (TableMetadata[]) tables.toArray(new TableMetadata[tables.size()]);
    }
    
    /**
     * 
     * @return
     */
    public int getTablesCount() {
        return tables.size();
    }
    
    /**
     * 
     * @param idx
     * @return
     */
    public TableMetadata getTable(int idx) {
        return (TableMetadata) tables.get(idx);
    }
    
    /**
     * 
     * @param table
     */
    public TableMetadata addTable() {
        TableMetadata ret = new TableMetadata(this);
        tables.add(ret);
        return ret;
    }
    
    /**
     * 
     * @param table
     */
    public void removeTable(TableMetadata table) {
        tables.remove(table);
    }
    
    /**
     * 
     * @param idx
     */
    public void removeTable(int idx) {
        tables.remove(idx);
    }
    
    /**
     *
     */
    public void removeAllTables() {
        tables.clear();
    }
    
    /**
     * @return the dateFormat.
     */
    public String getDateFormat() {
        return dateFormat;
    }
    /**
     * @param dateFormat the dateFormat to set.
     */
    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }
    
    public static SchemaMetadata parseSchema(String str, SchemaMetadata schema) throws SQLException {
        while ((str = str.trim()).length() > 0) {
	        if (str.startsWith("{")) {
	            str = str.substring(1);
	            String tabStr = ConnectionInfo.extractGroup(str);
	            
	            TableMetadata table = schema.addTable();
	            TableMetadata.parseTable(tabStr, table);
	            
	            str = str.substring(tabStr.length() + 1);
	        } else {
	            String propStr = ConnectionInfo.extractProperty(str);
	            
	            String[] prop = ConnectionInfo.parseProperty(propStr, FILE_PROPERTY);
	            String propName = prop[0].toLowerCase();
	            String propValue = prop[1];
	            schema.setProperty(propName, propValue);
	            
	            str = str.substring(propStr.length());
	        }
	        str = str.trim();
	        if (str.startsWith(";")) {
	            str = str.substring(1).trim();
	        }
        }
        
        return schema;
    }
    
    private static final String NAME_PROPERTY = "name";
    private static final String FILE_PROPERTY = "file";
    private static final String DATEFORMAT_PROPERTY = "dateformat";
    private static final String ROWS_PROPERTY = "rows";
    private static final String COLS_PROPERTY = "cols";
    private static final String HEAD_PROPERTY = "head";
    private void setProperty (String name, String value) {
        if (NAME_PROPERTY.equals(name)) {
            setName(value);
        } else if (FILE_PROPERTY.equals(name)) {
            setFile(value);
        } else if (DATEFORMAT_PROPERTY.equals(name)) {
            setDateFormat(value);
        } else if (ROWS_PROPERTY.equals(name)) {
            Blocks.fromString(value, getRows());
        } else if (COLS_PROPERTY.equals(name)) {
            Blocks.fromString(value, getCols());
        } else if (HEAD_PROPERTY.equals(name)) {
            Blocks.fromString(value, getHead());
        } else {
            // donothing
        }
    }
}
