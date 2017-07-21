/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package cz.opentech.jdbc.xlsdriver.db;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import jxl.Sheet;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import cz.opentech.jdbc.xlsdriver.XlsConnection;
import cz.opentech.jdbc.xlsdriver.db.util.Blocks;
import cz.opentech.jdbc.xlsdriver.metadata.ColumnMetadata;
import cz.opentech.jdbc.xlsdriver.metadata.ConnectionInfo;
import cz.opentech.jdbc.xlsdriver.metadata.SchemaMetadata;
import cz.opentech.jdbc.xlsdriver.metadata.TableMetadata;

/**
 * Handles
 * 
 * @author vitaz
 */
public class XlsDB {
    
    private static final HashMap workbookCache = new HashMap();
    private final File tmpDir;
    private final File dbFile;
    private final String HSQLDB_URL;
    private final ConnectionInfo info;

    static {
        try {
            //register HSQLDB driver
            Class.forName("org.hsqldb.jdbcDriver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("HSQDB database not linked");
        }
    }

    /**
     * 
     * @param info
     */
    public XlsDB(ConnectionInfo info) throws SQLException {
        this.info = info;
        tmpDir = createTempDir("xls", "db");
        tmpDir.deleteOnExit();
        dbFile = new File(tmpDir, "xlsdb.db");            
        HSQLDB_URL = "jdbc:hsqldb:file:" + dbFile;
        initDB();
    }

    /**
     * Loads workbooks and sheets and attaches them to hsqldb tables.
     * 
     * @throws SQLException an error occured.
     */
    private void initDB() throws SQLException {
        Connection hsqldbconn = DriverManager.getConnection(HSQLDB_URL);
        try {
            loadWorkbooks(hsqldbconn);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("I/O error:" + e.getMessage());
        } finally {
            hsqldbconn.close();
        }
    }
    /**
     * Loads workbooks.
     * 
     * @param conn
     * @throws SQLException
     * @throws IOException
     */
    private void loadWorkbooks(Connection conn) throws SQLException, IOException {
        int schCnt = info.getSchemasCount();
        for (int i = 0; i < schCnt; i++) {
            SchemaMetadata schema = info.getSchema(i);
            // xls file has to be referred by the absolute path
            schema.setFile(new File(schema.getFile()).getAbsolutePath());
            loadTables(conn, schema);
        }
    }
    /**
     * Loads sheets (aka tables) for a workbook.
     * @param conn
     * @param schema
     * @throws SQLException
     * @throws IOException
     */
    private void loadTables(Connection conn, SchemaMetadata schema)
    		throws SQLException, IOException {
        final XlsWorkbook wb = loadWorkbook(schema.getFile());
        final WritableWorkbook wwb = wb.getWWB();
        
        final int tblCnt = schema.getTablesCount();
        final int sheetsCnt = wwb.getNumberOfSheets();
        for (int i = 0, cnt = Math.max(tblCnt, sheetsCnt); i < cnt; i++) {
            final TableMetadata tablemeta = (i < tblCnt)
            		? schema.getTable(i) : schema.addTable();
            WritableSheet sheet;
            if (i < sheetsCnt) {
                sheet = wwb.getSheet(i);
            } else {
                sheet = wwb.createSheet(tablemeta.getName(), i);
            }
            loadTable(conn, sheet, tablemeta);
        }
    }
    /**
     * Loads concrete sheet/table. First the sheet and table metadata
     * (provided by URL) are analyzed and normalized to a correct
     * form, e.g. if not table definition is provided by URL the method
     * analyzes the sheet's content and tries to find header,
     * columns types etc. 
     * @param conn
     * @param sheet
     * @param meta
     * @throws SQLException
     */
    private void loadTable(Connection conn, WritableSheet sheet,
        TableMetadata meta) throws SQLException {
        updateTableMetadata(meta, sheet);
        if (meta.getName() == null || meta.getColumnsCount() == 0
                || meta.getCols().isEmpty()
                || meta.getRows().isEmpty()) {
            loadSignature(sheet, meta);
        }
        
        if (meta.getColumnsCount() == 0) {
            return; // skip empty sheet
        }
        attachSheetToHsqldb(conn, sheet, meta);
    }
    /**
     * Updaters table metadata with analyzed sheet's content.
     * 
     * @param sheet
     * @param meta
     * @throws SQLException
     */
    private void loadSignature(WritableSheet sheet, TableMetadata meta)
    		throws SQLException {
        if (meta.getHead().isEmpty() || meta.getCols().isEmpty()) {
            // try to find the header in the sheet
            XlsTableAnalyzer.analyzeHeader(sheet, meta);
        } else {
            // load header along the rows specified by head parameter
            XlsTableAnalyzer.loadHeader(sheet, meta);
        }
        // analyzes types of columns
        XlsTableAnalyzer.analyzeTypes(sheet, meta);
        // finds duplicate and empty column names
        XlsTableAnalyzer.normalizeHeader(meta);
        // set table name if not yet set
        if (meta.getName() == null) {
            meta.setName(sheet.getName());
        }
    }
    /**
     * Updates some table properties not yet set from the schema
     * @param table the table metadata.
     * @param sheet the schema metadata.
     */
    private void updateTableMetadata(TableMetadata table, Sheet sheet) {
        SchemaMetadata schema = table.getSchema();
        
        if (table.getHead().isEmpty() && !schema.getHead().isEmpty()) {
            table.getHead().union(schema.getHead());
        }
        if (table.getRows().isEmpty() && !schema.getRows().isEmpty()) {
            table.getRows().union(schema.getRows());
        } else {
        	table.getCols().remove(sheet.getRows(), Blocks.INF);
        }
        if (table.getCols().isEmpty() && !schema.getCols().isEmpty()) {
            table.getCols().union(schema.getCols());
        } else {
        	table.getCols().remove(sheet.getColumns(), Blocks.INF);
        }
        table.setSheet(sheet.getName());
        if (table.getFile() == null) {
            table.setFile(schema.getFile());
        }
    }
    /**
     * Attaches a sheet to a text table definition in the hsqldb.
     * So the sheet can be accessed through the hsqdb JDBC driver.
     * 
     * @param conn the hsqldb connection.
     * @param sheet the sheet.
     * @param meta the table metadata.
     * @throws SQLException an error occured.
     */
    private void attachSheetToHsqldb(Connection conn, WritableSheet sheet,
            TableMetadata meta) throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            final String tableName = meta.getName();
            stmt.execute("drop table \"" + tableName + "\" if exists");
            stmt.execute(createCreateTableStatement(meta));
            stmt.execute("set table \"" + tableName + "\" source \""
                    + createTableUrl(meta) + "\"");
        } finally {
            stmt.close();
        }
    }
    private static String createCreateTableStatement(TableMetadata meta) {
        StringBuffer sb = new StringBuffer();
        sb.append("create text table \"").append(meta.getName());
        sb.append("\" (");
        
        ColumnMetadata[] colsmeta = meta.getColumns();
        final int colsLen = colsmeta.length;
        for (int i = 0; i < colsLen; i++) {
            ColumnMetadata colmeta = colsmeta[i];
            if (i > 0) sb.append(", ");
            sb.append("\"").append(colmeta.getName()).append("\" ").append(colmeta.getType().hsqldbType);
        }
        sb.append(")");
        return sb.toString();
    }
    public String createTableUrl(TableMetadata meta) {
        StringBuffer sb = new StringBuffer();
        sb.append(meta.getFile());
        sb.append(";").append(meta.getName());
        if (meta.getSheet() != null) {
            sb.append(";" + TableMetadata.SHEET_PROPERTY + "=");
            sb.append(meta.getSheet());
        }
        if (!meta.getHead().isEmpty()) {
            sb.append(";" + TableMetadata.HEAD_PROPERTY + "=");
            sb.append(meta.getHead());
        }
        if (!meta.getRows().isEmpty()) {
            sb.append(";" + TableMetadata.ROWS_PROPERTY + "=");
            sb.append(meta.getRows());
        }
        if (!meta.getCols().isEmpty()) {
            sb.append(";" + TableMetadata.COLS_PROPERTY + "=");
            sb.append(meta.getCols());
        }
        sb.append(";" + TableMetadata.NAME_PROPERTY + "=");
        sb.append(meta.getName());
        sb.append("(");
        for (int i = 0; i < meta.getColumnsCount(); i++) {
            final ColumnMetadata col = meta.getColumn(i);
            if (i > 0) sb.append(", ");
            sb.append(col.getName()).append(" ").append(col.getType().toString());
        }
        sb.append(")");
        
        return sb.toString();
    }
    
    /**
     * Creates connection to db.
     * 
     * @return
     * @throws SQLException
     */
    public XlsConnection createConnection() throws SQLException {
        return new XlsConnection(createHsqldbConnection());
    }
    private Connection createHsqldbConnection() throws SQLException {
        return DriverManager.getConnection(HSQLDB_URL);
    }
    
    /**
     * 
     * @param file
     * @return
     * @throws IOException
     */
    public static XlsWorkbook loadWorkbook(String file) throws IOException {
        String key = file.toLowerCase();
        XlsWorkbook wb = (XlsWorkbook) workbookCache.get(key);
        if (wb == null) {
            wb = new XlsWorkbook();
            wb.load(file);
            workbookCache.put(key, wb);
        }
        return wb;
    }
    
    private File createTempDir(String prefix, String suffix) {
        String tmpDirParent = System.getProperty("java.io.tmpdir");
        String dirPath;
        
        File ret;
        int maxCycles = 100;
        do {
            Thread.yield();
            long rand = (System.currentTimeMillis() ^ (long)Math.random()) % 100000;
            ret = new File(tmpDirParent, prefix + rand + suffix);
        } while (!ret.mkdir() && maxCycles-- > 0);
    	
        return ret; 
    }
}
