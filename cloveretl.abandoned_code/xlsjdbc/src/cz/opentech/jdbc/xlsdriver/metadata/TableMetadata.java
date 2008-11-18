package cz.opentech.jdbc.xlsdriver.metadata;

import java.sql.SQLException;
import java.util.ArrayList;

import cz.opentech.jdbc.xlsdriver.db.XlsType;
import cz.opentech.jdbc.xlsdriver.db.util.Blocks;
import cz.opentech.jdbc.xlsdriver.db.util.EscapedStringTokenizer;

/**
 * @author vitek
 */
public class TableMetadata {
    
    private final SchemaMetadata schema;
    private String name;
    private String file;
    private String sheet;
    private final ArrayList columns = new ArrayList();
    private Blocks rows = new Blocks();
    private Blocks cols = new Blocks();
    private Blocks head = new Blocks();
	private int rowsScanned;

    public TableMetadata(SchemaMetadata schema) {
        this.schema = schema;
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
     * @return the name.
     */
    public String getName() {
        return name;
    }
    /**
     * @param name the name to set.
     */
    public void setName(String name) throws SQLException {
        parseSignature(name);
    }
    /**
     * @return the columns.
     */
    public ColumnMetadata[] getColumns() {
        return (ColumnMetadata[])columns.toArray(new ColumnMetadata[columns.size()]);
    }
    /**
     * 
     * @param idx
     * @return
     */
    public ColumnMetadata getColumn(int idx) {
        return (ColumnMetadata) columns.get(idx);
    }
    /**
     * 
     * @return
     */
    public int getColumnsCount() {
        return columns.size();
    }
    /**
     * 
     * @param column
     */
    public ColumnMetadata addColumn() {
        ColumnMetadata ret = new ColumnMetadata(this);
        columns.add(ret);
        return ret;
    }
    /**
     * 
     * @param idx
     */
    public void removeColumn(int idx) {
       columns.remove(idx); 
    }
    /**
     * 
     * @param column
     */
    public void removeColumn(ColumnMetadata column) {
        columns.remove(column);
    }
    /**
     * 
     *
     */
    public void removeAllColumns() {
        columns.clear();
    }
    /**
     * @return the schema.
     */
    public SchemaMetadata getSchema() {
        return schema;
    }
	
	public int getRowsScanned() {
		return rowsScanned;
	}
	public void setRowsScanned(int rowsScanned) {
		this.rowsScanned = rowsScanned;
	}

    /**
     * @return Returns the file.
     */
    public String getFile() {
        return file;
    }
    /**
     * @param file The file to set.
     */
    public void setFile(String file) {
        this.file = file;
    }
    /**
     * @return Returns the sheet.
     */
    public String getSheet() {
        return sheet;
    }
    /**
     * @param sheet The sheet to set.
     */
    public void setSheet(String sheet) {
        this.sheet = sheet;
    }

    public static TableMetadata parseTable(String str, TableMetadata schema) throws SQLException {
        while ((str = str.trim()).length() > 0) {
            String propStr = ConnectionInfo.extractProperty(str);
            
            String[] prop = ConnectionInfo.parseProperty(propStr, NAME_PROPERTY);
            String propName = prop[0].toLowerCase();
            String propValue = prop[1];
            schema.setProperty(propName, propValue);

            str = str.substring(propStr.length()).trim();
	        if (str.startsWith(";")) {
	            str = str.substring(1).trim();
	        }
        }
        
        return schema;
    }
    
    public static final String NAME_PROPERTY = "name";
    public static final String DATEFORMAT_PROPERTY = "dateformat";
    public static final String ROWS_PROPERTY = "rows";
    public static final String COLS_PROPERTY = "cols";
    public static final String HEAD_PROPERTY = "head";
    public static final String FILE_PROPERTY = "file";
    public static final String SHEET_PROPERTY = "sheet";
    public static final String ROWSSCANNED_PROPERTY = "rowsScanned";
	
    private void setProperty (String name, String value) throws SQLException {
        if (NAME_PROPERTY.equals(name)) {
            setName(value);
        } else if (ROWS_PROPERTY.equals(name)) {
            Blocks.fromString(value, getRows());
        } else if (COLS_PROPERTY.equals(name)) {
            Blocks.fromString(value, getCols());
        } else if (HEAD_PROPERTY.equals(name)) {
            Blocks.fromString(value, getHead());
        } else if (ROWSSCANNED_PROPERTY.equals(name)) try {
            rowsScanned = Integer.parseInt(value);
        } catch (NumberFormatException nfe) {
			rowsScanned = 0; // means scan over all rows
        } else if (FILE_PROPERTY.equals(name)) {
            setFile(value);
        } else if (SHEET_PROPERTY.equals(name)) {
            setSheet(value);
		} else {
            // donothing
        }
    }
    
    private void parseSignature(String sig) throws SQLException {
        int idx = sig.indexOf('(');
        if (idx != -1) {
            this.name = sig.substring(0, idx);
            sig = sig.substring(idx + 1).trim();
        } else {
            this.name = sig;
            return;
        }
        removeAllColumns();
        EscapedStringTokenizer tok = new EscapedStringTokenizer(sig, ",)", '\'');
        while (tok.hasMoreTokens()) {
            String colsig = tok.nextToken();
            idx = colsig.lastIndexOf(' ');
            String colType = colsig.substring(idx + 1).trim();
            String colName = colsig.substring(0, idx).trim();
            ColumnMetadata column = addColumn();
            column.setType(XlsType.getType(colType));
            column.setName(colName);
        }
    }
}
