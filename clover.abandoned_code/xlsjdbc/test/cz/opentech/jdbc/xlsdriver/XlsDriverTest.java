/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package cz.opentech.jdbc.xlsdriver;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/**
 * @author vitek
 */
public class XlsDriverTest {

    public static void main(String[] args) throws Exception {
        try {
//        test1();
//        test2();
//        test3();
            //testBig();
            //testRollback();
          testJoinTables();
//        for (int i = 0; i < 1000; i++) {
//            System.out.println("Listing no. " + i + "...");
//            run();
//        }
        } catch (SQLException e) {
            e.printStackTrace();
            SQLException ie;
            while ((ie = e.getNextException()) != null) { 
	            System.err.println(">>> SQL cause >>>");
	            ie.printStackTrace();
	            System.err.println("<<< SQL cause <<<");
            }
        }
    }
    
    public static void testJoinTables() throws Exception {
        Class.forName("cz.opentech.jdbc.xlsdriver.XlsDriver");
        File testFile = File.createTempFile("testJoin", ".xls");
        //testFile.deleteOnExit();
        final String URL = "jdbc:opentech:xls:"
            + testFile + ";{testA(ID numeric, TXT text)};{testB(ID numeric, TXT text)}";
        Connection conn = DriverManager.getConnection(URL);
        final String TABLEA_NAME = "\"testA\"";
        final String TABLEB_NAME = "\"testB\"";
        final String[][] TEXT = {{"A", "1"}, {"B", "2"}, {"C", "3"}, {"D", "4"}, {"E", null}, {"F", null}};
        try {
            System.out.println("Opening sheet....");
            browseTable(conn, TABLEA_NAME);
            browseTable(conn, TABLEB_NAME);
            
            System.out.println("Inserting rows to " + TABLEA_NAME + " and " + TABLEB_NAME + "...");
            
            PreparedStatement pstmtA = conn.prepareStatement("INSERT INTO "
                    + TABLEA_NAME + " VALUES (?,?)");
            PreparedStatement pstmtB = conn.prepareStatement("INSERT INTO "
                    + TABLEB_NAME + " VALUES (?,?)");
            try {
                int affected = 0;
                for (int i = 0; i < TEXT.length; i++) {
                    if (TEXT[i][0] != null) {
	                    pstmtA.setInt(1, i);
	                    pstmtA.setString(2, TEXT[i][0]);
	                    affected += pstmtA.executeUpdate();
                    }
                    if (TEXT[i][1] != null) {
	                    pstmtB.setInt(1, i);
	                    pstmtB.setString(2, TEXT[i][1]);
	                    affected += pstmtB.executeUpdate();
                    }
                }
                conn.commit();
                System.out.println(affected + " rows commited.");
            } finally {
                pstmtA.close();
                pstmtB.close();
            }
            
            browseTable(conn, TABLEA_NAME);
            browseTable(conn, TABLEB_NAME);

            System.out.println("Joining tables...");

            browseTable(conn, TABLEA_NAME + " join " + TABLEB_NAME + " ON "
                    + TABLEA_NAME + ".ID=" + TABLEB_NAME + ".ID");
            browseTable(conn, TABLEA_NAME + " left outer join " + TABLEB_NAME + " ON "
                    + TABLEA_NAME + ".ID=" + TABLEB_NAME + ".ID");
            browseTable(conn, TABLEA_NAME + ", " + TABLEB_NAME);
                
        } finally {
            conn.close();
        }
    }
    
    public static void testBig() throws Exception {
        Class.forName("cz.opentech.jdbc.xlsdriver.XlsDriver");
        final String URL = "jdbc:opentech:xls:testBig.xls;{name=test(ID numeric, TXT text)}";
        Connection conn = DriverManager.getConnection(URL);
        final String TABLE_NAME = "\"test\""; 
        final int ROWS_INSERTED = 257;
//        final int ROWS_INSERTED = 30730;
        try {
            System.out.println("Opening sheet....");
            browseTable(conn, TABLE_NAME);
            
            System.out.println("Deleting all content...");

            Statement stmt = conn.createStatement();
            try {
                int affected = stmt.executeUpdate("DELETE FROM " + TABLE_NAME);
                
                System.out.println(affected + " lines deleted.");
            } finally {
                stmt.close();
            }
            
            System.out.println("Inserting " + ROWS_INSERTED + " rows...");
            
            int count;
            PreparedStatement pstmt = conn.prepareStatement("INSERT INTO "
                    + TABLE_NAME + " VALUES (?,?)");
            try {
                int affected = 0;
                for (int i = 0; i < ROWS_INSERTED; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, "Row no. " + i);
                    pstmt.executeUpdate();
                    affected++;
                }
                System.out.println(affected + " rows affected.");
            } finally {
                pstmt.close();
            }
            
            browseTable(conn, TABLE_NAME);

            stmt = conn.createStatement();
            try {
                stmt.execute("update " + TABLE_NAME + " set txt='Hello world!'");
            } finally {
                stmt.close();
            }

            browseTable(conn, TABLE_NAME);
            
        } finally {
            conn.close();
        }
    }
    
    public static void testRollback() throws Exception {
        Class.forName("cz.opentech.jdbc.xlsdriver.XlsDriver");
        File testFile = File.createTempFile("test", ".xls");
        //testFile.deleteOnExit();
        final String URL = "jdbc:opentech:xls:"
            + testFile + ";{test(ID numeric, TXT text)}";
        Connection conn = DriverManager.getConnection(URL);
        conn.setAutoCommit(false);
        final String TABLE_NAME = "\"test\""; 
        final int ROWS_INSERTED = 10;
        try {
            System.out.println("Opening sheet....");
            browseTable(conn, TABLE_NAME);
            
            System.out.println("Inserting " + ROWS_INSERTED + " rows...");
            
            int count;
            PreparedStatement pstmt = conn.prepareStatement("INSERT INTO "
                    + TABLE_NAME + " VALUES (?,?)");
            try {
                int affected = 0;
                for (int i = 0; i < ROWS_INSERTED; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, "Row no. " + i);
                    pstmt.executeUpdate();
                    affected++;
                }
                conn.commit();
                System.out.println(affected + " rows commited.");
            } finally {
                pstmt.close();
            }
            
            browseTable(conn, TABLE_NAME);

            System.out.println("Deleting even rows...");

            Statement stmt = conn.createStatement();
            try {
                int affected = stmt.executeUpdate("DELETE FROM " + TABLE_NAME
                        + " WHERE MOD(id,2)=0");
                
                System.out.println(affected + " lines deleted.");
            } finally {
                stmt.close();
            }
            System.out.println("Table before rollback...");
            
            browseTable(conn, TABLE_NAME);
            
            conn.rollback();
            System.out.println("Table after rollback.");
            browseTable(conn, TABLE_NAME);
            
//            stmt = conn.createStatement();
//            try {
//                stmt.execute("update " + TABLE_NAME + " set txt='Hello world!'");
//            } finally {
//                stmt.close();
//            }
//
//            browseTable(conn, TABLE_NAME);
            
        } finally {
            conn.close();
        }
    }
    
    public static void test3() throws Exception {
        Class.forName("cz.opentech.jdbc.xlsdriver.XlsDriver");
        final String URL = "jdbc:opentech:xls:test3.xls";
//        final String URL = "jdbc:opentech:xls:test3.xls;{test(colA text, colB numeric, colC bool)}";
        Connection conn = DriverManager.getConnection(URL);
//        final String TABLE_NAME = "\"test\""; 
        final String TABLE_NAME = "\"Sheet1\""; 
        try {
            browseTable(conn, TABLE_NAME);
            int count;
            Statement stmt = conn.createStatement();
            try {
                ResultSet rs = stmt.executeQuery("select count(*) from " + TABLE_NAME);
                if (rs.next()) count = rs.getInt(1);
                else throw new IllegalStateException();
                rs.close();
            } finally {
                stmt.close();
            }
            
            stmt = conn.createStatement();
            try {
                stmt.execute("insert into " + TABLE_NAME + " values (" + ++count
                        + ", 'Zdrastvuj', " + Math.random() + ", null)");
            } finally {
                stmt.close();
            }
            
            browseTable(conn, TABLE_NAME);

            stmt = conn.createStatement();
            try {
                stmt.execute("update " + TABLE_NAME + " set num=" + Math.random());
            } finally {
                stmt.close();
            }

            browseTable(conn, TABLE_NAME);
            
        } finally {
            conn.close();
        }
    }
    
    public static void run() throws Exception {
        
        Class.forName("cz.opentech.jdbc.xlsdriver.XlsDriver");
//        Class.forName("org.objectweb.rmijdbc.Driver");
        
//        final String XSL_URL = "jdbc:opentech:xls:test.xls;head=0;cols=0-3;rows=1-";
//        final String XSL_URL = "jdbc:opentech:xls:test.xls;{head=0;cols=0-3;rows=1-};";
        final String XSL_URL = "jdbc:opentech:xls:test2.xls;{List1;head=4;rows=5-;cols=0-};{List2;head=4;rows=5-;cols=0-}";//;{};{};{tabulka(colA numeric, colB text, colC numeric, colD date);head=4-5;cols=1-4;rows=7-};";
//        final String XSL_URL = "jdbc:rmi://localhost:2000/jdbc:opentech:xls:test.xls;{};{};{tabulka(colA numeric, colB text, colC numeric, colD date);head=4-5;cols=1-4;rows=7-};";
                
        Connection conn = DriverManager.getConnection(XSL_URL);
        try {
            browseTable(conn, "\"List1\"");
            browseTable(conn, "\"List2\"");
            browseTable(conn, "\"tabulka\"");
        } finally {
            conn.close();
        }
    }
    public static void browseMetadata(Connection conn) throws SQLException {
        ResultSet rs = null;
        
        try {
	        DatabaseMetaData meta = conn.getMetaData();
	        
	        rs = meta.getCatalogs();
	        String cat = null;
	        if (rs.next()) {
	            cat = rs.getString(1);
	        }
	        rs.close();
	        
	        rs = meta.getSchemas();
	        String schema = null;
	        if (rs.next()) {
	            schema = rs.getString(1);
	        }
	        rs.close();
	        
	        rs = meta.getTables(cat, schema, null, null);
	        if (rs.next()) {
	            String tabName = rs.getString(3);
	            browseTable(conn, tabName);
	        } else {
	            System.out.println("No tables");
	        }
        } finally {
            if (rs != null) rs.close();
        }
    }
    public static void browseTable(Connection conn, String table)
    		throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery("select * from " + table);
            ResultSetMetaData meta = rs.getMetaData();
            
            System.out.println();
            System.out.println("Browsing table " + table + "...");
            
            for (int i = 0; i < meta.getColumnCount(); i++) {
                if (i > 0) System.out.print("|");
                System.out.print(meta.getColumnName(i+1));
            }
            System.out.println();
            System.out.println("-------------------------------------------------------------");
            while (rs.next()) {
                for (int i = 0; i < meta.getColumnCount(); i++) {
                    if (i > 0) System.out.print("|");
                    switch (meta.getColumnType(i + 1)) {
                    case Types.BIGINT:
                    case Types.DECIMAL:
                    case Types.DOUBLE:
                    case Types.FLOAT:
                    case Types.INTEGER:
                    case Types.NUMERIC:
                    case Types.REAL:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    	System.out.print(rs.getDouble(i + 1));
                    	break;
                    case Types.CHAR:
                    case Types.LONGVARCHAR:
                    case Types.VARCHAR:
                    	System.out.print(rs.getString(i + 1));
                    	break;
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIMESTAMP:
                    	System.out.print(rs.getDate(i + 1));
                    }
                }
                System.out.println();
            }
        } finally {
            if (rs != null) rs.close();
            stmt.close();
        }
    }
}
