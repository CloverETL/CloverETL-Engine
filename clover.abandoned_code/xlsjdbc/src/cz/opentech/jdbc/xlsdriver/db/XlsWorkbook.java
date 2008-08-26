/*
 * Created on 6.4.2005
 *
 */
package cz.opentech.jdbc.xlsdriver.db;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import jxl.JXLException;
import jxl.Workbook;
import jxl.write.WritableWorkbook;


public class XlsWorkbook {
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private String file;
    private WritableWorkbook writebook;
    
    public XlsWorkbook() {
        // empty
    }
    public void load(String path) throws IOException {
        this.file = path;
        buffer.reset();
        File f = new File(path);
        if (f.exists() && f.length() > 0) {
	        FileInputStream fin = new FileInputStream(path);
	        Workbook rwb = null;
	        try {
	            rwb = Workbook.getWorkbook(fin);
	        } catch (JXLException e) {
	            throw new IOException(e.getMessage());
	        } finally {
	            fin.close();
	        }
	        writebook = Workbook.createWorkbook(buffer, rwb);
        } else { // create empty workbook
            writebook = Workbook.createWorkbook(buffer);
        }
    }
    public void flush() throws IOException {
        try {
            writebook.write();
            writebook.close();
        	FileOutputStream fout = new FileOutputStream(file);
        	try {
        	    fout.write(buffer.toByteArray());
        	} finally {
        	    fout.close();
        	}
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        } finally {
            load(file);
        }
    }
    public void close() throws IOException {
        try {
            writebook.close();
            writebook = null;
            // delete empty sheet
            File f = new File(file);
            if (f.length() == 0) {
                f.delete();
            }
            file = null;
        } catch (JXLException e) {
            throw new IOException(e.getMessage());
        }
    }
    public WritableWorkbook getWWB() {
        return writebook;
    }
}