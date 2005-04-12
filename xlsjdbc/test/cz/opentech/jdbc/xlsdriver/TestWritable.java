/*
 * Created on 5.4.2005
 *
 */
package cz.opentech.jdbc.xlsdriver;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import jxl.Sheet;
import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;

/**
 * @author vitaz
 *
 */
public class TestWritable {

    public static void main(String[] args) throws Exception {
        FileInputStream in = new FileInputStream("test3.xls");
        Workbook wb = Workbook.getWorkbook(in);
        in.close();
//        Sheet sheet = wb.getSheet(0);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        WritableWorkbook wwb = Workbook.createWorkbook(out, wb);
        WritableSheet ws = wwb.getSheet(0);
        for (int i = 0; i < 10; i++) {
            ws.addCell(new Label(0, i, "Row no. " + i));
        }
        wwb.write();
        wwb.close();
        
        FileOutputStream fout = new FileOutputStream("test3.out.xls");
        fout.write(out.toByteArray());
        fout.close();
        //out.close();
    }
    
}
