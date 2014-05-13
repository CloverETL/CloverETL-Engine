/**
 * 
 */
package org.jetel.data.formatter;

import java.io.FileOutputStream;
import java.util.Calendar;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.database.dbf.DBFDataFormatter;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;


public class DBFDataFormatterTest extends CloverTestCase {

	private final static String TEST_DBF_FILE = "data/test_output.dbf";
	
	DataRecordMetadata metadata;
	DataRecord record;
	
	Formatter formatter;
	
	private int oldBufferSize;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		oldBufferSize = Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE;
		metadata = new DataRecordMetadata("meta", DataRecordMetadata.FIXEDLEN_RECORD);
		metadata.addField(new DataFieldMetadata("Field1", DataFieldMetadata.STRING_FIELD, (short)5));
		metadata.addField(new DataFieldMetadata("Field2", DataFieldMetadata.STRING_FIELD, (short)10));
		metadata.addField(new DataFieldMetadata("Field3", DataFieldMetadata.STRING_FIELD, (short)40));
		metadata.addField(new DataFieldMetadata("Field4", DataFieldMetadata.DATE_FIELD, (short)15));
		metadata.addField(new DataFieldMetadata("Field5", DataFieldMetadata.DECIMAL_FIELD, (short)18));
		metadata.getField(4).setProperty(DataFieldMetadata.SCALE_ATTR, "3");
		
		record = DataRecordFactory.newRecord(metadata);
		record.init();
		formatter = new DBFDataFormatter("US-ASCII", (byte) 0x03);
		formatter.init(metadata);
		
		record.getField(0).setValue("AB");
		record.getField(1).setValue("0123456789ABCDEFGH");
		record.getField(2).setValue("XXXXXXXXXX");
		record.getField(3).setValue(Calendar.getInstance().getTime());
		Decimal dec=DecimalFactory.getDecimal(10, 3);
		dec.setValue(123.456d);
		record.getField(4).setValue(dec);
		
	}

	public void testParsers() throws Exception {
		formatter.setDataTarget(new FileOutputStream(TEST_DBF_FILE).getChannel());
		formatter.writeHeader();
		for(int i=0;i<100;i++){
			formatter.write(record);
		}
		formatter.writeFooter();
		formatter.close();
	}

	
	@Override
	@SuppressWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
	protected void tearDown() throws Exception {
		Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE = oldBufferSize;
	}
}
