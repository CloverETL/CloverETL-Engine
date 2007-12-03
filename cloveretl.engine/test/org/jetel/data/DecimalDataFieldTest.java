
package org.jetel.data;

import java.util.Properties;

import junit.framework.TestCase;

import org.jetel.data.primitive.Decimal;
<<<<<<< .working
import org.jetel.graph.runtime.EngineInitializer;
=======
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.main.runGraph;
>>>>>>> .merge-right.r3636
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

public class DecimalDataFieldTest extends TestCase {
	
	private DataField field1;
	private DataFieldMetadata fieldMetadata;
	
	@Override
	protected void setUp() throws Exception {
<<<<<<< .working
		EngineInitializer.initEngine(null, null);
=======
	    EngineInitializer.initEngine(null, null);
>>>>>>> .merge-right.r3636
		fieldMetadata = new DataFieldMetadata("field1",	DataFieldMetadata.DECIMAL_FIELD,";");  
	}
	
	public void test_1(){
		field1 = new DecimalDataField(fieldMetadata,50,10);
		field1.setValue(8.859493587791455E25);
		System.out.println("Value set to: " + field1.getValue());
	}

	public void test_2(){
		Properties fieldProperties = new Properties();
		fieldProperties.put(DataFieldMetadata.LENGTH_ATTR, "50");
		fieldProperties.put(DataFieldMetadata.SCALE_ATTR, "10");
		fieldMetadata.setFieldProperties(fieldProperties);
		field1 = DataFieldFactory.createDataField(fieldMetadata, true);
		field1.setValue(8.859493587791455E25);
		System.out.println("Value set to: " + field1.getValue());
	}
	
	public void test_3(){
		fieldMetadata.setLocaleStr("cs.CZ");
		DataRecordMetadata recordMetadata = new DataRecordMetadata("recordMetadata");
		recordMetadata.addField(fieldMetadata);
		DataRecord record = new DataRecord(recordMetadata);
		record.init();
		String number = "11,28";//NumberFormat.getInstance().format(11.28);
		record.getField(0).fromString(number);
		System.out.println(record);
		assertEquals(11.28, ((Decimal)record.getField(0).getValue()).getDouble());
	}
	
}
