
package org.jetel.data;

import java.util.Properties;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

import junit.framework.TestCase;

public class DecimalDataFieldTest extends TestCase {
	
	private DataField field1;
	private DataFieldMetadata fieldMetadata = new DataFieldMetadata("field1",
			DataFieldMetadata.DECIMAL_FIELD,";"); 
	
	public void test_1(){
		field1 = new DecimalDataField(fieldMetadata,50,30);
		field1.setValue(8.859493587791455E25);
		System.out.println("Value set to: " + field1.getValue());
	}

	public void test_2(){
		Properties fieldProperties = new Properties();
		fieldProperties.put(DataFieldMetadata.LENGTH_ATTR, "50");
		fieldProperties.put(DataFieldMetadata.SCALE_ATTR, "30");
		fieldMetadata.setFieldProperties(fieldProperties);
		field1 = DataFieldFactory.createDataField(fieldMetadata, true);
		field1.setValue(8.859493587791455E25);
		System.out.println("Value set to: " + field1.getValue());
	}
	
	public void test_3(){
		DataRecordMetadata recordMetadata = new DataRecordMetadata("recordMetadata");
		recordMetadata.addField(fieldMetadata);
		DataRecord record = new DataRecord(recordMetadata);
		record.init();
	}
	
}
