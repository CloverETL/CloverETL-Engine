
package org.jetel.component;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.SetVal;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.sequence.PrimitiveSequence;

import junit.framework.TestCase;

public class CustomizedRecordTransformTest extends TestCase {
	
	CustomizedRecordTransform transform;
	TransformationGraph graph;
	DataRecordMetadata metadata, metadata1, metaOut, metaOut1;
	DataRecord record, record1, out, out1;
	
	@Override
	protected void setUp() throws Exception {
		transform = new CustomizedRecordTransform();
		
	    Defaults.init();
	    
        graph=new TransformationGraph();
        
		metadata=new DataRecordMetadata("in",DataRecordMetadata.DELIMITED_RECORD);
		
		metadata.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metadata.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		metadata.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n"));
		metadata.addField(new DataFieldMetadata("Value",DataFieldMetadata.INTEGER_FIELD, "\n"));
		
		metadata1=new DataRecordMetadata("in1",DataRecordMetadata.DELIMITED_RECORD);
		
		metadata1.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metadata1.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metadata1.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		metadata1.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n"));
		metadata1.addField(new DataFieldMetadata("Value",DataFieldMetadata.INTEGER_FIELD, "\n"));
		
		metaOut=new DataRecordMetadata("out",DataRecordMetadata.DELIMITED_RECORD);
		
		metaOut.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metaOut.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metaOut.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		metaOut.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n"));
		metaOut.addField(new DataFieldMetadata("Value",DataFieldMetadata.INTEGER_FIELD, "\n"));
				
		metaOut1=new DataRecordMetadata("out1",DataRecordMetadata.DELIMITED_RECORD);
		
		metaOut1.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metaOut1.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metaOut1.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		metaOut1.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n"));
		metaOut1.addField(new DataFieldMetadata("Value",DataFieldMetadata.INTEGER_FIELD, "\n"));

		record = new DataRecord(metadata);
		record.init();
		record1 = new DataRecord(metadata1);
		record1.init();
		out = new DataRecord(metaOut);
		out.init();
		out1 = new DataRecord(metaOut1);
		out1.init();
		
		SetVal.setString(record,0,"  HELLO ");
		SetVal.setString(record1,0,"  My name ");
		SetVal.setInt(record,1,135);
		SetVal.setDouble(record1,1,13);
		SetVal.setString(record,2,"Some silly longer string.");
		SetVal.setString(record1,2,"Prague");
		SetVal.setValue(record1,3,Calendar.getInstance().getTime());
		record.getField("Born").setNull(true);
		SetVal.setInt(record,4,-999);
		record1.getField("Value").setNull(true);
        
        Sequence seq = new PrimitiveSequence("test",graph,"test");
        graph.addSequence("test", seq);
		
	}
	
	@Override
	protected void tearDown() throws Exception {
		// TODO Auto-generated method stub
		super.tearDown();
	}
	
	public void test_fieldToField() {
		System.out.println("Field to field test:");
		transform.addFieldToFieldRule("0.1", "0.1");
		transform.addFieldToFieldRule("${1.?a*}", "${1.*e}");
		transform.addFieldToFieldRule("${out.0.Name}", "${in.0.0}");
		transform.addFieldToFieldRule(2, "1.2");
		transform.addFieldToFieldRule(0, 3, "${1.3}");
		transform.addFieldToFieldRule(0, "Value", "${in.1.Age}");
		transform.addFieldToFieldRule("*.City", 0, 2);
		transform.addFieldToFieldRule("{out1.3}", 0, 3);
		transform.addFieldToFieldRule("${out.1.V*}", 0, "Value");
		try {
			transform.init(null, new DataRecordMetadata[]{metadata, metadata1}, 
				new DataRecordMetadata[]{metaOut,metaOut1});
		} catch (ComponentNotReadyException e) {
			e.printStackTrace();
		}
		List<String> rules = transform.getResolvedRules();
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		try {
			transform.transform(new DataRecord[]{record, record1}, new DataRecord[]{out,out1});
		} catch (TransformException e) {
			e.printStackTrace();
		}
		assertEquals(out.getField(0).getValue().toString(), record.getField(0).getValue().toString());
		assertEquals(out.getField(1).getValue(), record.getField(1).getValue());
		assertEquals(out.getField(2).getValue().toString(), record.getField(2).getValue().toString());
		assertEquals(out.getField(3).getValue(), record1.getField(3).getValue());
		assertEquals(out.getField(4).getValue(), ((Double)record1.getField(1).getValue()).intValue());
		assertEquals(out1.getField(0).getValue().toString(), record1.getField(0).getValue().toString());
		assertEquals(out1.getField(1).getValue(),0.0);
		assertEquals(out1.getField(2).getValue().toString(), record.getField(2).getValue().toString());
		assertEquals(out1.getField(3).getValue(), record.getField(3).getValue());
		assertEquals(out1.getField(4).getValue(), record1.getField(4).getValue());
		System.out.println(record.toString());
		System.out.println(record1.toString());
		System.out.println(out.toString());
		System.out.println(out1.toString());
	}

	public void test_fieldToConstant(){
		System.out.println("Constant to field test:");
		transform.addFieldToFieldRule("*.Name", "Agata");
		transform.addConstantToFieldRule(3, new GregorianCalendar(23,3,73).getTime());
		transform.addConstantToFieldRule(0, 4, 45.55);
		try {
			transform.init(null, new DataRecordMetadata[]{metadata, metadata1}, 
				new DataRecordMetadata[]{metaOut,metaOut1});
		} catch (ComponentNotReadyException e) {
			e.printStackTrace();
		}
		List<String> rules = transform.getResolvedRules();
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		try {
			transform.transform(new DataRecord[]{record, record1}, new DataRecord[]{out,out1});
		} catch (TransformException e) {
			e.printStackTrace();
		}
		assertEquals(out.getField(0).getValue().toString(), "Agata");
		assertEquals(out1.getField(0).getValue().toString(), "Agata");
		assertEquals(out.getField(3).getValue(), new GregorianCalendar(23,3,73).getTime());
		assertEquals(out.getField(4).getValue().toString(), 45.55);
		System.out.println(record.toString());
		System.out.println(record1.toString());
		System.out.println(out.toString());
		System.out.println(out1.toString());
	}

}
