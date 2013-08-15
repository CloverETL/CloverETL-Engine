
package org.jetel.component;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import junit.framework.AssertionFailedError;

import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.SetVal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.PolicyType;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

public class CustomizedRecordTransformTest extends CloverTestCase {
	
	CustomizedRecordTransform transform;
	TransformationGraph graph;
	DataRecordMetadata metadata, metadata1, metaOut, metaOut1;
	DataRecord record, record1, out, out1;
	DataRecordMetadata[] inMetadata;
	DataRecordMetadata[] outMatedata;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
	    
		transform = new CustomizedRecordTransform(LogFactory.getLog(this.getClass()));
		
        graph=new TransformationGraph();
        
        transform.setGraph(graph);
		transform.setFieldPolicy(PolicyType.LENIENT);
        
		metadata=new DataRecordMetadata("in",DataRecordMetadata.DELIMITED_RECORD);
		
		metadata.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metadata.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		metadata.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n"));
		metadata.addField(new DataFieldMetadata("Value",DataFieldMetadata.DECIMAL_FIELD, "\n"));
		metadata.getField("Value").setFieldProperties(new Properties());
		metadata.getField("Value").getFieldProperties().setProperty(DataFieldMetadata.LENGTH_ATTR, "100");
		metadata.getField("Value").getFieldProperties().setProperty(DataFieldMetadata.SCALE_ATTR, "10");
		
		metadata1=new DataRecordMetadata("in1",DataRecordMetadata.DELIMITED_RECORD);
		
		metadata1.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metadata1.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metadata1.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		metadata1.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n"));
		metadata1.addField(new DataFieldMetadata("Value",DataFieldMetadata.INTEGER_FIELD, "\n"));
		
		metaOut=new DataRecordMetadata("out",DataRecordMetadata.DELIMITED_RECORD);
		
		metaOut.addField(new DataFieldMetadata("Name",DataFieldMetadata.BYTE_FIELD, ";"));
		metaOut.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metaOut.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		DataFieldMetadata dateMetadata = new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n");
		dateMetadata.setLocaleStr("en");
		metaOut.addField(dateMetadata);
		metaOut.addField(new DataFieldMetadata("Value",DataFieldMetadata.DECIMAL_FIELD, "\n"));
		metaOut.getField("Value").setFieldProperties(new Properties());
		metaOut.getField("Value").getFieldProperties().setProperty(DataFieldMetadata.LENGTH_ATTR, "4");
		metaOut.getField("Value").getFieldProperties().setProperty(DataFieldMetadata.SCALE_ATTR, "1");
		metaOut.getField("Value").setNullable(false);
		
		metaOut1=new DataRecordMetadata("out1",DataRecordMetadata.DELIMITED_RECORD);
		
		metaOut1.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metaOut1.addField(new DataFieldMetadata("Age",DataFieldMetadata.DECIMAL_FIELD, "|"));
		metaOut1.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		metaOut1.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n"));
		metaOut1.getField("Born").setFormatStr("dd-MM-yyyy");
		metaOut1.addField(new DataFieldMetadata("Value",DataFieldMetadata.INTEGER_FIELD, "\n"));
		metaOut1.getField("Age").setFieldProperties(new Properties());
		metaOut1.getField("Age").getFieldProperties().setProperty(DataFieldMetadata.LENGTH_ATTR, String.valueOf(DataFieldMetadata.INTEGER_LENGTH+1));
		metaOut1.getField("Age").getFieldProperties().setProperty(DataFieldMetadata.SCALE_ATTR, "1");

		inMetadata = new DataRecordMetadata[]{metadata, metadata1};
		outMatedata = new DataRecordMetadata[]{ metaOut, metaOut1};
		
		record = DataRecordFactory.newRecord(metadata);
		record.init();
		record1 = DataRecordFactory.newRecord(metadata1);
		record1.init();
		out = DataRecordFactory.newRecord(metaOut);
		out.init();
		out1 = DataRecordFactory.newRecord(metaOut1);
		out1.init();
		
		SetVal.setString(record,0,"  HELLO ");
		SetVal.setString(record1,0,"  My name ");
		SetVal.setInt(record,1,135);
		SetVal.setDouble(record1,1,13.25);
		SetVal.setString(record,2,"Some silly longer string.");
		SetVal.setString(record1,2,"Prague");
		SetVal.setValue(record1,3,Calendar.getInstance().getTime());
		record.getField("Born").setNull(true);
		SetVal.setInt(record,4,-999);
		record1.getField("Value").setNull(true);
        
        Sequence seq = SequenceFactory.createSequence(graph, "PRIMITIVE_SEQUENCE", new Object[]{"ID",graph,"name"}, new Class[]{String.class,TransformationGraph.class,String.class});
        graph.addSequence(seq);
		
        graph.getGraphParameters().addGraphParameter("WORKSPACE", "/home/avackova/workspace");
        graph.getGraphParameters().addGraphParameter("YourCity", "London");
	}
	
	@Override
	protected void tearDown() throws Exception {
		// TODO Auto-generated method stub
		super.tearDown();
	}
	
	public void test_fieldToField() {
		System.out.println("Field to field test:");
		System.out.println(record.getMetadata().getName() + ":\n" + record.toString());
		System.out.println(record1.getMetadata().getName() + ":\n" + record1.toString());
		System.out.println(out.getMetadata().getName() + ":\n" + out.toString());
		System.out.println(out1.getMetadata().getName() + ":\n" + out1.toString());
//		transform.setFieldPolicy(PolicyType.CONTROLLED);
		transform.addFieldToFieldRule("0.1", "0.1");
		transform.addFieldToFieldRule("${1.?a*}", "${1.*e}");
		transform.addFieldToFieldRule("${out.0.Name}", "${in.1.3}");
		transform.addFieldToFieldRule(2, "1.2");
		transform.addFieldToFieldRule(1, 3, "${1.3}");
		transform.addFieldToFieldRule("*.City", 0, 2);
		transform.addFieldToFieldRule("${out1.3}", 0, 3);
		transform.addFieldToFieldRule("${out.1.V*}", 0, "Value");
		transform.addFieldToFieldRule(1, 1, "in.Value");
		transform.addRule("out.Born", "${in.in1.Born}");
		transform.addFieldToFieldRule("out.Value", "in1.Value");
		transform.deleteRule("${out.out.Age}");
		try {
			transform.init(null, new DataRecordMetadata[]{metadata, metadata1}, 
				new DataRecordMetadata[]{metaOut,metaOut1});
		} catch (ComponentNotReadyException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		List<String> rules = transform.getRulesAsStrings();
		System.out.println("Rules:");
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		rules = transform.getResolvedRules();
		System.out.println("Resolved rules:");
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		List<Integer[]> fields = transform.getFieldsWithoutRules();
		System.out.println("Fields without rules:");
		Integer[] index;
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(outMatedata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					outMatedata[index[0]].getField(index[1]).getName());
		}
		fields = transform.getNotUsedFields();
		System.out.println("Not used input fields:");
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(inMetadata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					inMetadata[index[0]].getField(index[1]).getName());
		}
		System.out.println("Rule for field 0.0:");
		System.out.println(transform.getRule(0, 0));
		System.out.println("Rules with field 1.0:");
		fields = transform.getRulesWithField(1,0);
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(outMatedata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					outMatedata[index[0]].getField(index[1]).getName());
		}
		
		try {
			assertEquals(1, transform.transform(new DataRecord[]{record, record1}, new DataRecord[]{out,out1}));
		} catch (TransformException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		System.out.println(transform.getMessage());
		
		assertEquals(out.getField(0).toString(), record1.getField(3).getValue().toString());
//		assertEquals(out.getField(1).getValue(), record.getField(1).getValue());
		assertEquals(out.getField(2).getValue().toString(), record1.getField(2).getValue().toString());
		assertEquals(out.getField(3).getValue(), record1.getField(3).getValue());
		assertEquals(out1.getField(0).getValue().toString(), record1.getField(0).getValue().toString());
		assertEquals(out1.getField(2).getValue().toString(), record.getField(2).getValue().toString());
		assertEquals(out1.getField(3).getValue(), record.getField(3).getValue());
		assertEquals(out1.getField(1).getValue(), record.getField(4).getValue());
	}

	public void test_fieldToField2() {
		System.out.println("Field to field test:");
		
		DataFieldMetadata decField = new DataFieldMetadata("DecimalValue",DataFieldMetadata.DECIMAL_FIELD, "\n");
		metaOut.addField(decField);
		metadata1.addField(decField.duplicate());
		
		record1 = DataRecordFactory.newRecord(metadata1);
		record1.init();
		SetVal.setString(record1,0,"  My name ");
		SetVal.setDouble(record1,1,13.25);
		SetVal.setString(record1,2,"Prague");
		SetVal.setValue(record1,3,Calendar.getInstance().getTime());
		record1.getField("Value").setNull(true);
		SetVal.setDouble(record1, "DecimalValue", 1.1);
		out = DataRecordFactory.newRecord(metaOut);
		out.init();
        
		
		transform.setUseAlternativeRules(true);
		transform.addFieldToFieldRule("0.*", "0.*");
		transform.addFieldToFieldRule("0.*", "1.*");
		try {
			transform.init(null, new DataRecordMetadata[]{metadata, metadata1}, 
				new DataRecordMetadata[]{metaOut,metaOut1});
		} catch (ComponentNotReadyException e) {
			e.printStackTrace();
		}
		List<String> rules = transform.getRulesAsStrings();
		System.out.println("Rules:");
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		rules = transform.getResolvedRules();
		System.out.println("Resolved rules:");
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		List<Integer[]> fields = transform.getFieldsWithoutRules();
		System.out.println("Fields without rules:");
		Integer[] index;
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(outMatedata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					outMatedata[index[0]].getField(index[1]).getName());
		}
		fields = transform.getNotUsedFields();
		System.out.println("Not used input fields:");
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(inMetadata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					inMetadata[index[0]].getField(index[1]).getName());
		}
		try {
			assertEquals(RecordTransform.ALL, transform.transform(new DataRecord[]{record, record1}, new DataRecord[]{out,out1}));
		} catch (TransformException e) {
			System.out.println(e.getMessage());
		}
		
		for(int i = 0; i < metaOut.getNumFields() - 1; i++){
			try {
				if (!record.getField(i).isNull()) {
					assertEquals(out.getField(i), record.getField(i));
				}
			} catch (AssertionFailedError e) {
				assertEquals(out.getField(i).toString(), record.getField(i).toString());
			}
		}
		assertEquals(out.getField(metaOut.getNumFields() -1), record1.getField(metadata1.getNumFields() -1));
		assertEquals(out.getField("Born"), record1.getField("Born"));
		System.out.println(record.getMetadata().getName() + ":\n" + record.toString());
		System.out.println(record1.getMetadata().getName() + ":\n" + record1.toString());
		System.out.println(out.getMetadata().getName() + ":\n" + out.toString());
		System.out.println(out1.getMetadata().getName() + ":\n" + out1.toString());
	}
	
	public void test_constantToField(){
		System.out.println("Constant to field test:");
		transform.addConstantToFieldRule("*.Name", "Agata");
		transform.addConstantToFieldRule(1,3, new GregorianCalendar(1973,3,23).getTime());
		transform.addConstantToFieldRule(0, 1, 45.55);
		transform.addConstantToFieldRule("${out*.Valu?}", 100);
		transform.addConstantToFieldRule(1, "Age", DecimalFactory.getDecimal(new BigDecimal("36474.738393")));
		transform.addConstantToFieldRule("${out.*.2}", "Prague");
		transform.addConstantToFieldRule(0, 3, "2006-11-28");
		transform.addConstantToFieldRule(4, "1.111111111");
		transform.addRule("0.Name", "test");
		transform.deleteRule("0.?a*");
		try {
			transform.init(null, new DataRecordMetadata[]{metadata, metadata1}, 
				new DataRecordMetadata[]{metaOut,metaOut1});
		} catch (ComponentNotReadyException e) {
			e.printStackTrace();
		}
		List<String> rules = transform.getRulesAsStrings();
		System.out.println("Rules:");
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		rules = transform.getResolvedRules();
		System.out.println("Resolved rules:");
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		List<Integer[]> fields = transform.getFieldsWithoutRules();
		System.out.println("Fields without rules:");
		Integer[] index;
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(outMatedata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					outMatedata[index[0]].getField(index[1]).getName());
		}
		fields = transform.getNotUsedFields();
		System.out.println("Not used input fields:");
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(inMetadata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					inMetadata[index[0]].getField(index[1]).getName());
		}
		try {
			assertEquals(RecordTransform.ALL, transform.transform(new DataRecord[]{record, record1}, new DataRecord[]{out,out1}));
		} catch (TransformException e) {
			e.printStackTrace();
		}
		System.out.println(record.getMetadata().getName() + ":\n" + record.toString());
		System.out.println(record1.getMetadata().getName() + ":\n" + record1.toString());
		System.out.println(out.getMetadata().getName() + ":\n" + out.toString());
		System.out.println(out1.getMetadata().getName() + ":\n" + out1.toString());
//		assertEquals(out.getField(0).toString(), "test");
		assertEquals(out1.getField(0).getValue().toString(), "Agata");
		assertEquals(out1.getField(3).getValue(), new GregorianCalendar(1973,3,23).getTime());
		assertEquals(out.getField(1).getValue(), 45.55);
		assertEquals(out1.getField(4).getValue(), 100);
		assertEquals(out1.getField(1).getValue(), DecimalFactory.getDecimal(36474.738393,DataFieldMetadata.INTEGER_LENGTH + 1,1));
		assertEquals(out.getField(2).getValue().toString(), "Prague");
		assertEquals(out1.getField(2).getValue().toString(), "Prague");
		assertEquals(out.getField(3).getValue(), new GregorianCalendar(2006,10,28).getTime());
//		assertEquals(out.getField(4).getValue(), DecimalFactory.getDecimal(1.111111111,4,1));
	}

	public void test_sequenceToField(){
		System.out.println("Sequence to field test:");
		System.out.println(record.toString());
		System.out.println(record1.toString());
		System.out.println(out.toString());
		System.out.println(out1.toString());
		transform.addSequenceToFieldRule("*.Age", graph.getSequence("ID"));
		transform.addSequenceToFieldRule(4, "ID");
		transform.addSequenceToFieldRule("${1.Na*}", "ID.currentValueString()");
		transform.addSequenceToFieldRule(1, 2, "${seq.ID.nextValueString()}");
		transform.addSequenceToFieldRule(0, "Name", "id");
		transform.addRule("out.City", "${seq.ID.nextString}");
		transform.addSequenceToFieldRule("out.Born", graph.getSequence("ID"));
		transform.deleteRule("${o*.Value}");
		try {
			transform.init(null, new DataRecordMetadata[]{metadata, metadata1}, 
				new DataRecordMetadata[]{metaOut,metaOut1});
		} catch (ComponentNotReadyException e) {
			e.printStackTrace();
		}
		transform.deleteRule(3);
		try {
			transform.init(null, new DataRecordMetadata[]{metadata, metadata1}, 
				new DataRecordMetadata[]{metaOut,metaOut1});
		} catch (ComponentNotReadyException e) {
			e.printStackTrace();
		}
		List<String> rules = transform.getRulesAsStrings();
		System.out.println("Rules:");
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		rules = transform.getResolvedRules();
		System.out.println("Resolved rules:");
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		List<Integer[]> fields = transform.getFieldsWithoutRules();
		System.out.println("Fields without rules:");
		Integer[] index;
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(outMatedata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					outMatedata[index[0]].getField(index[1]).getName());
		}
		fields = transform.getNotUsedFields();
		System.out.println("Not used input fields:");
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(inMetadata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					inMetadata[index[0]].getField(index[1]).getName());
		}
		try {
			assertEquals(RecordTransform.ALL, transform.transform(new DataRecord[]{record, record1}, new DataRecord[]{out,out1}));
		} catch (TransformException e) {
			e.printStackTrace();
			System.out.println("Record number: " + e.getRecNo() + " , field number " + e.getFieldNo());
		}
		assertEquals(out.getField(1).getValue(), 0.0);
		assertEquals(out1.getField(1).getValue(), DecimalFactory.getDecimal(1.0));
//		assertEquals(out.getField(4).getValue(), DecimalFactory.getDecimal(3.0));
		assertEquals(out1.getField(0).getValue().toString(), "1");
		assertEquals(out1.getField(2).getValue().toString(), "2");
		assertEquals(out.getField(2).getValue().toString(), "3");
	}
	
	public void test_parameterToField(){
		System.out.println("Parameter to field test:");
		transform.addParameterToFieldRule("*.Age", "$ADULT");
		transform.addParameterToFieldRule(1, 0, "${WORKSPACE}");
		transform.addParameterToFieldRule(2, "MyCity");
		transform.addParameterToFieldRule(1, "City", "YourCity");
		transform.addRule("out.Value", "${par.ADULT}");
		transform.deleteRule(1, "Age");
		Properties properties = new Properties();
		properties.setProperty("$ADULT", "18");
		properties.setProperty("$MyCity", "Prague");
		try {
			transform.init(properties, new DataRecordMetadata[]{metadata, metadata1}, 
				new DataRecordMetadata[]{metaOut,metaOut1});
		} catch (ComponentNotReadyException e) {
			e.printStackTrace();
		}
		List<String> rules = transform.getRulesAsStrings();
		System.out.println("Rules:");
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		rules = transform.getResolvedRules();
		System.out.println("Resolved rules:");
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		List<Integer[]> fields = transform.getFieldsWithoutRules();
		System.out.println("Fields without rules:");
		Integer[] index;
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(outMatedata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					outMatedata[index[0]].getField(index[1]).getName());
		}
		fields = transform.getNotUsedFields();
		System.out.println("Not used input fields:");
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(inMetadata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					inMetadata[index[0]].getField(index[1]).getName());
		}
		try {
			assertEquals(RecordTransform.ALL, transform.transform(new DataRecord[]{record, record1}, new DataRecord[]{out,out1}));
		} catch (TransformException e) {
			e.printStackTrace();
		}
		System.out.println(record.getMetadata().getName() + ":\n" + record.toString());
		System.out.println(record1.getMetadata().getName() + ":\n" + record1.toString());
		System.out.println(out.getMetadata().getName() + ":\n" + out.toString());
		System.out.println(out1.getMetadata().getName() + ":\n" + out1.toString());
		assertEquals((Double)out.getField(1).getValue(), DecimalFactory.getDecimal(properties.getProperty("$ADULT")).getDouble());
//		assertEquals(out1.getField(1).getValue(), DecimalFactory.getDecimal(Integer.valueOf(properties.getProperty("$ADULT")),DataFieldMetadata.INTEGER_LENGTH +1,1));
		assertEquals(out1.getField(0).getValue().toString(), graph.getGraphParameters().getGraphParameter("WORKSPACE").getValue());
		assertEquals(out.getField(2).getValue().toString(), properties.getProperty("$MyCity"));
		assertEquals(out1.getField(2).getValue().toString(), graph.getGraphParameters().getGraphParameter("YourCity").getValue());
		assertEquals(out.getField(4).getValue(), DecimalFactory.getDecimal(Integer.valueOf(properties.getProperty("$ADULT")),4,1));
	}
	
	public void test_complex(){
		System.out.println("Complex:");
		transform.addFieldToFieldRule("${1.?a*}", "${1.*e}");
		transform.addFieldToFieldRule("*.City", 0, 2);
		transform.addConstantToFieldRule(1,3, new GregorianCalendar(1973,3,23).getTime());
		transform.addConstantToFieldRule(4, "1.111111111");
		transform.addSequenceToFieldRule("*.Age", graph.getSequence("ID"));
		transform.addRule("out.City", "${seq.ID.nextString}");
		transform.addParameterToFieldRule(1, 0, "${WORKSPACE}");
		transform.addParameterToFieldRule(1, "City", "YourCity");
		transform.deleteRule(1, "Age");
		Properties properties = new Properties();
		properties.setProperty("$ADULT", "18");
		properties.setProperty("$MyCity", "Prague");
		try {
			transform.init(properties, new DataRecordMetadata[]{metadata, metadata1}, 
				new DataRecordMetadata[]{metaOut,metaOut1});
		} catch (ComponentNotReadyException e) {
			e.printStackTrace();
		}
		List<String> rules = transform.getRulesAsStrings();
		System.out.println("Rules:");
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		rules = transform.getResolvedRules();
		System.out.println("Resolved rules:");
		for (Iterator<String> i = rules.iterator();i.hasNext();){
			System.out.println(i.next());
		}
		List<Integer[]> fields = transform.getFieldsWithoutRules();
		System.out.println("Fields without rules:");
		Integer[] index;
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(outMatedata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					outMatedata[index[0]].getField(index[1]).getName());
		}
		fields = transform.getNotUsedFields();
		System.out.println("Not used input fields:");
		for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
			index = i.next();
			System.out.println(inMetadata[index[0]].getName() + 
					CustomizedRecordTransform.DOT + 
					inMetadata[index[0]].getField(index[1]).getName());
		}
		try {
			assertEquals(RecordTransform.ALL, transform.transform(new DataRecord[]{record, record1}, new DataRecord[]{out,out1}));
		} catch (TransformException e) {
			e.printStackTrace();
		}
		System.out.println(record.getMetadata().getName() + ":\n" + record.toString());
		System.out.println(record1.getMetadata().getName() + ":\n" + record1.toString());
		System.out.println(out.getMetadata().getName() + ":\n" + out.toString());
		System.out.println(out1.getMetadata().getName() + ":\n" + out1.toString());
	}
	
}
