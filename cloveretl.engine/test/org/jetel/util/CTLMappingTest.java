/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.util;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.7.2012
 */
public class CTLMappingTest extends CloverTestCase {

    @Override
	protected void setUp() throws Exception {
        super.setUp();
        initEngine();
    }

    private CTLMapping createCTLMapping() {
    	TransformationGraph graph = new TransformationGraph();
    	Node component = new Node("componentId", graph) {
			@Override
			public String getType() {
				return "MY_COMPONENT";
			}
			
			@Override
			protected Result execute() throws Exception {
				return null;
			}
		};
		
		return new CTLMapping("My mapping", component);
    }
    
    private DataRecordMetadata createMetadata(String name, String... fieldNames) {
    	DataRecordMetadata metadata = new DataRecordMetadata(name);
    	
    	DataFieldMetadata field;
    	for (String fieldName : fieldNames) {
    		field = new DataFieldMetadata(fieldName, DataFieldType.STRING, "|");
    		metadata.addField(field);
    	}
    	return metadata;
    }
    
    public void testAddInputMetadata() {
    	CTLMapping ctlMapping = createCTLMapping();
    	DataRecord record;
    	
    	try {
    		ctlMapping.addInputMetadata(null, null);
    		fail();
    	} catch (AssertionError e) {
    		//OK
    	}
    	
		record = ctlMapping.addInputMetadata("first", null);
		assertNull(record);
		assertNull(ctlMapping.getInputRecord("first"));
    	
    	DataRecordMetadata metadata = createMetadata("myMetadata", "field1", "field2", "field3");
    	record = ctlMapping.addInputMetadata("second", metadata);
    	
    	assertTrue(record.getMetadata() == metadata);
		assertNull(ctlMapping.getInputRecord("first"));
    	assertTrue(ctlMapping.getInputRecord("second") == record);
    	assertNull(ctlMapping.getInputRecord("third"));
    	
    	assertTrue(ctlMapping.hasInput("first"));
    	assertTrue(ctlMapping.hasInput("second"));
    	assertFalse(ctlMapping.hasInput("third"));
    }

    public void testAddInputRecord() {
    	CTLMapping ctlMapping = createCTLMapping();
    	DataRecord record;
    	
    	try {
    		ctlMapping.addInputRecord(null, null);
    		fail();
    	} catch (AssertionError e) {
    		//OK
    	}
    	
		ctlMapping.addInputRecord("first", null);
		assertNull(ctlMapping.getInputRecord("first"));
    	
    	DataRecordMetadata metadata = createMetadata("myMetadata", "field1", "field2", "field3");
    	record = DataRecordFactory.newRecord(metadata);
    	ctlMapping.addInputRecord("second", record);
    	
		assertNull(ctlMapping.getInputRecord("first"));
    	assertTrue(ctlMapping.getInputRecord("second") == record);
    	assertNull(ctlMapping.getInputRecord("third"));

    	assertTrue(ctlMapping.hasInput("first"));
    	assertTrue(ctlMapping.hasInput("second"));
    	assertFalse(ctlMapping.hasInput("third"));
    }
    
    public void testAddOutputMetadata() {
    	CTLMapping ctlMapping = createCTLMapping();
    	DataRecord record;
    	
    	try {
    		ctlMapping.addOutputMetadata(null, null);
    		fail();
    	} catch (AssertionError e) {
    		//OK
    	}
    	
		record = ctlMapping.addOutputMetadata("first", null);
		assertNull(record);
		assertNull(ctlMapping.getOutputRecord("first"));
    	
    	DataRecordMetadata metadata = createMetadata("myMetadata", "field1", "field2", "field3");
    	record = ctlMapping.addOutputMetadata("second", metadata);
    	
    	assertTrue(record.getMetadata() == metadata);
		assertNull(ctlMapping.getOutputRecord("first"));
    	assertTrue(ctlMapping.getOutputRecord("second") == record);
    	assertNull(ctlMapping.getOutputRecord("third"));
    	
    	assertTrue(ctlMapping.hasOutput("first"));
    	assertTrue(ctlMapping.hasOutput("second"));
    	assertFalse(ctlMapping.hasOutput("third"));
    }

    public void testAddOutputRecord() {
    	CTLMapping ctlMapping = createCTLMapping();
    	DataRecord record;
    	
    	try {
    		ctlMapping.addOutputRecord(null, null);
    		fail();
    	} catch (AssertionError e) {
    		//OK
    	}
    	
		ctlMapping.addOutputRecord("first", null);
		assertNull(ctlMapping.getOutputRecord("first"));
    	
    	DataRecordMetadata metadata = createMetadata("myMetadata", "field1", "field2", "field3");
    	record = DataRecordFactory.newRecord(metadata);
    	ctlMapping.addOutputRecord("second", record);
    	
		assertNull(ctlMapping.getOutputRecord("first"));
    	assertTrue(ctlMapping.getOutputRecord("second") == record);
    	assertNull(ctlMapping.getOutputRecord("third"));
    	
    	assertTrue(ctlMapping.hasOutput("first"));
    	assertTrue(ctlMapping.hasOutput("second"));
    	assertFalse(ctlMapping.hasOutput("third"));
    }

    public void testComplex() throws Exception {
    	CTLMapping mapping = createCTLMapping();
    	
    	DataRecordMetadata inMetadata = createMetadata("inMetadata", "field1_in", "field2_in", "field3_in", "field4_out");
    	DataRecord inRecord = mapping.addInputMetadata("firstIn", inMetadata);
    	
    	DataRecordMetadata outMetadata = createMetadata("outMetadata", "field1_out", "field2_out", "field3_out", "field4_out");
    	DataRecord outRecord = mapping.addOutputMetadata("firstOut", outMetadata);

    	mapping.setTransformation("//#CTL2\n" +
    			"function integer transform() {" +
    			"$out.0.field1_out = $in.0.field1_in;" +
    			"$out.0.field2_out = $in.0.field2_in;" +
    			"$out.0.field3_out = $in.0.field3_in;" +
    			"string tmp = $in.0.field4_out;" +
    			"return ALL;" +
    			"}");
    	
    	mapping.init(null);
    	mapping.preExecute();
    	
    	
    	inRecord.getField(0).setValue("neco1");
    	
    	mapping.execute();
    	
    	assertEquals("neco1", mapping.getOutput("firstOut", "field1_out").toString());
    	assertTrue(mapping.isOutputOverridden(outRecord, outRecord.getField("field1_out")));
    	assertTrue(mapping.isOutputOverridden(outRecord, outRecord.getField("field2_out")));
    	assertFalse(mapping.isOutputOverridden(outRecord, outRecord.getField("field4_out")));
    	
    	try {
    		mapping.isOutputOverridden(inRecord, inRecord.getField("field1_in"));
    		fail();
    	} catch (IllegalArgumentException e) {
    		//OK
    	}

    	assertEquals("neco1", mapping.getOutputRecord("firstOut").getField(0).getValue().toString());
    	assertNull(mapping.getOutputRecord("firstOut").getField(1).getValue());
    	assertNull(mapping.getOutputRecord("firstOut").getField(2).getValue());
    	
    	mapping.postExecute();
    }

    public void testComplex1() throws Exception {
    	CTLMapping mapping = createCTLMapping();
    	
    	DataRecordMetadata inMetadata = createMetadata("inMetadata", "field1", "field2", "field3");
    	DataRecord inRecord = mapping.addInputMetadata("firstIn", inMetadata);
    	
    	DataRecord outRecord = mapping.addOutputMetadata("firstOut", inMetadata);

    	mapping.setTransformation("//#CTL2\n" +
    			"function integer transform() {" +
    			"$out.0.field1 = $in.0.field1;" +
    			"$out.0.field2 = $in.0.field2;" +
    			"$out.0.field3 = $in.0.field3;" +
    			"return ALL;" +
    			"}");
    	
    	mapping.init(null);
    	mapping.preExecute();
    	
    	
    	inRecord.getField(0).setValue("neco1");
    	
    	mapping.execute();
    	
    	assertEquals("neco1", mapping.getOutput("firstOut", "field1").toString());
    	assertTrue(mapping.isOutputOverridden(outRecord, outRecord.getField("field1")));
    	assertTrue(mapping.isOutputOverridden(outRecord, outRecord.getField("field2")));
    	
    	try {
    		mapping.isOutputOverridden(inRecord, inRecord.getField("field1"));
    		fail();
    	} catch (IllegalArgumentException e) {
    		//OK
    	}

    	assertEquals("neco1", mapping.getOutputRecord("firstOut").getField(0).getValue().toString());
    	assertNull(mapping.getOutputRecord("firstOut").getField(1).getValue());
    	assertNull(mapping.getOutputRecord("firstOut").getField(2).getValue());
    	
    	mapping.postExecute();
    }

    public void testComplex2() throws Exception {
    	CTLMapping mapping = createCTLMapping();
    	
    	DataRecordMetadata inMetadata0 = createMetadata("inMetadata1", "field1", "field2", "field3");
    	DataRecord inRecord0 = mapping.addInputMetadata("firstIn", inMetadata0);
    	
    	DataRecordMetadata inMetadata1 = createMetadata("inMetadata2", "field1", "field2", "field3");
    	DataRecord inRecord1 = mapping.addInputMetadata("secondIn", inMetadata1);
    	
    	DataRecordMetadata outMetadata0 = createMetadata("outMetadata1", "field1", "field2", "field3", "field4");
    	DataRecord outRecord0 = mapping.addOutputMetadata("firstOut", outMetadata0);

    	DataRecordMetadata outMetadata1 = createMetadata("outMetadata2", "field1", "field2", "field3", "field4");
    	DataRecord outRecord1 = mapping.addOutputMetadata("secondOut", outMetadata1);

    	mapping.addAutoMapping("firstIn", "firstOut");
    	mapping.addAutoMapping("secondIn", "secondOut");

    	mapping.setTransformation("//#CTL2\n" +
    			"function integer transform() {" +
    			"$out.0.field1 = $in.1.field1;" +
    			"$out.0.field2 = $in.1.field2;" +
    			"$out.0.field3 = $in.1.field3;" +
    			"$out.1.field1 = $in.0.field1;" +
    			"$out.1.field2 = $in.0.field2;" +
    			"$out.1.field3 = $in.0.field3;" +
    			"return ALL;" +
    			"}");
    	
    	mapping.init(null);
    	mapping.preExecute();
    	
    	
    	inRecord0.getField(0).setValue("neco1");
    	inRecord1.getField(1).setValue("neco2");
    	
    	mapping.execute();
    	
    	assertEquals("neco1", mapping.getOutput("secondOut", "field1").toString());
    	assertEquals("neco2", mapping.getOutput("firstOut", "field2").toString());
    	assertTrue(mapping.isOutputOverridden(outRecord0, outRecord0.getField("field2")));
    	assertTrue(mapping.isOutputOverridden(outRecord0, outRecord0.getField("field1")));
    	assertFalse(mapping.isOutputOverridden(outRecord0, outRecord0.getField("field4")));
    	assertTrue(mapping.isOutputOverridden(outRecord1, outRecord1.getField("field1")));
    	assertTrue(mapping.isOutputOverridden(outRecord1, outRecord1.getField("field3")));
    	assertFalse(mapping.isOutputOverridden(outRecord1, outRecord1.getField("field4")));

    	assertEquals("neco1", mapping.getOutputRecord("secondOut").getField(0).getValue().toString());
    	assertNull(mapping.getOutputRecord("secondOut").getField(1).getValue());
    	assertNull(mapping.getOutputRecord("secondOut").getField(2).getValue());

    	assertNull(mapping.getOutputRecord("firstOut").getField(0).getValue());
    	assertEquals("neco2", mapping.getOutputRecord("firstOut").getField(1).getValue().toString());
    	assertNull(mapping.getOutputRecord("firstOut").getField(2).getValue());

    	mapping.postExecute();
    }

    public void testComplex3() throws Exception {
    	CTLMapping mapping = createCTLMapping();
    	
    	DataRecordMetadata inMetadata0 = createMetadata("inMetadata1", "field1", "field2", "field3");
    	DataRecord inRecord0 = mapping.addInputMetadata("firstIn", inMetadata0);
    	
    	DataRecordMetadata inMetadata1 = createMetadata("inMetadata2", "field1", "field2", "field3");
    	DataRecord inRecord1 = mapping.addInputMetadata("secondIn", inMetadata1);
    	
    	DataRecordMetadata outMetadata0 = createMetadata("outMetadata1", "field1", "field2", "field3", "field4");
    	DataRecord outRecord0 = mapping.addOutputMetadata("firstOut", outMetadata0);

    	DataRecordMetadata outMetadata1 = createMetadata("outMetadata2", "field1", "field2", "field3", "field4");
    	DataRecord outRecord1 = mapping.addOutputMetadata("secondOut", outMetadata1);

    	mapping.addAutoMapping("firstIn", "secondOut");
    	mapping.addAutoMapping("secondIn", "firstOut");
    	
    	mapping.init(null);
    	mapping.preExecute();
    	
    	
    	inRecord0.getField(0).setValue("neco1");
    	inRecord1.getField(1).setValue("neco2");

    	mapping.execute();
    	
    	assertEquals("neco1", mapping.getOutput("secondOut", "field1").toString());
    	assertEquals("neco2", mapping.getOutput("firstOut", "field2").toString());
    	assertTrue(mapping.isOutputOverridden(outRecord0, outRecord0.getField("field2")));
    	assertTrue(mapping.isOutputOverridden(outRecord0, outRecord0.getField("field1")));
    	assertFalse(mapping.isOutputOverridden(outRecord0, outRecord0.getField("field4")));
    	assertTrue(mapping.isOutputOverridden(outRecord1, outRecord1.getField("field1")));
    	assertTrue(mapping.isOutputOverridden(outRecord1, outRecord1.getField("field3")));
    	assertFalse(mapping.isOutputOverridden(outRecord1, outRecord1.getField("field4")));

    	assertEquals("neco1", mapping.getOutputRecord("secondOut").getField(0).getValue().toString());
    	assertNull(mapping.getOutputRecord("secondOut").getField(1).getValue());
    	assertNull(mapping.getOutputRecord("secondOut").getField(2).getValue());

    	assertNull(mapping.getOutputRecord("firstOut").getField(0).getValue());
    	assertEquals("neco2", mapping.getOutputRecord("firstOut").getField(1).getValue().toString());
    	assertNull(mapping.getOutputRecord("firstOut").getField(2).getValue());

    	mapping.getOutputRecord("firstOut").getField("field1").setValue("neco3");
    	
    	mapping.execute();
    	
    	assertEquals("neco1", mapping.getOutput("secondOut", "field1").toString());
    	assertEquals("neco2", mapping.getOutput("firstOut", "field2").toString());
    	assertTrue(mapping.isOutputOverridden(outRecord0, outRecord0.getField("field2")));
    	assertTrue(mapping.isOutputOverridden(outRecord0, outRecord0.getField("field1")));
    	assertFalse(mapping.isOutputOverridden(outRecord0, outRecord0.getField("field4")));
    	assertTrue(mapping.isOutputOverridden(outRecord1, outRecord1.getField("field1")));
    	assertTrue(mapping.isOutputOverridden(outRecord1, outRecord1.getField("field3")));
    	assertFalse(mapping.isOutputOverridden(outRecord1, outRecord1.getField("field4")));

    	assertEquals("neco1", mapping.getOutputRecord("secondOut").getField(0).getValue().toString());
    	assertNull(mapping.getOutputRecord("secondOut").getField(1).getValue());
    	assertNull(mapping.getOutputRecord("secondOut").getField(2).getValue());

    	assertNull(mapping.getOutputRecord("firstOut").getField(0).getValue());
    	assertEquals("neco2", mapping.getOutputRecord("firstOut").getField(1).getValue().toString());
    	assertNull(mapping.getOutputRecord("firstOut").getField(2).getValue());

    	mapping.postExecute();
    }
    
}
