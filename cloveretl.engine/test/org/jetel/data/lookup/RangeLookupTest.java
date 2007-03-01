package org.jetel.data.lookup;


import java.io.IOException;
import java.util.Random;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.parser.Parser;
import org.jetel.main.runGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

import junit.framework.TestCase;

public class RangeLookupTest extends TestCase {

    LookupTable lookup,lookupNotOverlap;
    DataRecordMetadata lookupMetadata, metadata;
    DataRecord record;
    Random random = new Random();
    
    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        runGraph.initEngine(null, null);
        lookupMetadata = new DataRecordMetadata("lookupTest",DataRecordMetadata.DELIMITED_RECORD);
        lookupMetadata.addField(new DataFieldMetadata("name",DataFieldMetadata.STRING_FIELD,";"));
        lookupMetadata.addField(new DataFieldMetadata("start",DataFieldMetadata.INTEGER_FIELD,";"));
        lookupMetadata.addField(new DataFieldMetadata("end",DataFieldMetadata.INTEGER_FIELD,";"));
        lookupMetadata.addField(new DataFieldMetadata("start1",DataFieldMetadata.INTEGER_FIELD,";"));
        lookupMetadata.addField(new DataFieldMetadata("end1",DataFieldMetadata.INTEGER_FIELD,";"));
        lookup = LookupTableFactory.createLookupTable(null, "rangeLookup", 
        		new Object[]{"RangeLookup",lookupMetadata,null}, 
        		new Class[]{String.class,DataRecordMetadata.class,Parser.class});
        lookupNotOverlap = LookupTableFactory.createLookupTable(null, "rangeLookup", 
        		new Object[]{"RangeLookup",lookupMetadata,null,false}, 
        		new Class[]{String.class,DataRecordMetadata.class,Parser.class,boolean.class});
        lookup.init();
        lookupNotOverlap.init();
    	record = new DataRecord(lookupMetadata);
    	record.init();
      	record.getField("name").setValue("10-20,100-200");
    	record.getField("start").setValue(10);
       	record.getField("end").setValue(20);
      	record.getField("start1").setValue(100);
      	record.getField("end1").setValue(200);
       	lookup.put(record, record.duplicate());
    	record.getField("name").setValue("20-30,0-100");
    	record.getField("start").setValue(20);
       	record.getField("end").setValue(30);
      	record.getField("start1").setValue(0);
      	record.getField("end1").setValue(100);
       	lookup.put(record, record.duplicate());
    	record.getField("name").setValue("20-30,100-200");
    	record.getField("start").setValue(20);
       	record.getField("end").setValue(30);
      	record.getField("start1").setValue(100);
      	record.getField("end1").setValue(200);
       	lookup.put(record, record.duplicate());
    	record.getField("name").setValue("30-40,0-100");
    	record.getField("start").setValue(30);
       	record.getField("end").setValue(40);
      	record.getField("start1").setValue(0);
      	record.getField("end1").setValue(100);
       	lookup.put(record, record.duplicate());
    	record.getField("name").setValue("30-40,100-200");
    	record.getField("start").setValue(30);
       	record.getField("end").setValue(40);
      	record.getField("start1").setValue(100);
      	record.getField("end1").setValue(200);
       	lookup.put(record, record.duplicate());
    	record.getField("name").setValue("0-10,0-100");
    	record.getField("start").setValue(0);
      	record.getField("end").setValue(10);
      	record.getField("start1").setValue(0);
      	record.getField("end1").setValue(100);
       	lookup.put(record, record.duplicate());
       	record.getField("name").setValue("0-10,100-200");
    	record.getField("start").setValue(0);
       	record.getField("start1").setValue(100);
      	record.getField("end").setValue(10);
      	record.getField("end1").setValue(200);
      	lookup.put(record, record.duplicate());
      	record.getField("name").setValue("10-20,0-100");
    	record.getField("start").setValue(10);
       	record.getField("end").setValue(20);
      	record.getField("start1").setValue(0);
      	record.getField("end1").setValue(100);
       	lookup.put(record, record.duplicate());

      	record.getField("name").setValue("10-20,100-200");
    	record.getField("start").setValue(11);
       	record.getField("end").setValue(20);
      	record.getField("start1").setValue(101);
      	record.getField("end1").setValue(200);
       	lookupNotOverlap.put(record, record.duplicate());
    	record.getField("name").setValue("20-30,0-100");
    	record.getField("start").setValue(21);
       	record.getField("end").setValue(30);
      	record.getField("start1").setValue(0);
      	record.getField("end1").setValue(100);
       	lookupNotOverlap.put(record, record.duplicate());
    	record.getField("name").setValue("20-30,100-200");
    	record.getField("start").setValue(21);
       	record.getField("end").setValue(30);
      	record.getField("start1").setValue(101);
      	record.getField("end1").setValue(200);
       	lookupNotOverlap.put(record, record.duplicate());
    	record.getField("name").setValue("30-40,0-100");
    	record.getField("start").setValue(31);
       	record.getField("end").setValue(40);
      	record.getField("start1").setValue(0);
      	record.getField("end1").setValue(100);
       	lookupNotOverlap.put(record, record.duplicate());
    	record.getField("name").setValue("30-40,100-200");
    	record.getField("start").setValue(31);
       	record.getField("end").setValue(40);
      	record.getField("start1").setValue(101);
      	record.getField("end1").setValue(200);
       	lookupNotOverlap.put(record, record.duplicate());
    	record.getField("name").setValue("0-10,0-100");
    	record.getField("start").setValue(0);
      	record.getField("end").setValue(10);
      	record.getField("start1").setValue(0);
      	record.getField("end1").setValue(100);
       	lookupNotOverlap.put(record, record.duplicate());
       	record.getField("name").setValue("0-10,100-200");
    	record.getField("start").setValue(0);
       	record.getField("start1").setValue(101);
      	record.getField("end").setValue(10);
      	record.getField("end1").setValue(200);
      	lookupNotOverlap.put(record, record.duplicate());
      	record.getField("name").setValue("10-20,0-100");
    	record.getField("start").setValue(11);
       	record.getField("end").setValue(20);
      	record.getField("start1").setValue(0);
      	record.getField("end1").setValue(100);
       	lookupNotOverlap.put(record, record.duplicate());

       	metadata = new DataRecordMetadata("in",DataRecordMetadata.DELIMITED_RECORD);
       	metadata.addField(new DataFieldMetadata("id",DataFieldMetadata.INTEGER_FIELD,";"));
       	metadata.addField(new DataFieldMetadata("value",DataFieldMetadata.INTEGER_FIELD,";"));
       	metadata.addField(new DataFieldMetadata("value1",DataFieldMetadata.INTEGER_FIELD,";"));
      	record = new DataRecord(metadata);
    	record.init();
       	RecordKey key = new RecordKey(new int[]{1,2},metadata);
       	lookup.setLookupKey(key);
       	lookupNotOverlap.setLookupKey(key);
    }

    public void test_1() throws IOException{
    	DataRecord tmp,tmp1;
    	for (int i=0;i<500;i++){
    		record.getField("id").setValue(i);
    		record.getField("value").setValue(random.nextInt(41));
    		record.getField("value1").setValue(random.nextInt(201));
    		tmp = lookup.get(record);
    		tmp1 = lookupNotOverlap.get(record);
    		System.out.println("Input record " + i + ":\n" + record + "From lookup table:\n" + tmp);
    		assertTrue((Integer)record.getField("value").getValue() >= (Integer)tmp.getField("start").getValue());
    		assertTrue((Integer)record.getField("value").getValue() <= (Integer)tmp.getField("end").getValue());
    		assertTrue((Integer)record.getField("value1").getValue() >= (Integer)tmp.getField("start1").getValue());
    		assertTrue((Integer)record.getField("value1").getValue() <= (Integer)tmp.getField("end1").getValue());
    		System.out.println("From lookupNotOverlap table:\n" + tmp1);
    		assertEquals(tmp.getField("end"), tmp1.getField("end"));
    		assertEquals(tmp.getField("end1"), tmp1.getField("end1"));
//    		if ((Integer)record.getField("value").getValue()%10 == 0 || 
//    				(Integer)record.getField("value1").getValue()%100 == 0 ){
//    			System.in.read();
//    		}
    		tmp = lookup.getNext();
    		tmp1 = lookupNotOverlap.getNext();
    		while (tmp != null) {
    			System.out.println("Next:\n" + tmp);
        		assertTrue((Integer)record.getField("value").getValue() >= (Integer)tmp.getField("start").getValue());
        		assertTrue((Integer)record.getField("value").getValue() <= (Integer)tmp.getField("end").getValue());
        		assertTrue((Integer)record.getField("value1").getValue() >= (Integer)tmp.getField("start1").getValue());
        		assertTrue((Integer)record.getField("value1").getValue() <= (Integer)tmp.getField("end1").getValue());
    			System.out.println("Next1:\n" + tmp1);
     			tmp = lookup.getNext();
    		}
    	}
  }
}
