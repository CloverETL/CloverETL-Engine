/**
 * 
 */
package org.jetel.lookup;

import java.io.FileInputStream;
import java.util.Random;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.parser.DataParser;
import org.jetel.data.parser.TextParserConfiguration;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.test.CloverTestCase;
import org.jetel.util.primitive.TypedProperties;

/**
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Nov 7, 2008
 */
public class SimpleLookupTest extends CloverTestCase {
	
	Lookup lookup, lookup2, lookup3;
	DataRecord inRecord, inRecord2, inRecord3;
		
	@Override
	protected void setUp() throws Exception {
		initEngine();
		TypedProperties lookupProperties = new TypedProperties();
		lookupProperties.load(new FileInputStream("test/org/jetel/lookup/MySimpleLookup.cfg"));
		DataRecordMetadataXMLReaderWriter reader = new DataRecordMetadataXMLReaderWriter();
		DataRecordMetadata lookupMetadata = reader.read(new FileInputStream(
				lookupProperties.getProperty(LookupTable.XML_METADATA_ID)));
		SimpleLookupTable lookupTable = new SimpleLookupTable("myLookup", lookupMetadata, 
				lookupProperties.getProperty("key").split(";"), new DataParser(new TextParserConfiguration(lookupMetadata)));
		lookupTable.setData(lookupProperties.getProperty("data"));
		lookupTable.setKeyDuplicates(true);
		lookupTable.init();
		lookupTable.preExecute();
		
		DataRecordMetadata meta = new DataRecordMetadata("input");
		meta.addField(new DataFieldMetadata("integer_field", DataFieldMetadata.INTEGER_FIELD, null));
		RecordKey key = new RecordKey(new int[]{0}, meta);
		inRecord = DataRecordFactory.newRecord(meta);
		inRecord.init();
		lookup = lookupTable.createLookup(key, inRecord);

		RecordKey key2 = new RecordKey(new int[]{0}, meta);
		inRecord2 = DataRecordFactory.newRecord(meta);
		inRecord2.init();
		lookup2 = lookupTable.createLookup(key2, inRecord2);
		
		RecordKey key3 = new RecordKey(new int[]{0}, meta);
		inRecord3 = DataRecordFactory.newRecord(meta);
		inRecord3.init();
		lookup3 = lookupTable.createLookup(key3, inRecord3);
	}

	public void testLookup() throws Exception {
		LookingUp l1 = new LookingUp(lookup, inRecord);
		LookingUp l2 = new LookingUp(lookup2, inRecord2);
		LookingUp l3 = new LookingUp(lookup3, inRecord3);
		l1.start();
		l2.start();
		l3.start();
		l1.join();
		l2.join();
		l3.join();
		
		assertTrue(l1.isFinishedOk());
		assertTrue(l2.isFinishedOk());
		assertTrue(l3.isFinishedOk());
	}
	
	private final static int[] id = {11, 12, 17, 18, 19,13,8333};
	private final static String[] name = {"Andrew", "Nancy", "Robert", "Laura", "Anne",null, null};
	class LookingUp extends Thread {

		private Lookup lookup;
		private Random r;
		private DataRecord inRecord;
		private boolean finishedOk = false;
		
		private final static int ITERATION_NUMBER = 10000;
		
		LookingUp(Lookup lookup, DataRecord inPut){
			this.lookup = lookup;
			this.inRecord = inPut;
			r = new Random();
		}
		
		@Override
		public void run() {
			int number;
			DataRecord result;
			for (int i = 0; i < ITERATION_NUMBER; i++){
				number = r.nextInt(id.length);
				inRecord.getField(0).setValue(id[number]);
				lookup.seek();
				boolean first = true;
				while (lookup.hasNext()) {
					result = lookup.next();
					if( first ){
						assertEquals(name[number], result.getField(2).getValue().toString());
					} else {
						assertEquals(name[number].toLowerCase(), result.getField(2).getValue().toString());
					}
					first = false;
				}
				if( name[number]!= null){
					assertFalse("no record found for "+id[number]+" (index number)", first);
					if( 11 == id[number]){
						assertEquals(1, lookup.getNumFound());
					} else {
						assertEquals(2, lookup.getNumFound());
					}
				} else {
					assertEquals(0, lookup.getNumFound());
				}
				
			}
			finishedOk = true;
		}
		
		public boolean isFinishedOk() {
			return finishedOk;
		}
	}}

