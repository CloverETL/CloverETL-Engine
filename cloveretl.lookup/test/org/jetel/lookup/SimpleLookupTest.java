/**
 * 
 */
package org.jetel.lookup;

import java.io.FileInputStream;
import java.util.Random;

import junit.framework.AssertionFailedError;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.parser.DataParser;
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
	
	Lookup lookup, lookup2;
	DataRecord inRecord, inRecord2;
		
	@Override
	protected void setUp() throws Exception {
		initEngine();
		TypedProperties lookupProperties = new TypedProperties();
		lookupProperties.load(new FileInputStream("test/org/jetel/lookup/MySimpleLookup.cfg"));
		DataRecordMetadataXMLReaderWriter reader = new DataRecordMetadataXMLReaderWriter();
		DataRecordMetadata lookupMetadata = reader.read(new FileInputStream(
				lookupProperties.getProperty(LookupTable.XML_METADATA_ID)));
		SimpleLookupTable lookupTable = new SimpleLookupTable("myLookup", lookupMetadata, 
				lookupProperties.getProperty("key").split(";"), new DataParser());
		lookupTable.setData(lookupProperties.getProperty("data"));
		lookupTable.setKeyDuplicates(true);
		lookupTable.init();
		
		DataRecordMetadata meta = new DataRecordMetadata("input");
		meta.addField(new DataFieldMetadata("integer_field", DataFieldMetadata.INTEGER_FIELD, null));
		RecordKey key = new RecordKey(new int[]{0}, meta);
		inRecord = new DataRecord(meta);
		inRecord.init();
		lookup = lookupTable.createLookup(key, inRecord);

		RecordKey key2 = new RecordKey(new int[]{0}, meta);
		inRecord2 = new DataRecord(meta);
		inRecord2.init();
		lookup2 = lookupTable.createLookup(key2, inRecord2);
		
	}

	public void testLookup() throws Exception {
		LookingUp l1 = new LookingUp(lookup, inRecord);
		l1.run();
		LookingUp l2 = new LookingUp(lookup2, inRecord2);
		l2.run();
		l1.join();
		l2.join();
	}
	
}

class LookingUp extends Thread {

	private final static int[] id = {11, 12, 17, 18, 19};
	private final static String[] name = {"Andrew", "Nancy", "Robert", "Laura", "Anne"};
	private Lookup lookup;
	private Random r;
	private DataRecord inRecord;
	
	private final static int ITERATION_NUMBER = 100;
	
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
				if (first && !result.getField(2).equals(name[number])) throw new AssertionFailedError(
						"expected: " + name[number] + ", actual: " + result.getField(2));
				if (!first && !result.getField(2).equals(name[number].toLowerCase())) throw new AssertionFailedError(
						"expected: " + name[number].toLowerCase() + ", actual: " + result.getField(2));
				first = false;
			}
		}
	}
}