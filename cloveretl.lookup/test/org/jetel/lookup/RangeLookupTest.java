package org.jetel.lookup;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.RuleBasedCollator;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.RecordKey;
import org.jetel.data.StringDataField;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTableFactory;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.JExcelXLSDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.data.parser.XLSParser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

public class RangeLookupTest extends CloverTestCase {

	RangeLookupTable lookup, lookupNotOverlap;
	Lookup lookupResult, lookupNOResult;
	DataRecordMetadata lookupMetadata, metadata;
	DataRecord record;
	Random random = new Random();

	public void test_getDataRecord() throws IOException, ComponentNotReadyException {
		lookupMetadata = new DataRecordMetadata("lookupTest", DataRecordMetadata.DELIMITED_RECORD);
		lookupMetadata.addField(new DataFieldMetadata("name", DataFieldMetadata.STRING_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("start", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("end", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("start1", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("end1", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookup = (RangeLookupTable) LookupTableFactory.createLookupTable(null, "rangeLookup", new Object[] { "RangeLookup", lookupMetadata, new String[] { "start", "start1" }, new String[] { "end", "end1" }, null }, new Class[] { String.class, DataRecordMetadata.class, String[].class, String[].class, Parser.class });
		lookupNotOverlap = (RangeLookupTable) LookupTableFactory.createLookupTable(null, "rangeLookup", new Object[] { "RangeLookup", lookupMetadata, new String[] { "start", "start1" }, new String[] { "end", "end1" }, null }, new Class[] { String.class, DataRecordMetadata.class, String[].class, String[].class, Parser.class });
		lookup.init();
		lookupNotOverlap.init();
		lookup.preExecute();
		lookupNotOverlap.preExecute();
		record = DataRecordFactory.newRecord(lookupMetadata);
		record.init();
		record.getField("name").setValue("10-20,100-200");
		record.getField("start").setValue(10);
		record.getField("end").setValue(20);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("15-20,100-200");
		record.getField("start").setValue(15);
		record.getField("end").setValue(20);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("20-25,100-200");
		record.getField("start").setValue(20);
		record.getField("end").setValue(25);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("20-30,0-100");
		record.getField("start").setValue(20);
		record.getField("end").setValue(30);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookup.put(record);
		record.getField("name").setValue("20-30,100-200");
		record.getField("start").setValue(20);
		record.getField("end").setValue(30);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("30-40,0-100");
		record.getField("start").setValue(30);
		record.getField("end").setValue(40);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookup.put(record);
		record.getField("name").setValue("30-40,100-200");
		record.getField("start").setValue(30);
		record.getField("end").setValue(40);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("0-10,0-100");
		record.getField("start").setValue(0);
		record.getField("end").setValue(10);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookup.put(record);
		record.getField("name").setValue("0-10,100-200");
		record.getField("start").setValue(0);
		record.getField("start1").setValue(100);
		record.getField("end").setValue(10);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("10-20,0-100");
		record.getField("start").setValue(10);
		record.getField("end").setValue(20);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookup.put(record);

		record.getField("name").setValue("10-20,100-200");
		record.getField("start").setValue(11);
		record.getField("end").setValue(20);
		record.getField("start1").setValue(101);
		record.getField("end1").setValue(200);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("20-30,0-100");
		record.getField("start").setValue(21);
		record.getField("end").setValue(30);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("20-30,100-200");
		record.getField("start").setValue(21);
		record.getField("end").setValue(30);
		record.getField("start1").setValue(101);
		record.getField("end1").setValue(200);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("30-40,0-100");
		record.getField("start").setValue(31);
		record.getField("end").setValue(40);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("30-40,100-200");
		record.getField("start").setValue(31);
		record.getField("end").setValue(40);
		record.getField("start1").setValue(101);
		record.getField("end1").setValue(200);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("0-10,0-100");
		record.getField("start").setValue(0);
		record.getField("end").setValue(10);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("0-10,100-200");
		record.getField("start").setValue(0);
		record.getField("start1").setValue(101);
		record.getField("end").setValue(10);
		record.getField("end1").setValue(200);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("10-20,0-100");
		record.getField("start").setValue(11);
		record.getField("end").setValue(20);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookupNotOverlap.put(record);

		metadata = new DataRecordMetadata("in", DataRecordMetadata.DELIMITED_RECORD);
		metadata.addField(new DataFieldMetadata("id", DataFieldMetadata.INTEGER_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("value", DataFieldMetadata.INTEGER_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("value1", DataFieldMetadata.INTEGER_FIELD, ";"));
		record = DataRecordFactory.newRecord(metadata);
		record.init();
		RecordKey key = new RecordKey(new int[] { 1, 2 }, metadata);
		lookupResult = lookup.createLookup(key, record);
		lookupNOResult = lookupNotOverlap.createLookup(key, record);

		DataRecord tmp;
		System.out.println("Lookup table:");
		for (Iterator<DataRecord> iter = lookup.iterator(); iter.hasNext();) {
			System.out.print(iter.next() + "\n");
		}

		System.out.println("LookupNotOverlap table:");
		for (Iterator<DataRecord> iter = lookupNotOverlap.iterator(); iter.hasNext();) {
			System.out.print(iter.next() + "\n");
		}

		for (int i = 0; i < 200; i++) {
			record.getField("id").setValue(i);
			record.getField("value").setValue(random.nextInt(41));
			record.getField("value1").setValue(random.nextInt(201));
			System.out.println("Record " + i + ":\n" + record);
			lookupResult.seek();
			lookupNOResult.seek();
			System.out.println("Num found: " + lookupResult.getNumFound());
			System.out.println("Num found: " + lookupNOResult.getNumFound());
			while (lookupResult.hasNext()) {
				tmp = lookupResult.next();
				System.out.println("Next:\n" + tmp);
				assertTrue((Integer) record.getField("value").getValue() >= (Integer) tmp.getField("start").getValue());
				assertTrue((Integer) record.getField("value").getValue() <= (Integer) tmp.getField("end").getValue());
				assertTrue((Integer) record.getField("value1").getValue() >= (Integer) tmp.getField("start1").getValue());
				assertTrue((Integer) record.getField("value1").getValue() <= (Integer) tmp.getField("end1").getValue());
				if (lookupNOResult.hasNext()) {
					System.out.println("Next1:\n" + lookupNOResult.next());
				}
			}
		}
		lookup.postExecute();
		lookupNotOverlap.postExecute();
	}

	public void test_getDataRecord2() throws IOException, ComponentNotReadyException {
		lookupMetadata = new DataRecordMetadata("lookupTest", DataRecordMetadata.DELIMITED_RECORD);
		lookupMetadata.addField(new DataFieldMetadata("name", DataFieldMetadata.STRING_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("start", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("end", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("start1", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("end1", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookup = (RangeLookupTable) LookupTableFactory.createLookupTable(null, "rangeLookup", new Object[] { "RangeLookup", lookupMetadata, new String[] { "start1", "start" }, new String[] { "end", "end1" }, null }, new Class[] { String.class, DataRecordMetadata.class, String[].class, String[].class, Parser.class });
		lookupNotOverlap = (RangeLookupTable) LookupTableFactory.createLookupTable(null, "rangeLookup", new Object[] { "RangeLookup", lookupMetadata, new String[] { "start1", "start" }, new String[] { "end", "end1" }, null }, new Class[] { String.class, DataRecordMetadata.class, String[].class, String[].class, Parser.class });
		lookup.init();
		lookupNotOverlap.init();
		lookup.preExecute();
		lookupNotOverlap.preExecute();
		record = DataRecordFactory.newRecord(lookupMetadata);
		record.init();
		record.getField("name").setValue("10-20,100-200");
		record.getField("start").setValue(10);
		record.getField("end").setValue(20);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("15-20,100-200");
		record.getField("start").setValue(15);
		record.getField("end").setValue(20);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("20-25,100-200");
		record.getField("start").setValue(20);
		record.getField("end").setValue(25);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("20-30,0-100");
		record.getField("start").setValue(20);
		record.getField("end").setValue(30);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookup.put(record);
		record.getField("name").setValue("20-30,100-200");
		record.getField("start").setValue(20);
		record.getField("end").setValue(30);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("30-40,0-100");
		record.getField("start").setValue(30);
		record.getField("end").setValue(40);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookup.put(record);
		record.getField("name").setValue("30-40,100-200");
		record.getField("start").setValue(30);
		record.getField("end").setValue(40);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("0-10,0-100");
		record.getField("start").setValue(0);
		record.getField("end").setValue(10);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookup.put(record);
		record.getField("name").setValue("0-10,100-200");
		record.getField("start").setValue(0);
		record.getField("start1").setValue(100);
		record.getField("end").setValue(10);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("10-20,0-100");
		record.getField("start").setValue(10);
		record.getField("end").setValue(20);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookup.put(record);

		record.getField("name").setValue("10-20,100-200");
		record.getField("start").setValue(11);
		record.getField("end").setValue(20);
		record.getField("start1").setValue(101);
		record.getField("end1").setValue(200);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("20-30,0-100");
		record.getField("start").setValue(21);
		record.getField("end").setValue(30);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("20-30,100-200");
		record.getField("start").setValue(21);
		record.getField("end").setValue(30);
		record.getField("start1").setValue(101);
		record.getField("end1").setValue(200);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("30-40,0-100");
		record.getField("start").setValue(31);
		record.getField("end").setValue(40);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("30-40,100-200");
		record.getField("start").setValue(31);
		record.getField("end").setValue(40);
		record.getField("start1").setValue(101);
		record.getField("end1").setValue(200);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("0-10,0-100");
		record.getField("start").setValue(0);
		record.getField("end").setValue(10);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("0-10,100-200");
		record.getField("start").setValue(0);
		record.getField("start1").setValue(101);
		record.getField("end").setValue(10);
		record.getField("end1").setValue(200);
		lookupNotOverlap.put(record);
		record.getField("name").setValue("10-20,0-100");
		record.getField("start").setValue(11);
		record.getField("end").setValue(20);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookupNotOverlap.put(record);

		metadata = new DataRecordMetadata("in", DataRecordMetadata.DELIMITED_RECORD);
		metadata.addField(new DataFieldMetadata("id", DataFieldMetadata.INTEGER_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("value", DataFieldMetadata.INTEGER_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("value1", DataFieldMetadata.INTEGER_FIELD, ";"));
		record = DataRecordFactory.newRecord(metadata);
		record.init();
		RecordKey key = new RecordKey(new int[] { 1, 2 }, metadata);
		lookupResult = lookup.createLookup(key, record);
		lookupNOResult = lookupNotOverlap.createLookup(key, record);

		DataRecord tmp, tmp1;
		System.out.println("Lookup table:");
		for (Iterator<DataRecord> iter = lookup.iterator(); iter.hasNext();) {
			System.out.print(iter.next() + "\n");
		}

		System.out.println("LookupNotOverlap table:");
		for (Iterator<DataRecord> iter = lookupNotOverlap.iterator(); iter.hasNext();) {
			System.out.print(iter.next() + "\n");
		}

		for (int i = 0; i < 200; i++) {
			record.getField("id").setValue(i);
			record.getField("value").setValue(random.nextInt(41));
			record.getField("value1").setValue(random.nextInt(201));
			System.out.println("Record " + i + ":\n" + record);
			lookupResult.seek();
			lookupNOResult.seek();
			System.out.println("Num found: " + lookupResult.getNumFound());
			System.out.println("Num found: " + lookupNOResult.getNumFound());
			do {
				tmp = lookupResult.hasNext() ? lookupResult.next() : null;
				if (tmp != null) {
					System.out.println("Next:\n" + tmp);
					assertTrue((Integer) record.getField("value").getValue() >= (Integer) tmp.getField("start1").getValue());
					assertTrue((Integer) record.getField("value").getValue() <= (Integer) tmp.getField("end").getValue());
					assertTrue((Integer) record.getField("value1").getValue() >= (Integer) tmp.getField("start").getValue());
					assertTrue((Integer) record.getField("value1").getValue() <= (Integer) tmp.getField("end1").getValue());
				}
				tmp1 = lookupNOResult.hasNext() ? lookupNOResult.next() : null;
				if (tmp1 != null) {
					System.out.println("Next1:\n" + tmp1);
					assertTrue((Integer) record.getField("value").getValue() >= (Integer) tmp1.getField("start1").getValue());
					assertTrue((Integer) record.getField("value").getValue() <= (Integer) tmp1.getField("end").getValue());
					assertTrue((Integer) record.getField("value1").getValue() >= (Integer) tmp1.getField("start").getValue());
					assertTrue((Integer) record.getField("value1").getValue() <= (Integer) tmp1.getField("end1").getValue());
				}
			} while (tmp != null || tmp1 != null);
		}
		lookup.postExecute();
		lookupNotOverlap.postExecute();
	}

	public void test_largeData() throws IOException, ComponentNotReadyException, JetelException {
		lookupMetadata = new DataRecordMetadata("lookupTest", DataRecordMetadata.DELIMITED_RECORD);
		lookupMetadata.addField(new DataFieldMetadata("name", DataFieldMetadata.STRING_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("s1", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("e1", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("s2", DataFieldMetadata.NUMERIC_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("e2", DataFieldMetadata.NUMERIC_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("s3", DataFieldMetadata.STRING_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("e3", DataFieldMetadata.STRING_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("s4", DataFieldMetadata.LONG_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("e4", DataFieldMetadata.LONG_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("s5", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("e5", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("s6", DataFieldMetadata.NUMERIC_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("e6", DataFieldMetadata.NUMERIC_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("s7", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("e7", DataFieldMetadata.INTEGER_FIELD, ";"));
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("s8", DataFieldMetadata.DATE_FIELD, ";");
		fieldMetadata.setFormatStr("dd.MM.yy");
		lookupMetadata.addField(fieldMetadata);
		fieldMetadata = new DataFieldMetadata("e8", DataFieldMetadata.DATE_FIELD, ";");
		fieldMetadata.setFormatStr("dd.MM.yy");
		lookupMetadata.addField(fieldMetadata);

		XLSParser parser = new JExcelXLSDataParser(lookupMetadata);
		parser.setSheetNumber("0");
		parser.init();
		parser.setDataSource(new FileInputStream("../cloveretl.engine/data" + File.separatorChar + "rangeLookup.dat.xls"));

		lookup = (RangeLookupTable) LookupTableFactory.createLookupTable(null, "rangeLookup", new Object[] { "RangeLookup", lookupMetadata, new String[] { "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8" }, new String[] { "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8" }, parser }, new Class[] { String.class, DataRecordMetadata.class, String[].class, String[].class, Parser.class });
		lookup.init();
		lookup.preExecute();
		
		DataRecord tmp = DataRecordFactory.newRecord(lookupMetadata);
		DataRecord tmp1 = DataRecordFactory.newRecord(lookupMetadata);
		tmp.init();
		tmp1.init();

		Iterator<DataRecord> iter = lookup.iterator();
		tmp1 = iter.next();
		int count = 1;

		for (; iter.hasNext();) {
			System.out.println("Record nr " + count++ + "\n" + tmp1);
			tmp = tmp1.duplicate();
			tmp1 = (DataRecord) iter.next();
			assertTrue(checkOrder(tmp, tmp1));
		}

		metadata = new DataRecordMetadata("in", DataRecordMetadata.DELIMITED_RECORD);
		metadata.addField(new DataFieldMetadata("id", DataFieldMetadata.INTEGER_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("value1", DataFieldMetadata.INTEGER_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("value2", DataFieldMetadata.NUMERIC_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("value3", DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("value4", DataFieldMetadata.LONG_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("value5", DataFieldMetadata.INTEGER_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("value6", DataFieldMetadata.NUMERIC_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("value7", DataFieldMetadata.INTEGER_FIELD, ";"));
		fieldMetadata = new DataFieldMetadata("value8", DataFieldMetadata.DATE_FIELD, ";");
		fieldMetadata.setFormatStr("dd.MM.yy");
		metadata.addField(fieldMetadata);

		record = DataRecordFactory.newRecord(metadata);
		record.init();
		RecordKey key = new RecordKey(new int[] { 1, 2, 3, 4, 5, 6, 7, 8 }, metadata);
		lookupResult = lookup.createLookup(key, record);

		DelimitedDataParser dataParser = new DelimitedDataParser(metadata);
		dataParser.init();
		dataParser.setDataSource(new FileInputStream("../cloveretl.engine/data" + File.separatorChar + "data.dat"));

		for (int i = 0; i < 500; i++) {
			record = dataParser.getNext(record);
			System.out.println("Input record " + i + ":\n" + record);
			lookupResult.seek();
			System.out.println("Num found: " + lookupResult.getNumFound());
			while (lookupResult.hasNext()) {
				tmp = lookupResult.next();
				System.out.println("From lookup table:\n" + tmp);
				assertTrue((Integer) record.getField("value1").getValue() >= (Integer) tmp.getField("s1").getValue());
				assertTrue((Integer) record.getField("value1").getValue() <= (Integer) tmp.getField("e1").getValue());
				assertTrue((Double) record.getField("value2").getValue() >= (Double) tmp.getField("s2").getValue());
				assertTrue((Double) record.getField("value2").getValue() <= (Double) tmp.getField("e2").getValue());
				assertTrue((record.getField("value3").getValue().toString()).compareTo(tmp.getField("s3").getValue().toString()) >= 0);
				assertTrue((record.getField("value3").getValue().toString()).compareTo(tmp.getField("e3").getValue().toString()) <= 0);
				assertTrue((Long) record.getField("value4").getValue() >= (Long) tmp.getField("s4").getValue());
				assertTrue((Long) record.getField("value4").getValue() <= (Long) tmp.getField("e4").getValue());
				assertTrue((Integer) record.getField("value5").getValue() >= (Integer) tmp.getField("s5").getValue());
				assertTrue((Integer) record.getField("value5").getValue() <= (Integer) tmp.getField("e5").getValue());
				assertTrue((Double) record.getField("value6").getValue() >= (Double) tmp.getField("s6").getValue());
				assertTrue((Double) record.getField("value6").getValue() <= (Double) tmp.getField("e6").getValue());
				assertTrue((Integer) record.getField("value7").getValue() >= (Integer) tmp.getField("s7").getValue());
				assertTrue((Integer) record.getField("value7").getValue() <= (Integer) tmp.getField("e7").getValue());
				assertTrue(((Date) record.getField("value8").getValue()).compareTo((Date) tmp.getField("s8").getValue()) >= 0);
				assertTrue(((Date) record.getField("value8").getValue()).compareTo((Date) tmp.getField("e8").getValue()) <= 0);
			}
		}
		lookup.postExecute();
	}

	public void test_IncludeExclude() throws ComponentNotReadyException {
		lookupMetadata = new DataRecordMetadata("lookupTest", DataRecordMetadata.DELIMITED_RECORD);
		lookupMetadata.addField(new DataFieldMetadata("name", DataFieldMetadata.STRING_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("start", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("end", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("start1", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("end1", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookup = (RangeLookupTable) LookupTableFactory.createLookupTable(null, "rangeLookup", new Object[] { "RangeLookup", lookupMetadata, new String[] { "start", "start1" }, new String[] { "end", "end1" }, null, new boolean[] { true, true }, new boolean[] { true, true } }, new Class[] { String.class, DataRecordMetadata.class, String[].class, String[].class, Parser.class, boolean[].class, boolean[].class });
		lookup.init();
		lookup.preExecute();
		record = DataRecordFactory.newRecord(lookupMetadata);
		record.init();
		record.getField("name").setValue("10-20,100-200");
		record.getField("start").setValue(10);
		record.getField("end").setValue(20);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("15-20,100-200");
		record.getField("start").setValue(15);
		record.getField("end").setValue(20);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("20-25,100-200");
		record.getField("start").setValue(20);
		record.getField("end").setValue(25);
		record.getField("start1").setValue(100);
		record.getField("end1").setValue(200);
		lookup.put(record);
		record.getField("name").setValue("20-30,0-100");
		record.getField("start").setValue(20);
		record.getField("end").setValue(30);
		record.getField("start1").setValue(0);
		record.getField("end1").setValue(100);
		lookup.put(record);

		for (Iterator<DataRecord> iter = lookup.iterator(); iter.hasNext();) {
			System.out.print(iter.next());

		}

		metadata = new DataRecordMetadata("in", DataRecordMetadata.DELIMITED_RECORD);
		metadata.addField(new DataFieldMetadata("id", DataFieldMetadata.INTEGER_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("value", DataFieldMetadata.INTEGER_FIELD, ";"));
		record = DataRecordFactory.newRecord(metadata);
		record.init();
		RecordKey key = new RecordKey(new int[]{0,1}, metadata);
		
		Integer[] keys = new Integer[2];

		keys[0] = 20;
		keys[1] = 120;
		record.getField(0).setValue(keys[0]);
		record.getField(1).setValue(keys[1]);

		lookupResult = lookup.createLookup(key, record);
		lookupResult.seek();
		assertEquals(3, lookupResult.getNumFound());

		((RangeLookupTable) lookup).setStartInclude(new boolean[] { false, true });

		lookupResult = lookup.createLookup(key, record);
		lookupResult.seek();
		assertEquals(2, lookupResult.getNumFound());

		((RangeLookupTable) lookup).setEndInclude(new boolean[] { false, false });

		lookupResult = lookup.createLookup(key, record);
		lookupResult.seek();
		assertEquals(0, lookupResult.getNumFound());

		keys[0] = 17;
		keys[1] = 100;
		record.getField(0).setValue(keys[0]);
		record.getField(1).setValue(keys[1]);

		lookupResult = lookup.createLookup(key, record);
		lookupResult.seek();
		assertEquals(2, lookupResult.getNumFound());
		lookup.postExecute();
	}

	public void test_Strings() throws ComponentNotReadyException {
		lookupMetadata = new DataRecordMetadata("lookupTest", DataRecordMetadata.DELIMITED_RECORD);
		lookupMetadata.addField(new DataFieldMetadata("name", DataFieldMetadata.STRING_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("start", DataFieldMetadata.STRING_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("end", DataFieldMetadata.STRING_FIELD, ";"));
		RuleBasedCollator collator = (RuleBasedCollator) RuleBasedCollator.getInstance();
		lookup = (RangeLookupTable) LookupTableFactory.createLookupTable(null, "rangeLookup", new Object[] { "RangeLookup", lookupMetadata, new String[] { "start" }, new String[] { "end" }, null, collator }, new Class[] { String.class, DataRecordMetadata.class, String[].class, String[].class, Parser.class, RuleBasedCollator.class });
		lookup.init();
		lookup.preExecute();
		record = DataRecordFactory.newRecord(lookupMetadata);
		record.init();
		record.getField("name").setValue("p1");
		record.getField("start").setValue("aaaaa");
		record.getField("end").setValue("baaaa");
		lookup.put(record);
		record.getField("name").setValue("p2");
		record.getField("start").setValue("baaaa");
		record.getField("end").setValue("Baaaa");
		lookup.put(record);
		record.getField("name").setValue("p3");
		record.getField("start").setValue("Baaaa");
		record.getField("end").setValue("KZZZZ");
		lookup.put(record);
		record.getField("name").setValue("p4");
		record.getField("start").setValue("laaaa");
		record.getField("end").setValue("XZZZZ");
		lookup.put(record);
		record.getField("name").setValue("p5");
		record.getField("start").setValue("yaaaa");
		record.getField("end").setValue("ZZZZZZ");
		lookup.put(record);
		record.getField("name").setValue("p6");
		record.getField("start").setValue("faaaa");
		record.getField("end").setValue("kguef");
		lookup.put(record);
		record.getField("name").setValue("p7");
		record.getField("start").setValue("waaaa");
		record.getField("end").setValue("ZZZZa");
		lookup.put(record);
		record.getField("name").setValue("p8");
		record.getField("start").setValue("ecykr");
		record.getField("end").setValue("htedyu");
		lookup.put(record);

		DataRecord tmp;
		for (Iterator<DataRecord> iter = lookup.iterator(); iter.hasNext();) {
			System.out.print(iter.next());

		}

		String value;

		metadata = new DataRecordMetadata("in", DataRecordMetadata.DELIMITED_RECORD);
		metadata.addField(new DataFieldMetadata("value", DataFieldMetadata.STRING_FIELD, ";"));
		record = DataRecordFactory.newRecord(metadata);
		record.init();
		RecordKey key = new RecordKey(new int[]{0}, metadata);
		lookupResult = lookup.createLookup(key, record);

		for (int i = 0; i < 200; i++) {
			value = randomString(5, 5);
			record.getField(0).setValue(value);
			lookupResult.seek();
			System.out.println(i + " - Value: " + value + "\n");
			while (lookupResult.hasNext()) {
				tmp = lookupResult.next();
				System.out.println("Next:\n" + tmp);
				assertTrue(((StringDataField) tmp.getField("start")).compareTo(value, collator) <= 0);
				assertTrue(((StringDataField) tmp.getField("end")).compareTo(value, collator) > 0);
			}
		}

		lookup.postExecute();
	}

	public void test_nulls() throws ComponentNotReadyException {
		lookupMetadata = new DataRecordMetadata("lookupTest", DataRecordMetadata.DELIMITED_RECORD);
		lookupMetadata.addField(new DataFieldMetadata("name", DataFieldMetadata.STRING_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("start", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookupMetadata.addField(new DataFieldMetadata("end", DataFieldMetadata.INTEGER_FIELD, ";"));
		lookup = (RangeLookupTable) LookupTableFactory.createLookupTable(null, "rangeLookup", new Object[] { "RangeLookup", lookupMetadata, new String[] { "start" }, new String[] { "end" }, null }, new Class[] { String.class, DataRecordMetadata.class, String[].class, String[].class, Parser.class });
		lookup.init();
		lookup.preExecute();
		record = DataRecordFactory.newRecord(lookupMetadata);
		record.init();
		record.getField("name").setValue("start is null");
		record.getField("start").setValue(null);
		record.getField("end").setValue(20);
		lookup.put(record);
		record.getField("name").setValue("end is null");
		record.getField("start").setValue(20);
		record.getField("end").setValue(null);
		lookup.put(record);
		record.getField("name").setValue("both are null");
		record.getField("start").setValue(null);
		record.getField("end").setValue(null);
		lookup.put(record);

		for (Iterator<DataRecord> iter = lookup.iterator(); iter.hasNext();) {
			System.out.print(iter.next());

		}
		
		metadata = new DataRecordMetadata("in", DataRecordMetadata.DELIMITED_RECORD);
		metadata.addField(new DataFieldMetadata("id", DataFieldMetadata.INTEGER_FIELD, ";"));
		record = DataRecordFactory.newRecord(metadata);
		record.init();
		RecordKey key = new RecordKey(new int[]{0}, metadata);
		lookupResult = lookup.createLookup(key, record);

		record.getField(0).setValue(10);
		System.out.println();
		System.out.println("Looking for 10:");
		lookupResult.seek();
		System.out.println(lookupResult.next());
		System.out.println(lookupResult.next());
		assertEquals(2, lookupResult.getNumFound());

		record.getField(0).setValue(25);
		System.out.println();
		System.out.println("Looking for 25:");
		lookupResult.seek();
		System.out.println(lookupResult.next());
		System.out.println(lookupResult.next());
		assertEquals(2, lookupResult.getNumFound());

		record.getField(0).setValue(20);
		System.out.println();
		System.out.println("Looking for 20:");
		lookupResult.seek();
		System.out.println(lookupResult.next());
		System.out.println(lookupResult.next());
		assertEquals(2, lookupResult.getNumFound());

		((RangeLookupTable) lookup).setEndInclude(true);
		lookupResult = lookup.createLookup(key, record);
		record.getField(0).setValue(10);
		System.out.println();
		System.out.println("Looking for 10:");
		lookupResult.seek();
		System.out.println(lookupResult.next());
		System.out.println(lookupResult.next());
		assertEquals(2, lookupResult.getNumFound());

		record.getField(0).setValue(25);
		System.out.println();
		System.out.println("Looking for 25:");
		lookupResult.seek();
		System.out.println(lookupResult.next());
		System.out.println(lookupResult.next());
		assertEquals(2, lookupResult.getNumFound());

		record.getField(0).setValue(20);
		System.out.println();
		System.out.println("Looking for 20:");
		lookupResult.seek();
		System.out.println(lookupResult.next());
		System.out.println(lookupResult.next());
		System.out.println(lookupResult.next());
		assertEquals(3, lookupResult.getNumFound());
		lookup.postExecute();
	}

	private boolean checkOrder(DataRecord previous, DataRecord following) {
		int startComparison;
		int endComparison;
		for (int i = 1; i < previous.getNumFields() - 1; i += 2) {
			startComparison = previous.getField(i).compareTo(following.getField(i));
			endComparison = previous.getField(i + 1).compareTo(following.getField(i + 1));
			if (endComparison < 0) {
				return true;
			}
			if (endComparison == 0 && startComparison >= 0) {
				return true;
			}
		}
		return false;
	}

	private String randomString(int minLenght, int maxLenght) {
		StringBuilder result;
		if (maxLenght != minLenght) {
			result = new StringBuilder(random.nextInt(maxLenght - minLenght + 1) + minLenght);
		} else {// minLenght == maxLenght
			result = new StringBuilder(minLenght);
		}
		for (int i = 0; i < result.capacity(); i++) {
			result.append((char) (random.nextInt('z' - 'a' + 1) + 'a'));
		}
		return result.toString();
	}

}
