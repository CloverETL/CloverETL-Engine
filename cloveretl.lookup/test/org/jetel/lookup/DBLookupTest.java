package org.jetel.lookup;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Properties;

import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.SQLDataParser;
import org.jetel.connection.jdbc.specific.DBConnectionInstance;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

public class DBLookupTest extends CloverTestCase {

	Lookup lookup;
	DataRecord customer, employee = null;
	SQLDataParser parser;
	DBConnectionInstance aDBConnection;
	private DBLookupTable lookupTable;
	private RecordKey recordKey;

	@Override
	protected void setUp() throws ComponentNotReadyException, FileNotFoundException, SQLException, JetelException {
		initEngine();
		
	    
		DBConnection conn = new DBConnection("conn", "../cloveretl.connection/test/org/jetel/connection/koule_postgre.cfg");
		conn.init();
		aDBConnection = conn.getConnection(conn.getId());

		Properties p = new Properties();
		p.put("sqlQuery", "select * from customer");
		DataRecordMetadata customerMetadata = conn.createMetadata(p);
		customer = new DataRecord(customerMetadata);
		customer.init();
		parser = new SQLDataParser(customerMetadata, "select * from customer");
		parser.init();
		parser.setDataSource(aDBConnection);

		lookupTable = new DBLookupTable("MyLookup", conn, null, "select * from employee where last_name=?", 0);
		lookupTable.init();
		recordKey = new RecordKey(new String[] { "lname" }, customerMetadata);
		recordKey.init();
	}

	@Override
	protected void tearDown() throws Exception {
		parser.close();
		lookupTable.free();
	}

	public void testThreads() throws Exception {
		lookupTable.preExecute();
		lookup = lookupTable.createLookup(recordKey, customer);
		RecordKey key = new RecordKey(recordKey.getKeyFields(), customer.getMetadata());
		DataRecord inRecord = new DataRecord(customer.getMetadata());
		inRecord.init();
		Lookup lookup2 = lookupTable.createLookup(key, inRecord);
		DataRecord record;
		while ((parser.getNext(customer)) != null) {
			inRecord.copyFrom(customer);
//			 System.out.println("Looking pair for :\n" + customer);
			lookup.seek();
			lookup2.seek();
			while (lookup.hasNext()) {
				employee = lookup.next();
				record = lookup2.next();
//				System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
				assertEquals(customer.getField("lname"), record.getField("last_name"));
			}
		}
		lookupTable.postExecute();
	}
	
	
	public void test1() throws JetelException, ComponentNotReadyException {
		lookupTable.preExecute();
		lookup = lookupTable.createLookup(recordKey, customer);
		long start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
//			 System.out.println("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee =  lookup.next();
//				System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		System.out.println("Without cashing:");
		System.out.println("Total number searched: " + ((DBLookup) lookup).getTotalNumber());
		System.out.println("From cache found: " + ((DBLookup) lookup).getCacheNumber());
		System.out.println("Timing: " + (System.currentTimeMillis() - start));
		lookupTable.postExecute();
//		lookupTable.free();

		lookupTable.preExecute();
		parser.setDataSource(aDBConnection);
		lookup = lookupTable.createLookup(recordKey, customer);
		start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
			// System.out.println("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee =  lookup.next();
				// System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		System.out.println("Without cashing:");
		System.out.println("Total number searched: " + ((DBLookup) lookup).getTotalNumber());
		System.out.println("From cache found: " + ((DBLookup) lookup).getCacheNumber());
		System.out.println("Timing: " + (System.currentTimeMillis() - start));
		lookupTable.postExecute();
//		lookupTable.free();

		lookupTable.preExecute();
		parser.setDataSource(aDBConnection);
		lookupTable.setNumCached(1000);
		lookupTable.setStoreNulls(false);
		lookupTable.init();
		lookup = lookupTable.createLookup(recordKey, customer);
		start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
//			 System.out.println("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee = lookup.next();
//				 System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		System.out.println("Cashing=1000, storeNulls=false:");
		System.out.println("Total number searched: " + ((DBLookup) lookup).getTotalNumber());
		System.out.println("From cache found: " + ((DBLookup) lookup).getCacheNumber());
		System.out.println("Timing: " + (System.currentTimeMillis() - start));
		lookupTable.postExecute();
//		lookupTable.free();

		lookupTable.preExecute();
		parser.setDataSource(aDBConnection);
		lookupTable.setNumCached(1000);
		lookupTable.setStoreNulls(true);
		lookupTable.init();
		lookup = lookupTable.createLookup(recordKey, customer);
		start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
//			 System.out.println("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee = lookup.next();
//				 System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		System.out.println("Cashing=1000, storeNulls=true:");
		System.out.println("Total number searched: " + ((DBLookup) lookup).getTotalNumber());
		System.out.println("From cache found: " + ((DBLookup) lookup).getCacheNumber());
		System.out.println("Timing: " + (System.currentTimeMillis() - start));
		lookupTable.postExecute();
//		lookupTable.free();

		lookupTable.preExecute();
		parser.setDataSource(aDBConnection);
		lookupTable.setNumCached(3000);
		lookupTable.setStoreNulls(false);
		lookupTable.init();
		lookup = lookupTable.createLookup(recordKey, customer);
		start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
			// System.out.println("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee = lookup.next();
				// System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		System.out.println("Cashing=3000, storeNulls=false:");
		System.out.println("Total number searched: " + ((DBLookup) lookup).getTotalNumber());
		System.out.println("From cache found: " + ((DBLookup) lookup).getCacheNumber());
		System.out.println("Timing: " + (System.currentTimeMillis() - start));
		lookupTable.postExecute();
//		lookupTable.free();

		lookupTable.preExecute();
		parser.setDataSource(aDBConnection);
		lookupTable.setNumCached(3000);
		lookupTable.setStoreNulls(true);
		lookupTable.init();
		lookup = lookupTable.createLookup(recordKey, customer);
		start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
			// System.out.println("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee = lookup.next();
				// System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		lookupTable.postExecute();
		System.out.println("Cashing=3000, storeNulls=true:");
		System.out.println("Total number searched: " + ((DBLookup) lookup).getTotalNumber());
		System.out.println("From cache found: " + ((DBLookup) lookup).getCacheNumber());
		System.out.println("Timing: " + (System.currentTimeMillis() - start));
	}

}
