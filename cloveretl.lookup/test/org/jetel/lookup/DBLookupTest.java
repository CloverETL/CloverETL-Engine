package org.jetel.lookup;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.SQLDataParser;
import org.jetel.connection.jdbc.specific.DBConnectionInstance;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

public class DBLookupTest extends CloverTestCase {

	private static final Logger log = Logger.getLogger(DBLookupTest.class);
	
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
		customer = DataRecordFactory.newRecord(customerMetadata);
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
		DataRecord inRecord = DataRecordFactory.newRecord(customer.getMetadata());
		inRecord.init();
		Lookup lookup2 = lookupTable.createLookup(key, inRecord);
		DataRecord record;
		long start = System.currentTimeMillis();
		int seeks = 0;
		while ((parser.getNext(customer)) != null) {
			inRecord.copyFrom(customer);
//			 log.info("Looking pair for :\n" + customer);
			lookup.seek();
			lookup2.seek();
			seeks += 2;
			while (lookup.hasNext()) {
				employee = lookup.next();
				record = lookup2.next();
//				log.info("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
				assertEquals(customer.getField("lname"), record.getField("last_name"));
			}
		}
		log.info("Processed " + seeks + " seeks in " + (System.currentTimeMillis() - start) + " ms");
		lookupTable.postExecute();
	}
	
	
	public void test1() throws JetelException, ComponentNotReadyException {
		lookupTable.preExecute();
		lookup = lookupTable.createLookup(recordKey, customer);
		long start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
			lookup.seek();
			while (lookup.hasNext()) {
				employee =  lookup.next();
//				log.info("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		log.info("Without caching:");
		log.info("Total number searched: " + ((DBLookup) lookup).getTotalNumber());
		log.info("From cache found: " + ((DBLookup) lookup).getCacheNumber());
		log.info("Timing: " + (System.currentTimeMillis() - start));
		lookupTable.postExecute();

		lookupTable.preExecute();
		parser.setDataSource(aDBConnection);
		lookup = lookupTable.createLookup(recordKey, customer);
		start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
			// log.info("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee =  lookup.next();
				// log.info("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		log.info("Without caching:");
		log.info("Total number searched: " + ((DBLookup) lookup).getTotalNumber());
		log.info("From cache found: " + ((DBLookup) lookup).getCacheNumber());
		log.info("Timing: " + (System.currentTimeMillis() - start));
		lookupTable.postExecute();

		lookupTable.preExecute();
		parser.setDataSource(aDBConnection);
		lookupTable.setNumCached(1000);
		lookupTable.setStoreNulls(false);
		lookupTable.init();
		lookup = lookupTable.createLookup(recordKey, customer);
		start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
//			 log.info("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee = lookup.next();
//				 log.info("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		log.info("caching=1000, storeNulls=false:");
		log.info("Total number searched: " + ((DBLookup) lookup).getTotalNumber());
		log.info("From cache found: " + ((DBLookup) lookup).getCacheNumber());
		log.info("Timing: " + (System.currentTimeMillis() - start));
		lookupTable.postExecute();

		lookupTable.preExecute();
		parser.setDataSource(aDBConnection);
		lookupTable.setNumCached(1000);
		lookupTable.setStoreNulls(true);
		lookupTable.init();
		lookup = lookupTable.createLookup(recordKey, customer);
		start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
//			 log.info("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee = lookup.next();
//				 log.info("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		log.info("caching=1000, storeNulls=true:");
		log.info("Total number searched: " + ((DBLookup) lookup).getTotalNumber());
		log.info("From cache found: " + ((DBLookup) lookup).getCacheNumber());
		log.info("Timing: " + (System.currentTimeMillis() - start));
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
			// log.info("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee = lookup.next();
				// log.info("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		log.info("caching=3000, storeNulls=false:");
		log.info("Total number searched: " + ((DBLookup) lookup).getTotalNumber());
		log.info("From cache found: " + ((DBLookup) lookup).getCacheNumber());
		log.info("Timing: " + (System.currentTimeMillis() - start));
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
			// log.info("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee = lookup.next();
				// log.info("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		lookupTable.postExecute();
		log.info("caching=3000, storeNulls=true:");
		log.info("Total number searched: " + ((DBLookup) lookup).getTotalNumber());
		log.info("From cache found: " + ((DBLookup) lookup).getCacheNumber());
		log.info("Timing: " + (System.currentTimeMillis() - start));
	}

}
