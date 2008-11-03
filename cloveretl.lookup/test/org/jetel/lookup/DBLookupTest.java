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
		parser = new SQLDataParser("select * from customer");
		parser.init(customerMetadata);
		parser.setDataSource(aDBConnection);

		lookupTable = new DBLookupTable("MyLookup", aDBConnection, null, "select * from employee where last_name=?", 0);
		lookupTable.init();
		recordKey = new RecordKey(new String[] { "lname" }, customerMetadata);
		recordKey.init();
	}

	@Override
	protected void tearDown() throws Exception {
		parser.close();
		lookupTable.free();
	}

	public void test1() throws JetelException, ComponentNotReadyException {
		lookup = lookupTable.createLookup(recordKey, customer);
		long start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
//			 System.out.println("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee = (DataRecord) lookup.next();
//				System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		System.out.println("Without cashing:");
//		System.out.println("Total number searched: " + lookupTable.getTotalNumber());
//		System.out.println("From cache found: " + lookupTable.getCacheNumber());
		System.out.println("Timing: " + (System.currentTimeMillis() - start));
//		lookupTable.free();

		parser.setDataSource(aDBConnection);
		lookup = lookupTable.createLookup(recordKey, customer);
		start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
			// System.out.println("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee = (DataRecord) lookup.next();
				// System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		System.out.println("Without cashing:");
//		System.out.println("Total number searched: " + lookupTable.getTotalNumber());
//		System.out.println("From cache found: " + lookupTable.getCacheNumber());
		System.out.println("Timing: " + (System.currentTimeMillis() - start));
		lookupTable.free();

		parser.setDataSource(aDBConnection);
		lookupTable.setNumCached(1000);
		lookupTable.setStoreNulls(false);
		lookupTable.init();
		lookup = lookupTable.createLookup(recordKey, customer);
		start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
			 System.out.println("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee = (DataRecord) lookup.next();
				 System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		System.out.println("Cashing=1000, storeNulls=false:");
//		System.out.println("Total number searched: " + lookupTable.getTotalNumber());
//		System.out.println("From cache found: " + lookupTable.getCacheNumber());
		System.out.println("Timing: " + (System.currentTimeMillis() - start));
		lookupTable.free();

		parser.setDataSource(aDBConnection);
		lookupTable.setNumCached(1000);
		lookupTable.setStoreNulls(true);
		lookupTable.init();
		lookup = lookupTable.createLookup(recordKey, customer);
		start = System.currentTimeMillis();
		while ((parser.getNext(customer)) != null) {
			// System.out.println("Looking pair for :\n" + customer);
			lookup.seek();
			while (lookup.hasNext()) {
				employee = (DataRecord) lookup.next();
				// System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		System.out.println("Cashing=1000, storeNulls=true:");
//		System.out.println("Total number searched: " + lookupTable.getTotalNumber());
//		System.out.println("From cache found: " + lookupTable.getCacheNumber());
		System.out.println("Timing: " + (System.currentTimeMillis() - start));
		lookupTable.free();

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
				employee = (DataRecord) lookup.next();
				// System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		System.out.println("Cashing=3000, storeNulls=false:");
//		System.out.println("Total number searched: " + lookupTable.getTotalNumber());
//		System.out.println("From cache found: " + lookupTable.getCacheNumber());
		System.out.println("Timing: " + (System.currentTimeMillis() - start));
		lookupTable.free();

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
				employee = (DataRecord) lookup.next();
				// System.out.println("Found record:\n" + employee);
				assertEquals(customer.getField("lname"), employee.getField("last_name"));
			}
		}
		System.out.println("Cashing=3000, storeNulls=true:");
//		System.out.println("Total number searched: " + lookupTable.getTotalNumber());
//		System.out.println("From cache found: " + lookupTable.getCacheNumber());
		System.out.println("Timing: " + (System.currentTimeMillis() - start));
	}

}
