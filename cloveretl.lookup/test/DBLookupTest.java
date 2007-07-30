import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.TestCase;

import org.jetel.connection.DBConnection;
import org.jetel.connection.SQLDataParser;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.lookup.DBLookupTable;
import org.jetel.main.runGraph;
import org.jetel.metadata.DataRecordMetadata;



public class DBLookupTest extends TestCase {
	
	DBLookupTable lookupTable;
	DataRecord customer, employee = null;
	SQLDataParser parser;

	@Override
	protected void setUp() throws ComponentNotReadyException, FileNotFoundException, SQLException{

		runGraph.initEngine("../cloveretl.engine/plugins", null);
		DBConnection aDBConnection = new DBConnection("conn", "../cloveretl.engine/examples/koule_postgre.cfg");
		aDBConnection.init();

		Properties p= new Properties();
		p.put("sqlQuery", "select * from customer");
		DataRecordMetadata customerMetadata = aDBConnection.createMetadata(p);
		customer = new DataRecord(customerMetadata);
		customer.init();
		parser = new SQLDataParser("select * from customer");
		parser.init(customerMetadata);
		parser.setDataSource(aDBConnection);
		
		lookupTable = new DBLookupTable("MyLookup",aDBConnection,null,"select * from employee where last_name=?",500);
        lookupTable.init();
		RecordKey recordKey = new RecordKey(new String[]{"lname"}, customerMetadata);
		recordKey.init();
		lookupTable.setLookupKey(recordKey);

	}

	@Override
	protected void tearDown() throws Exception {
		parser.close();
		lookupTable.free();
	}
	
	public void test1() throws JetelException{
		while ((customer = parser.getNext()) != null){
			System.out.println("Looking pair for :\n" + customer);
			employee = lookupTable.get(customer);
			if (employee != null) {
				do {
					System.out.println("Found record:\n" + employee);
				}while ((employee = lookupTable.getNext()) != null);
			}
		}
		System.out.println("Total number found: " + lookupTable.getTotalNumber());
		System.out.println("From cache found: " + lookupTable.getCacheNumber());
	}
}
