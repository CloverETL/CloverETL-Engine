/*
 * Created on Jun 4, 2003
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package test.org.jetel.util;

import org.jetel.data.DataRecord;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ClassBuilder;

import junit.framework.TestCase;

/**
 * @author Wes Maciorowski
 * @version 1.0
 * 
 * JUnit tests for org.jetel.util.CodeParser class.
 */
public class CodeParserTest extends TestCase  {
	StringBuffer test0 = null;
	StringBuffer test1 = null;
	StringBuffer test2 = null;
	StringBuffer test3 = null;
	StringBuffer test4 = null;
	ClassBuilder aClassBuilder = null;

	protected void setUp() {
		//lets create 5 test cases
		
		// no record references in the code
		
		// input record reference in the code
		
		// reference to another field in the code
		
		// input record reference and reference to another field in the code

		// input record reference and reference to another field in the code
		// plus incorrect refs
		
		DataFieldMetadata aDataFieldMetadata = null;

		DataRecordMetadata atestRecordMetadata = new DataRecordMetadata("test",DataRecordMetadata.FIXEDLEN_RECORD);
		aDataFieldMetadata = new DataFieldMetadata("Field 0",DataFieldMetadata.STRING_FIELD,(short)20);
		aDataFieldMetadata.setCodeStr(test0.toString());
		aDataFieldMetadata = new DataFieldMetadata("Field 1",DataFieldMetadata.NUMERIC_FIELD,(short)1);
		aDataFieldMetadata.setCodeStr(test0.toString());
		aDataFieldMetadata = new DataFieldMetadata("Field 2",DataFieldMetadata.INTEGER_FIELD,(short)5);
		aDataFieldMetadata.setCodeStr(test0.toString());
		aDataFieldMetadata = new DataFieldMetadata("Field 3",DataFieldMetadata.NUMERIC_FIELD,(short)5);
		aDataFieldMetadata.setCodeStr(test0.toString());
		aDataFieldMetadata = new DataFieldMetadata("Field 4",DataFieldMetadata.NUMERIC_FIELD,(short)5);
		aDataFieldMetadata.setCodeStr(test0.toString());

		atestRecordMetadata.addField(aDataFieldMetadata);


		// lets create out test data record
		DataRecord record = new DataRecord(atestRecordMetadata);
		
		// lets create 2 input records
		DataRecordMetadata[] arrayDataRecordMetadata = null;
		arrayDataRecordMetadata = new DataRecordMetadata[2];
		
		DataRecordMetadata aFixedDataRecordMetadata = new DataRecordMetadata("record1",DataRecordMetadata.FIXEDLEN_RECORD);
		aFixedDataRecordMetadata.addField(new DataFieldMetadata("Field 0",DataFieldMetadata.STRING_FIELD,(short)20));
		aFixedDataRecordMetadata.addField(new DataFieldMetadata("Field 1",DataFieldMetadata.BYTE_FIELD,(short)1));
		aFixedDataRecordMetadata.addField(new DataFieldMetadata("Field 2",DataFieldMetadata.INTEGER_FIELD,(short)5));
		aFixedDataRecordMetadata.addField(new DataFieldMetadata("Field 3",DataFieldMetadata.INTEGER_FIELD,(short)5));
		aFixedDataRecordMetadata.addField(new DataFieldMetadata("Field 4",DataFieldMetadata.NUMERIC_FIELD,(short)10));

		DataRecordMetadata aDelimitedDataRecordMetadata = new DataRecordMetadata("record2",DataRecordMetadata.DELIMITED_RECORD);
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 0",DataFieldMetadata.STRING_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 1",DataFieldMetadata.BYTE_FIELD,":"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 3",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field 4",DataFieldMetadata.NUMERIC_FIELD,";"));
		
		arrayDataRecordMetadata[0] = aFixedDataRecordMetadata;
		arrayDataRecordMetadata[1] = aDelimitedDataRecordMetadata;
		
		aClassBuilder = new ClassBuilder(record,arrayDataRecordMetadata);
	}

	protected void tearDown() {
	}


	/**
	 *  Test for @link int[][]  org.jetel.util.CodeParser.parse()
	 *
	 */

	public void testparse() {}
	
}
