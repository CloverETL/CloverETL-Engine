package org.jetel.util;

import java.util.ArrayList;
import java.util.List;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.joinKey.JoinKeyUtils;

public class JoinKeyUtilsTest extends CloverTestCase {

	private final static int MASTER = 0;
	private final static int SLAVE = 1;
	
	List<DataRecordMetadata> metadata = new ArrayList<DataRecordMetadata>();
	DataRecordMetadata meta1;
	DataRecordMetadata meta2;
	DataRecordMetadata meta3;
	DataRecordMetadata meta4;
	
	public JoinKeyUtilsTest() {
		initEngine();
		meta1 = new DataRecordMetadata("master");
		meta2 = new DataRecordMetadata("slave1");
		meta3 = new DataRecordMetadata("slave2");
		meta4 = new DataRecordMetadata("slave3");
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
    	DataFieldMetadata f1 = new DataFieldMetadata("field1",";");
    	DataFieldMetadata f2 = new DataFieldMetadata("field2",";");
    	DataFieldMetadata f3 = new DataFieldMetadata("field3",";");
    	meta1.addField(f1);
    	meta1.addField(f2);
    	meta1.addField(f3);
    	meta2.addField(f1);
    	meta2.addField(f2);
    	meta2.addField(f3);
    	meta3.addField(f1);
    	meta3.addField(f2);
    	meta3.addField(f3);
    	meta4.addField(f1);
    	meta4.addField(f2);
    	meta4.addField(f3);
		metadata.add(meta1);
		metadata.add(meta2);
		metadata.add(meta3);
		metadata.add(meta4);
	}

	public void testParseHashJoiners() throws ComponentNotReadyException{
		String joinKey = "master.field1=slave1.field1;master.field2=slave1.field2#" +
						 "master.field1=slave2.field1;master.field3=slave2.field3";
		String[][][] parsedKey = JoinKeyUtils.parseHashJoinKey(joinKey, metadata);
		assertEquals("field1", parsedKey[MASTER][0][0]);
		assertEquals("field1", parsedKey[SLAVE][0][0]);
		assertEquals("field1", parsedKey[SLAVE][1][0]);
		assertEquals("field2", parsedKey[SLAVE][0][1]);
		assertEquals("field3", parsedKey[SLAVE][1][1]);
		
		joinKey = "master.field1=slave2.field1;master.field3=slave2.field3#master.field1=slave1.field1;master.field2=slave1.field2";
		parsedKey = JoinKeyUtils.parseHashJoinKey(joinKey, metadata);
		assertEquals("field1", parsedKey[MASTER][0][0]);
		assertEquals("field1", parsedKey[SLAVE][0][0]);
		assertEquals("field1", parsedKey[SLAVE][1][0]);
		assertEquals("field2", parsedKey[SLAVE][0][1]);
		assertEquals("field3", parsedKey[SLAVE][1][1]);
		
		joinKey = "field1=slave2.field1;field3=slave2.field3#=slave1.field1;=slave1.field2";
		parsedKey = JoinKeyUtils.parseHashJoinKey(joinKey, metadata);
		assertEquals("field1", parsedKey[MASTER][0][0]);
		assertEquals("field1", parsedKey[SLAVE][0][0]);
		assertEquals("field1", parsedKey[SLAVE][1][0]);
		assertEquals("field2", parsedKey[SLAVE][0][1]);
		assertEquals("field3", parsedKey[SLAVE][1][1]);

		joinKey = "master.field1=slave2.field1;master.field3=slave2.field3#=slave3.field1;=field3#master.field1=slave1.field1;master.field2=slave1.field2";
		parsedKey = JoinKeyUtils.parseHashJoinKey(joinKey, metadata);
		assertEquals("field1", parsedKey[MASTER][0][0]);
		assertEquals("field1", parsedKey[SLAVE][0][0]);
		assertEquals("field1", parsedKey[SLAVE][1][0]);
		assertEquals("field1", parsedKey[SLAVE][2][0]);
		assertEquals("field2", parsedKey[SLAVE][0][1]);
		assertEquals("field3", parsedKey[SLAVE][1][1]);
		assertEquals("field3", parsedKey[SLAVE][2][1]);
		
		joinKey = "0.field1=2.field1;0.field3=2.field3#=3.field1;=field3#0.field1=1.field1;0.field2=1.field2";
		parsedKey = JoinKeyUtils.parseHashJoinKey(joinKey, metadata);
		assertEquals("field1", parsedKey[MASTER][0][0]);
		assertEquals("field1", parsedKey[SLAVE][0][0]);
		assertEquals("field1", parsedKey[SLAVE][1][0]);
		assertEquals("field1", parsedKey[SLAVE][2][0]);
		assertEquals("field2", parsedKey[SLAVE][0][1]);
		assertEquals("field3", parsedKey[SLAVE][1][1]);
		assertEquals("field3", parsedKey[SLAVE][2][1]);
		
		joinKey = "master.field1=slave2.field1;master.field3=slave.field3#=slave3.field1;=field3#master.field1=slave1.field1;master.field2=slave1.field2";
		try{
			parsedKey = JoinKeyUtils.parseHashJoinKey(joinKey, metadata);
			fail("should rise ComponentNotReadyException");
		}catch(ComponentNotReadyException ex){
			System.out.println(ex.getMessage());
		}
		
		joinKey = "master.field1=slave.field1;master.field3=slave.field3#=slave3.field1;=field3#master.field1=slave1.field1;master.field2=slave1.field2";
		try{
			parsedKey = JoinKeyUtils.parseHashJoinKey(joinKey, metadata);
			fail("should rise ComponentNotReadyException");
		}catch(ComponentNotReadyException ex){
			System.out.println(ex.getMessage());
		}

	}

	public void testParseMergeoiners() throws ComponentNotReadyException{
		String joinKey = "master.field1=slave1.field1;master.field2=slave1.field2#" +
						 "master.field1=slave2.field1;master.field2=slave2.field3";
		String[][] parsedKey = JoinKeyUtils.parseMergeJoinKey(joinKey, metadata);
		String[] masterKey = parsedKey[0];
		String[] slave1Key = parsedKey[1];
		String[] slave2Key = parsedKey[2];
		assertEquals("field1", masterKey[0]);
		assertEquals("field1", slave1Key[0]);
		assertEquals("field1", slave2Key[0]);
		assertEquals("field2", masterKey[1]);
		assertEquals("field3", slave2Key[1]);
		
		joinKey = "master.field1=slave2.field1;master.field3=slave2.field3#" +
				  "master.field1=slave1.field1;master.field3=slave1.field2";
		parsedKey = JoinKeyUtils.parseMergeJoinKey(joinKey, metadata);
		masterKey = parsedKey[0];
		slave1Key = parsedKey[1];
		slave2Key = parsedKey[2];
		assertEquals("field1", masterKey[0]);
		assertEquals("field1", slave1Key[0]);
		assertEquals("field1", slave2Key[0]);
		assertEquals("field3", masterKey[1]);
		assertEquals("field3", slave2Key[1]);
		
		joinKey = "$field1=$slave2.field1;$field3=$slave2.field3#" +
						  "$slave1.field1;        $slave1.field2";
		parsedKey = JoinKeyUtils.parseMergeJoinKey(joinKey, metadata);
		masterKey = parsedKey[0];
		slave1Key = parsedKey[1];
		slave2Key = parsedKey[2];
		assertEquals("field1", masterKey[0]);
		assertEquals("field1", slave1Key[0]);
		assertEquals("field1", slave2Key[0]);
		assertEquals("field3", masterKey[1]);
		assertEquals("field3", slave2Key[1]);

		joinKey = "field1=field1#field1=field2";
		parsedKey = JoinKeyUtils.parseMergeJoinKey(joinKey, metadata);
		masterKey = parsedKey[0];
		slave1Key = parsedKey[1];
		slave2Key = parsedKey[2];
		assertEquals("field1", masterKey[0]);
		assertEquals("field1", slave1Key[0]);
		assertEquals("field2", slave2Key[0]);

		joinKey = "master.field1;master.field2#slave2.field1;slave2.field3#slave1.field1;slave1.field2";
		parsedKey = JoinKeyUtils.parseMergeJoinKey(joinKey, metadata);
		masterKey = parsedKey[0];
		slave1Key = parsedKey[1];
		slave2Key = parsedKey[2];
		assertEquals("field1", masterKey[0]);
		assertEquals("field1", slave1Key[0]);
		assertEquals("field1", slave2Key[0]);
		assertEquals("field2", masterKey[1]);
		assertEquals("field3", slave2Key[1]);
		
		joinKey = "0.field1;0.field2#2.field1;2.field3#1.field1;1.field2";
		parsedKey = JoinKeyUtils.parseMergeJoinKey(joinKey, metadata);
		masterKey = parsedKey[0];
		slave1Key = parsedKey[1];
		slave2Key = parsedKey[2];
		assertEquals("field1", masterKey[0]);
		assertEquals("field1", slave1Key[0]);
		assertEquals("field1", slave2Key[0]);
		assertEquals("field2", masterKey[1]);
		assertEquals("field3", slave2Key[1]);
		
		joinKey = "field1;field2#field1;field2#field1;field3";
		parsedKey = JoinKeyUtils.parseMergeJoinKey(joinKey, metadata);
		masterKey = parsedKey[0];
		slave1Key = parsedKey[1];
		slave2Key = parsedKey[2];
		assertEquals("field1", masterKey[0]);
		assertEquals("field1", slave1Key[0]);
		assertEquals("field1", slave2Key[0]);
		assertEquals("field2", masterKey[1]);
		assertEquals("field3", slave2Key[1]);
		
		joinKey = "field1;field2;field3";
		parsedKey = JoinKeyUtils.parseMergeJoinKey(joinKey, metadata);
		assertEquals(1, parsedKey.length);
		masterKey = parsedKey[0];
		assertEquals("field1", masterKey[0]);
		assertEquals("field2", masterKey[1]);
		assertEquals("field3", masterKey[2]);
		
		joinKey = "master.field1=slave2.field1;master.field3=slave.field3#" +
				  "              slave3.field1;              slave3.field3#" +
				  "master.field1=slave1.field1;master.field2=slave1.field2";
		try{
			parsedKey = JoinKeyUtils.parseMergeJoinKey(joinKey, metadata);
			fail("should rise ComponentNotReadyException");
		}catch(ComponentNotReadyException ex){
			System.out.println(ex.getMessage());
		}
		
		joinKey = "master.field1=slave.field1;master.field3=slave.field3#=slave3.field1;=field3#master.field1=slave1.field1;master.field2=slave1.field2";
		try{
			parsedKey = JoinKeyUtils.parseMergeJoinKey(joinKey, metadata);
			fail("should rise ComponentNotReadyException");
		}catch(ComponentNotReadyException ex){
			System.out.println(ex.getMessage());
		}

		joinKey = "master.field1;master.field2#slave2.field1;slave2.field3#slave1.field1;slave1.field2;slave1.field3";
		try{
			parsedKey = JoinKeyUtils.parseMergeJoinKey(joinKey, metadata);
			fail("should rise ComponentNotReadyException");
		}catch(ComponentNotReadyException ex){
			System.out.println(ex.getMessage());
		}
	}

}
