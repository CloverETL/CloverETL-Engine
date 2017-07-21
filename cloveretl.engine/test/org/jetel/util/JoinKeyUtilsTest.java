package org.jetel.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.joinKey.JoinKeyUtils;
import org.jetel.util.joinKey.OrderedKey;

public class JoinKeyUtilsTest extends CloverTestCase {

	private final static int MASTER = 0;
	private final static int SLAVE = 1;
	
	List<DataRecordMetadata> metadata = new ArrayList<DataRecordMetadata>();
	DataRecordMetadata meta1;
	DataRecordMetadata meta2;
	DataRecordMetadata meta3;
	DataRecordMetadata meta4;
	DataRecordMetadata[] myMeta;
	
	public JoinKeyUtilsTest() {
		initEngine();
		meta1 = new DataRecordMetadata("master");
		meta2 = new DataRecordMetadata("slave1");
		meta3 = new DataRecordMetadata("slave2");
		meta4 = new DataRecordMetadata("slave3");
		
		myMeta = new DataRecordMetadata[3];
		myMeta[0] = new DataRecordMetadata("mymaster");
		myMeta[1] = new DataRecordMetadata("myfields");
		myMeta[2] = new DataRecordMetadata("mybanana");
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
		
		String[][] fieldNames = {{"master1", "master2", "master3"}, {"fieldA", "fieldB", "fieldC"}, {"apple", "banana", "carrot"}};
		for (int i = 0; i < fieldNames.length; i++) {
			for (String fieldName : fieldNames[i]) {
				myMeta[i].addField(new DataFieldMetadata(fieldName, ";"));
			}
		}
		
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
	
	public void testParseOrderedKey1() throws ComponentNotReadyException {
		String joinKey = "$master1(a)=$fieldA;$master2(d)=$fieldB;$master3(a)=$fieldC;#$master1(a)=$apple;$master2(d)=$banana;$master3(a)=$carrot;";
		OrderedKey[][] parsedKey = JoinKeyUtils.parseMergeJoinOrderedKey(joinKey, Arrays.asList(myMeta));
		OrderedKey[] masterKey = parsedKey[0];
		OrderedKey[] slave1Key = parsedKey[1];
		OrderedKey[] slave2Key = parsedKey[2];

		assertEquals("master1", masterKey[0].getKeyName());
		assertEquals("master2", masterKey[1].getKeyName());
		assertEquals("master3", masterKey[2].getKeyName());
		assertEquals("a", masterKey[0].getOrdering().getShortCut());
		assertEquals("d", masterKey[1].getOrdering().getShortCut());
		assertEquals("a", masterKey[2].getOrdering().getShortCut());
		
		assertEquals("fieldA", slave1Key[0].getKeyName());
		assertEquals("fieldB", slave1Key[1].getKeyName());
		assertEquals("fieldC", slave1Key[2].getKeyName());
		assertNull(slave1Key[0].getOrdering());
		assertNull(slave1Key[1].getOrdering());
		assertNull(slave1Key[2].getOrdering());

		assertEquals("apple", slave2Key[0].getKeyName());
		assertEquals("banana", slave2Key[1].getKeyName());
		assertEquals("carrot", slave2Key[2].getKeyName());
		assertNull(slave2Key[0].getOrdering());
		assertNull(slave2Key[1].getOrdering());
		assertNull(slave2Key[2].getOrdering());
	}
	public void testParseOrderedKey2() throws ComponentNotReadyException {
		String joinKey = "$master1(a);$master2(d);$master3(a)#$fieldA(a);$fieldB(d);$fieldC(a);#$apple(a);$banana(d);$carrot(a);";
		OrderedKey[][] parsedKey = JoinKeyUtils.parseMergeJoinOrderedKey(joinKey, Arrays.asList(myMeta));
		OrderedKey[] masterKey = parsedKey[0];
		OrderedKey[] slave1Key = parsedKey[1];
		OrderedKey[] slave2Key = parsedKey[2];

		assertEquals("master1", masterKey[0].getKeyName());
		assertEquals("master2", masterKey[1].getKeyName());
		assertEquals("master3", masterKey[2].getKeyName());
		assertEquals("a", masterKey[0].getOrdering().getShortCut());
		assertEquals("d", masterKey[1].getOrdering().getShortCut());
		assertEquals("a", masterKey[2].getOrdering().getShortCut());
		
		assertEquals("fieldA", slave1Key[0].getKeyName());
		assertEquals("fieldB", slave1Key[1].getKeyName());
		assertEquals("fieldC", slave1Key[2].getKeyName());
		assertEquals("a", slave1Key[0].getOrdering().getShortCut());
		assertEquals("d", slave1Key[1].getOrdering().getShortCut());
		assertEquals("a", slave1Key[2].getOrdering().getShortCut());

		assertEquals("apple", slave2Key[0].getKeyName());
		assertEquals("banana", slave2Key[1].getKeyName());
		assertEquals("carrot", slave2Key[2].getKeyName());
		assertEquals("a", slave2Key[0].getOrdering().getShortCut());
		assertEquals("d", slave2Key[1].getOrdering().getShortCut());
		assertEquals("a", slave2Key[2].getOrdering().getShortCut());
	}

}
