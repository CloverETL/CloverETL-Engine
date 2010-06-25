package org.jetel.ctl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import junit.framework.AssertionFailedError;

import org.jetel.component.CTLRecordTransform;
import org.jetel.component.RecordTransform;
import org.jetel.data.DataRecord;
import org.jetel.data.SetVal;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.lookup.LookupTableFactory;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.PackedDecimal;
import org.jetel.util.crypto.Base64;
import org.jetel.util.crypto.Digest;
import org.jetel.util.crypto.Digest.DigestType;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.string.StringUtils;

public abstract class CompilerTestCase extends CloverTestCase {

	// ---------- RECORD NAMES -----------
	protected static final String INPUT_1 = "firstInput";
	protected static final String INPUT_2 = "secondInput";
	protected static final String OUTPUT_1 = "firstOutput";
	protected static final String OUTPUT_2 = "secondOutput";
	protected static final String LOOKUP = "lookupMetadata";

	protected static final String NAME_VALUE = "  HELLO  ";
	protected static final Double AGE_VALUE = 20.25;
	protected static final String CITY_VALUE = "Chong'La";

	protected static final Date BORN_VALUE;
	protected static final Long BORN_MILLISEC_VALUE;
	static {
		Calendar c = Calendar.getInstance();
		c.set(2008, 12, 25, 13, 25, 55);
		BORN_VALUE = c.getTime();
		BORN_MILLISEC_VALUE = c.getTimeInMillis();
	}

	protected static final Integer VALUE_VALUE = Integer.MAX_VALUE - 10;
	protected static final Boolean FLAG_VALUE = true;
	protected static final byte[] BYTEARRAY_VALUE = "Abeceda zedla deda".getBytes();

	protected static final BigDecimal CURRENCY_VALUE = new BigDecimal("133.525");
	protected static final int DECIMAL_PRECISION = 7;
	protected static final int DECIMAL_SCALE = 3;
	protected static final int NORMALIZE_RETURN_OK = 0;
	
	public static final int DECIMAL_MAX_PRECISION = 32;
	public static final MathContext MAX_PRECISION = new MathContext(DECIMAL_MAX_PRECISION,RoundingMode.DOWN);

	/** Flag to trigger Java compilation */
	private boolean compileToJava;
	
	protected DataRecord[] inputRecords;
	protected DataRecord[] outputRecords;

	public CompilerTestCase(boolean compileToJava) {
		this.compileToJava = compileToJava;
	}

	/**
	 * Method to execute tested CTL code in a way specific to testing scenario.
	 * 
	 * @param compiler
	 */
	public abstract void executeCode(ITLCompiler compiler);

	/**
	 * Method which provides access to specified global variable
	 * 
	 * @param varName
	 *            global variable to be accessed
	 * @return
	 * 
	 */
	protected abstract Object getVariable(String varName);

	protected void check(String varName, Object expectedResult) {
		assertEquals(varName, expectedResult, getVariable(varName));
	}
	
	protected void checkEquals(String varName1, String varName2) {
		assertEquals("Comparing " + varName1 + " and " + varName2 + " : ", getVariable(varName1), getVariable(varName2));
	}
	
	protected void checkNull(String varName) {
		assertNull(getVariable(varName));
	}

	protected void setUp() {
		initEngine();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		inputRecords = null;
		outputRecords = null;
	}

	protected TransformationGraph createEmptyGraph() {
		return new TransformationGraph();
	}

	protected TransformationGraph createDefaultGraph() {
		TransformationGraph g = createEmptyGraph();
		final HashMap<String, DataRecordMetadata> metadataMap = new HashMap<String, DataRecordMetadata>();
		metadataMap.put(INPUT_1, createDefaultMetadata(INPUT_1));
		metadataMap.put(INPUT_2, createDefaultMetadata(INPUT_2));
		metadataMap.put(OUTPUT_1, createDefaultMetadata(OUTPUT_1));
		metadataMap.put(OUTPUT_2, createDefaultMetadata(OUTPUT_2));
		metadataMap.put(LOOKUP, createDefaultMetadata(LOOKUP));
		g.addDataRecordMetadata(metadataMap);
		g.addSequence(createDefaultSequence(g, "TestSequence"));
		g.addLookupTable(createDefaultLookup(g, "TestLookup"));

		return g;
	}

	protected Sequence createDefaultSequence(TransformationGraph graph, String name) {
		Sequence seq = SequenceFactory.createSequence(graph, "PRIMITIVE_SEQUENCE", new Object[] { "Sequence0", graph, name }, new Class[] { String.class, TransformationGraph.class, String.class });

		try {
			seq.checkConfig(new ConfigurationStatus());
			seq.init();
		} catch (ComponentNotReadyException e) {
			throw new RuntimeException(e);
		}

		return seq;
	}

	/**
	 * Creates default lookup table of type SimpleLookupTable with 4 records using default metadata and a composite
	 * lookup key Name+Value. Use field City for testing response.
	 * 
	 * @param graph
	 * @param name
	 * @return
	 */
	protected LookupTable createDefaultLookup(TransformationGraph graph, String name) {
		final TypedProperties props = new TypedProperties();
		props.setProperty("id", "LookupTable0");
		props.setProperty("type", "simpleLookup");
		props.setProperty("metadata", LOOKUP);
		props.setProperty("key", "Name;Value");
		props.setProperty("name", name);
		props.setProperty("keyDuplicates", "true");

		/*
		 * The test lookup table is populated from file TestLookup.dat. Alternatively uncomment the populating code
		 * below, however this will most probably break down test_lookup() because free() will wipe away all data and
		 * noone will restore them
		 */
		URL dataFile = getClass().getResource("TestLookup.dat");
		if (dataFile == null) {
			throw new RuntimeException("Unable to populate testing lookup table. File 'TestLookup.dat' not found by classloader");
		}
		props.setProperty("fileURL", dataFile.getFile());

		LookupTableFactory.init();
		LookupTable lkp = LookupTableFactory.createLookupTable(props);
		lkp.setGraph(graph);

		try {
			lkp.checkConfig(new ConfigurationStatus());
			lkp.init();
			lkp.preExecute();
		} catch (ComponentNotReadyException ex) {
			throw new RuntimeException(ex);
		}

		// ********* POPULATING CODE *************
		/*DataRecord lkpRecord = createEmptyRecord(createDefaultMetadata("lookupResponse"));

		lkpRecord.getField("Name").setValue("Alpha");
		lkpRecord.getField("Value").setValue(1);
		lkpRecord.getField("City").setValue("Andorra la Vella");
		lkp.put(lkpRecord);

		lkpRecord.getField("Name").setValue("Bravo");
		lkpRecord.getField("Value").setValue(2);
		lkpRecord.getField("City").setValue("Bruxelles");
		lkp.put(lkpRecord);

		// duplicate entry 
		lkpRecord.getField("Name").setValue("Charlie");
		lkpRecord.getField("Value").setValue(3);
		lkpRecord.getField("City").setValue("Chamonix");
		lkp.put(lkpRecord);
		lkpRecord.getField("Name").setValue("Charlie");
		lkpRecord.getField("Value").setValue(3);
		lkpRecord.getField("City").setValue("Chomutov");
		lkp.put(lkpRecord);*/

		// ************ END OF POPULATING CODE ************
		 
		return lkp;
	}

	/**
	 * Creates records with default structure
	 * 
	 * @param name
	 *            name for the record to use
	 * @return metadata with default structure
	 */
	protected DataRecordMetadata createDefaultMetadata(String name) {
		DataRecordMetadata ret = new DataRecordMetadata(name);
		ret.addField(new DataFieldMetadata("Name", DataFieldMetadata.STRING_FIELD, "|"));
		ret.addField(new DataFieldMetadata("Age", DataFieldMetadata.NUMERIC_FIELD, "|"));
		ret.addField(new DataFieldMetadata("City", DataFieldMetadata.STRING_FIELD, "|"));

		DataFieldMetadata dateField = new DataFieldMetadata("Born", DataFieldMetadata.DATE_FIELD, "|");
		dateField.setFormatStr("yyyy-MM-dd HH:mm:ss");
		ret.addField(dateField);

		ret.addField(new DataFieldMetadata("BornMillisec", DataFieldMetadata.LONG_FIELD, "|"));
		ret.addField(new DataFieldMetadata("Value", DataFieldMetadata.INTEGER_FIELD, "|"));
		ret.addField(new DataFieldMetadata("Flag", DataFieldMetadata.BOOLEAN_FIELD, "|"));
		ret.addField(new DataFieldMetadata("ByteArray", DataFieldMetadata.BYTE_FIELD, "|"));

		DataFieldMetadata decimalField = new DataFieldMetadata("Currency", DataFieldMetadata.DECIMAL_FIELD, "\n");
		decimalField.setProperty(DataFieldMetadata.LENGTH_ATTR, String.valueOf(DECIMAL_PRECISION));
		decimalField.setProperty(DataFieldMetadata.SCALE_ATTR, String.valueOf(DECIMAL_SCALE));
		ret.addField(decimalField);

		return ret;
	}

	/**
	 * Creates new record with specified metadata and sets its field to default values. The record structure will be
	 * created by {@link #createDefaultMetadata(String)}
	 * 
	 * @param dataRecordMetadata
	 *            metadata to use
	 * @return record initialized to default values
	 */
	protected DataRecord createDefaultRecord(DataRecordMetadata dataRecordMetadata) {
		final DataRecord ret = new DataRecord(dataRecordMetadata);
		ret.init();

		SetVal.setString(ret, "Name", NAME_VALUE);
		SetVal.setDouble(ret, "Age", AGE_VALUE);
		SetVal.setString(ret, "City", CITY_VALUE);
		SetVal.setDate(ret, "Born", BORN_VALUE);
		SetVal.setLong(ret, "BornMillisec", BORN_MILLISEC_VALUE);
		SetVal.setInt(ret, "Value", VALUE_VALUE);
		SetVal.setValue(ret, "Flag", FLAG_VALUE);
		SetVal.setValue(ret, "ByteArray", BYTEARRAY_VALUE);
		SetVal.setValue(ret, "Currency", CURRENCY_VALUE);

		return ret;
	}

	/**
	 * Allocates new records with structure prescribed by metadata and sets all its fields to <code>null</code>
	 * 
	 * @param metadata
	 *            structure to use
	 * @return empty record
	 */
	protected DataRecord createEmptyRecord(DataRecordMetadata metadata) {
		DataRecord ret = new DataRecord(metadata);
		ret.init();

		for (int i = 0; i < ret.getNumFields(); i++) {
			SetVal.setNull(ret, i);
		}

		return ret;
	}

	protected void doCompile(String expStr, String testIdentifier) {
		TransformationGraph graph = createDefaultGraph();
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };

		// prepend the compilation mode prefix
		if (compileToJava) {
			expStr = "//#CTL2:COMPILE\n" + expStr;
		}

		print_code(expStr);

		ITLCompiler compiler = TLCompilerFactory.createCompiler(graph, inMetadata, outMetadata, "UTF-8");
		List<ErrorMessage> messages = compiler.compile(expStr, CTLRecordTransform.class, testIdentifier);
		printMessages(messages);
		if (compiler.errorCount() > 0) {
			throw new AssertionFailedError("Error in execution. Check standard output for details.");
		}

		// CLVFStart parseTree = compiler.getStart();
		// parseTree.dump("");

		executeCode(compiler);
	}

	protected void doCompileExpectError(String expStr, String testIdentifier, List<String> errCodes) {
		TransformationGraph graph = createDefaultGraph();
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };

		// prepend the compilation mode prefix
		if (compileToJava) {
			expStr = "//#CTL2:COMPILE\n" + expStr;
		}

		print_code(expStr);

		ITLCompiler compiler = TLCompilerFactory.createCompiler(graph, inMetadata, outMetadata, "UTF-8");
		List<ErrorMessage> messages = compiler.compile(expStr, CTLRecordTransform.class, testIdentifier);
		printMessages(messages);

		if (compiler.errorCount() == 0) {
			throw new AssertionFailedError("No errors in parsing. Expected " + errCodes.size() + " errors.");
		}

		if (compiler.errorCount() != errCodes.size()) {
			throw new AssertionFailedError(compiler.errorCount() + " errors in code, but expected " + errCodes.size() + " errors.");
		}

		Iterator<String> it = errCodes.iterator();

		for (ErrorMessage errorMessage : compiler.getDiagnosticMessages()) {
			String expectedError = it.next();
			if (!expectedError.equals(errorMessage.getErrorMessage())) {
				throw new AssertionFailedError("Error : \'" + compiler.getDiagnosticMessages().get(0).getErrorMessage() + "\', but expected: \'" + expectedError + "\'");
			}
		}

		// CLVFStart parseTree = compiler.getStart();
		// parseTree.dump("");

		// executeCode(compiler);
	}

	protected void doCompileExpectError(String testIdentifier, String errCode) {
		doCompileExpectErrors(testIdentifier, Arrays.asList(errCode));
	}

	protected void doCompileExpectErrors(String testIdentifier, List<String> errCodes) {
		URL importLoc = CompilerTestCase.class.getResource(testIdentifier + ".ctl");
		if (importLoc == null) {
			throw new RuntimeException("Test case '" + testIdentifier + ".ctl" + "' not found");
		}

		final StringBuilder sourceCode = new StringBuilder();
		String line = null;
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(importLoc.openStream()));
			while ((line = rd.readLine()) != null) {
				sourceCode.append(line).append("\n");
			}
			rd.close();
		} catch (IOException e) {
			throw new RuntimeException("I/O error occured when reading source file", e);
		}

		doCompileExpectError(sourceCode.toString(), testIdentifier, errCodes);
	}

	/**
	 * Method loads tested CTL code from a file with the name <code>testIdentifier.ctl</code> The CTL code files should
	 * be stored in the same directory as this class.
	 * 
	 * @param Test
	 *            identifier defining CTL file to load code from
	 */
	protected void doCompile(String testIdentifier) {
		URL importLoc = CompilerTestCase.class.getResource(testIdentifier + ".ctl");
		if (importLoc == null) {
			throw new RuntimeException("Test case '" + testIdentifier + ".ctl" + "' not found");
		}

		final StringBuilder sourceCode = new StringBuilder();
		String line = null;
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(importLoc.openStream()));
			while ((line = rd.readLine()) != null) {
				sourceCode.append(line).append("\n");
			}
			rd.close();
		} catch (IOException e) {
			throw new RuntimeException("I/O error occured when reading source file", e);
		}

		doCompile(sourceCode.toString(), testIdentifier);
	}

	protected void printMessages(List<ErrorMessage> diagnosticMessages) {
		for (ErrorMessage e : diagnosticMessages) {
			System.out.println(e);
		}
	}

	/**
	 * Compares two records if they have the same number of fields and identical values in their fields. Does not
	 * consider (or examine) metadata.
	 * 
	 * @param lhs
	 * @param rhs
	 * @return true if records have the same number of fields and the same values in them
	 */
	protected static boolean recordEquals(DataRecord lhs, DataRecord rhs) {
		if (lhs == rhs)
			return true;
		if (rhs == null)
			return false;
		if (lhs == null) {
			return false;
		}
		if (lhs.getNumFields() != rhs.getNumFields()) {
			return false;
		}
		for (int i = 0; i < lhs.getNumFields(); i++) {
			if (lhs.getField(i).isNull()) {
				if (!rhs.getField(i).isNull()) {
					return false;
				}
			} else if (!lhs.getField(i).equals(rhs.getField(i))) {
				return false;
			}
		}
		return true;
	}

	public void print_code(String text) {
		String[] lines = text.split("\n");
		System.out.println("\t:         1         2         3         4         5         ");
		System.out.println("\t:12345678901234567890123456789012345678901234567890123456789");
		for (int i = 0; i < lines.length; i++) {
			System.out.println((i + 1) + "\t:" + lines[i]);
		}
	}
	
//----------------------------- TESTS -----------------------------

	public void test_return_constants() {
		// test case for issue 2257
		System.out.println("Return constants test:");
		doCompile("test_return_constants");

		check("skip", RecordTransform.SKIP);
		check("all", RecordTransform.ALL);
		check("ok", NORMALIZE_RETURN_OK);
	}

	public void test_raise_error_terminal() {
		// test case for issue 2337
		doCompile("test_raise_error_terminal");
	}

	public void test_case_unique_check() {
		// test case for issue 2515
		doCompileExpectErrors("test_case_unique_check", Arrays.asList("Duplicate case", "Duplicate case"));
	}

	public void test_case_unique_check2() {
		// test case for issue 2515
		doCompileExpectErrors("test_case_unique_check2", Arrays.asList("Duplicate case", "Duplicate case"));
	}

	public void test_case_unique_check3() {
		doCompileExpectError("test_case_unique_check3", "Default case is already defined");
	}

	public void test_rvalue_for_append() {
		// test case for issue 3956
		doCompile("test_rvalue_for_append");
		check("a", Arrays.asList("1", "2"));
		check("b", Arrays.asList("a", "b", "c"));
		check("c", Arrays.asList("1", "2", "a", "b", "c"));
	}

	public void test_rvalue_for_map_append() {
		// test case for issue 3960
		doCompile("test_rvalue_for_map_append");
		HashMap<Integer, String> map1instance = new HashMap<Integer, String>();
		map1instance.put(1, "a");
		map1instance.put(2, "b");
		HashMap<Integer, String> map2instance = new HashMap<Integer, String>();
		map2instance.put(3, "c");
		map2instance.put(4, "d");
		HashMap<Integer, String> map3instance = new HashMap<Integer, String>();
		map3instance.put(1, "a");
		map3instance.put(2, "b");
		map3instance.put(3, "c");
		map3instance.put(4, "d");
		check("map1", map1instance);
		check("map2", map2instance);
		check("map3", map3instance);
	}

	public void test_global_field_access() {
		// test case for issue 3957
		doCompileExpectError("test_global_field_access", "Unable to access record field in global scope");
	}

	//TODO Implement
	/*public void test_new() {
		doCompile("test_new");
	}*/

	public void test_parser() {
		System.out.println("\nParser test:");
		doCompile("test_parser");		
	}

	public void test_import() {
		System.out.println("\nImport test:");

		URL importLoc = getClass().getResource("import.ctl");
		String expStr = "import '" + importLoc + "';\n";
		importLoc = getClass().getResource("other.ctl");
		expStr += "import '" + importLoc + "';\n" +
				"integer sumInt;\n" +
				"function integer transform() {\n" +
				"	if (a == 3) {\n" +
				"		otherImportVar++;\n" +
				"	}\n" +
				"	sumInt = sum(a, otherImportVar);\n" + 
				"	return 0;\n" +
				"}\n";
		doCompile(expStr, "test_import");
	}

	public void test_scope() throws ComponentNotReadyException, TransformException {
		System.out.println("\nMapping test:");
		// String expStr =
		// "function string computeSomething(int n) {\n" +
		// "	string s = '';\n" +
		// "	do  {\n" +
		// "		int i = n--;\n" +
		// "		s = s + '-' + i;\n" +
		// "	} while (n > 0)\n" +
		// "	return s;" +
		// "}\n\n" +
		// "function int transform() {\n" +
		// "	printErr(computeSomething(10));\n" +
		// "   return 0;\n" +
		// "}";
		URL importLoc = getClass().getResource("samplecode.ctl");
		String expStr = "import '" + importLoc + "';\n";

		// "function int getIndexOfOffsetStart(string encodedDate) {\n" +
		// "int offsetStart;\n" +
		// "int actualLastMinus;\n" +
		// "int lastMinus = -1;\n" +
		// "if ( index_of(encodedDate, '+') != -1 )\n" +
		// "	return index_of(encodedDate, '+');\n" +
		// "do {\n" +
		// "	actualLastMinus = index_of(encodedDate, '-', lastMinus+1);\n" +
		// "	if ( actualLastMinus != -1 )\n" +
		// "		lastMinus = actualLastMinus;\n" +
		// "} while ( actualLastMinus != -1 )\n" +
		// "return lastMinus;\n" +
		// "}\n" +
		// "function int transform() {\n" +
		// "	getIndexOfOffsetStart('2009-04-24T08:00:00-05:00');\n" +
		// " 	return 0;\n" +
		// "}\n";

		doCompile(expStr, "test_scope");

	}

//-------------------------- Data Types Tests ---------------------

	public void test_type_void() {
		doCompileExpectErrors("test_type_void", Arrays.asList("Syntax error on token 'void'",
				"Variable 'voidVar' is not declared",
				"Variable 'voidVar' is not declared",
				"Syntax error on token 'void'"));
	}
	
	public void test_type_integer() {
		doCompile("test_type_integer");
		check("i", 0);
		check("j", -1);
		check("field", VALUE_VALUE);
		checkNull("nullValue");
		check("varWithInitializer", 123);
		checkNull("varWithNullInitializer");
	}
	
	public void test_type_integer_edge() {
		String testExpression = 
			"integer minInt;\n"+
			"integer maxInt;\n"+			
			"function integer transform() {\n" +
				"minInt=" + Integer.MIN_VALUE + ";\n" +
				"printErr(minInt, true);\n" + 
				"maxInt=" + Integer.MAX_VALUE + ";\n" +
				"printErr(maxInt, true);\n" + 
				"return 0;\n" +
			"}\n";
		doCompile(testExpression, "test_int_edge");
		check("minInt", Integer.MIN_VALUE);
		check("maxInt", Integer.MAX_VALUE);
	}
	
	public void test_type_long() {
		doCompile("test_type_long");
		check("i", Long.valueOf(0));
		check("j", Long.valueOf(-1));
		check("field", BORN_MILLISEC_VALUE);
		check("def", Long.valueOf(0));
		checkNull("nullValue");
		check("varWithInitializer", 123L);
		checkNull("varWithNullInitializer");
	}
	
	public void test_type_long_edge() {
		String expStr = 
			"long minLong;\n"+
			"long maxLong;\n"+			
			"function integer transform() {\n" +
				"minLong=" + (Long.MIN_VALUE) + "L;\n" +
				"printErr(minLong);\n" +
				"maxLong=" + (Long.MAX_VALUE) + "L;\n" +
				"printErr(maxLong);\n" +
				"return 0;\n" +
			"}\n";

		doCompile(expStr,"test_long_edge");
		check("minLong", Long.MIN_VALUE);
		check("maxLong", Long.MAX_VALUE);
	}
	
	public void test_type_decimal() {
		doCompile("test_type_decimal");
		check("i", new BigDecimal(0, MAX_PRECISION));
		check("j", new BigDecimal(-1, MAX_PRECISION));
		check("field", CURRENCY_VALUE);
		check("def", new BigDecimal(0, MAX_PRECISION));
		checkNull("nullValue");
		check("varWithInitializer", new BigDecimal("123.35", MAX_PRECISION));
		checkNull("varWithNullInitializer");
		check("varWithInitializerNoDist", new BigDecimal(123.35, MAX_PRECISION));
	}
	
	public void test_type_decimal_edge() {
		String testExpression = 
			"decimal minLong;\n"+
			"decimal maxLong;\n"+
			"decimal minLongNoDist;\n"+
			"decimal maxLongNoDist;\n"+
			"decimal minDouble;\n"+
			"decimal maxDouble;\n"+
			"decimal minDoubleNoDist;\n"+
			"decimal maxDoubleNoDist;\n"+
			
			"function integer transform() {\n" +
				"minLong=" + String.valueOf(Long.MIN_VALUE) + "d;\n" +
				"printErr(minLong);\n" + 
				"maxLong=" + String.valueOf(Long.MAX_VALUE) + "d;\n" +
				"printErr(maxLong);\n" + 
				"minLongNoDist=" + String.valueOf(Long.MIN_VALUE) + "L;\n" +
				"printErr(minLongNoDist);\n" + 
				"maxLongNoDist=" + String.valueOf(Long.MAX_VALUE) + "L;\n" +
				"printErr(maxLongNoDist);\n" +
				// distincter will cause the double-string be parsed into exact representation within BigDecimal
				"minDouble=" + String.valueOf(Double.MIN_VALUE) + "D;\n" +
				"printErr(minDouble);\n" + 
				"maxDouble=" + String.valueOf(Double.MAX_VALUE) + "D;\n" +
				"printErr(maxDouble);\n" +
				// no distincter will cause the double-string to be parsed into inexact representation within double
				// then to be assigned into BigDecimal (which will extract only MAX_PRECISION digits)
				"minDoubleNoDist=" + String.valueOf(Double.MIN_VALUE) + ";\n" +
				"printErr(minDoubleNoDist);\n" + 
				"maxDoubleNoDist=" + String.valueOf(Double.MAX_VALUE) + ";\n" +
				"printErr(maxDoubleNoDist);\n" +
				"return 0;\n" +
			"}\n";
		
		doCompile(testExpression, "test_decimal_edge");
		
		check("minLong", new BigDecimal(String.valueOf(Long.MIN_VALUE), MAX_PRECISION));
		check("maxLong", new BigDecimal(String.valueOf(Long.MAX_VALUE), MAX_PRECISION));
		check("minLongNoDist", new BigDecimal(String.valueOf(Long.MIN_VALUE), MAX_PRECISION));
		check("maxLongNoDist", new BigDecimal(String.valueOf(Long.MAX_VALUE), MAX_PRECISION));
		// distincter will cause the MIN_VALUE to be parsed into exact representation (i.e. 4.9E-324)
		check("minDouble", new BigDecimal(String.valueOf(Double.MIN_VALUE), MAX_PRECISION));
		check("maxDouble", new BigDecimal(String.valueOf(Double.MAX_VALUE), MAX_PRECISION));
		// no distincter will cause MIN_VALUE to be parsed into double inexact representation and extraction of
		// MAX_PRECISION digits (i.e. 4.94065.....E-324)
		check("minDoubleNoDist", new BigDecimal(Double.MIN_VALUE, MAX_PRECISION));
		check("maxDoubleNoDist", new BigDecimal(Double.MAX_VALUE, MAX_PRECISION));
	}
	
	public void test_type_number() {
		doCompile("test_type_number");
		
		check("i", Double.valueOf(0));
		check("j", Double.valueOf(-1));
		check("field", AGE_VALUE);
		check("def", Double.valueOf(0));
		checkNull("nullValue");
		checkNull("varWithNullInitializer");
	}
	
	public void test_type_number_edge() {
		String testExpression = 
			"number minDouble;\n" +
			"number maxDouble;\n"+		
			"function integer transform() {\n" +
				"minDouble=" + Double.MIN_VALUE + ";\n" +
				"printErr(minDouble);\n" +
				"maxDouble=" + Double.MAX_VALUE + ";\n" +
				"printErr(maxDouble);\n" +
				"return 0;\n" +
			"}\n";
		doCompile(testExpression, "test_number_edge");
		check("minDouble", Double.valueOf(Double.MIN_VALUE));
		check("maxDouble", Double.valueOf(Double.MAX_VALUE));
	}

	public void test_type_string() {
		doCompile("test_type_string");
		check("i","0");
		check("helloEscaped", "hello\\nworld");
		check("helloExpanded", "hello\nworld");
		check("fieldName", NAME_VALUE);
		check("fieldCity", CITY_VALUE);
		check("escapeChars", "a\u0101\u0102A");
		check("doubleEscapeChars", "a\\u0101\\u0102A");
		check("specialChars", "špeciálne značky s mäkčeňom môžu byť");
		check("dQescapeChars", "a\u0101\u0102A");
		//TODO:Is next test correct?
		check("dQdoubleEscapeChars", "a\\u0101\\u0102A");
		check("dQspecialChars", "špeciálne značky s mäkčeňom môžu byť");
		check("empty", "");
		check("def", "");
		checkNull("varWithNullInitializer");
}
	
	public void test_type_string_long() {
		int length = 1000;
		StringBuilder tmp = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			tmp.append(i % 10);
		}
		String testExpression = 
			"string longString;\n" +
			"function integer transform() {\n" +
				"longString=\"" + tmp + "\";\n" +
				"printErr(longString);\n" +
				"return 0;\n" +
			"}\n";
		doCompile(testExpression, "test_string_long");
		
		check("longString", String.valueOf(tmp));
	}
	
	public void test_type_date() {
		doCompile("test_type_date");
		check("d3", new GregorianCalendar(2006, GregorianCalendar.AUGUST, 1).getTime());
		check("d2", new GregorianCalendar(2006, GregorianCalendar.AUGUST, 2, 15, 15, 3).getTime());
		check("d1", new GregorianCalendar(2006, GregorianCalendar.JANUARY, 1, 1, 2, 3).getTime());
		check("field", BORN_VALUE);
		checkNull("nullValue");
		check("minValue", new GregorianCalendar(1970, GregorianCalendar.JANUARY, 1, 1, 0, 0).getTime());
		checkNull("varWithNullInitializer");
	}
	
	public void test_type_boolean() {
		doCompile("test_type_boolean");
		check("b1", true);
		check("b2", false);
		check("b3", false);
		checkNull("nullValue");
		checkNull("varWithNullInitializer");
	}
	
	public void test_type_boolean_compare() {
		doCompileExpectErrors("test_type_boolean_compare", Arrays.asList(
				"Operator '>' is not defined for types 'boolean' and 'boolean'", 
				"Operator '>=' is not defined for types 'boolean' and 'boolean'",
				"Operator '<' is not defined for types 'boolean' and 'boolean'",
				"Operator '<=' is not defined for types 'boolean' and 'boolean'",
				"Operator '<' is not defined for types 'boolean' and 'boolean'",
				"Operator '>' is not defined for types 'boolean' and 'boolean'",
				"Operator '>=' is not defined for types 'boolean' and 'boolean'",
				"Operator '<=' is not defined for types 'boolean' and 'boolean'"));
	}
	
	@SuppressWarnings("unchecked")
	public void test_type_list() {
		doCompile("test_type_list");
		check("intList", Arrays.asList(1, 2, 3, 4, 5, 6));
		check("intList2", Arrays.asList(1, 2, 3));
		check("stringList", Arrays.asList(
				"first", "replaced", "third", "fourth",
				"fifth", "sixth", "seventh", "extra"));
		assertEquals((List<String>) getVariable("stringList"), (List<String>) getVariable("stringListCopy"));
	}
	
	@SuppressWarnings("unchecked")
	public void test_type_map() {
		doCompile("test_type_map");
		Map<String, Integer> testMap = (Map<String, Integer>) getVariable("testMap");
		assertEquals(Integer.valueOf(1), testMap.get("zero"));
		assertEquals(Integer.valueOf(2), testMap.get("one"));
		assertEquals(Integer.valueOf(3), testMap.get("two"));
		assertEquals(Integer.valueOf(4), testMap.get("three"));
		assertEquals(4, testMap.size());

		Map<Date, String> dayInWeek = (Map<Date, String>) getVariable("dayInWeek");
		Calendar c = Calendar.getInstance();
		c.set(2009, Calendar.MARCH, 2, 0, 0, 0);
		c.set(Calendar.MILLISECOND, 0);
		assertEquals("Monday", dayInWeek.get(c.getTime()));

		Map<Date, String> dayInWeekCopy = (Map<Date, String>) getVariable("dayInWeekCopy");
		c.set(2009, Calendar.MARCH, 3, 0, 0, 0);
		c.set(Calendar.MILLISECOND, 0);
		assertEquals("Tuesday", ((Map<Date, String>) getVariable("tuesday")).get(c.getTime()));
		assertEquals("Tuesday", dayInWeekCopy.get(c.getTime()));

		c.set(2009, Calendar.MARCH, 4, 0, 0, 0);
		c.set(Calendar.MILLISECOND, 0);
		assertEquals("Wednesday", ((Map<Date, String>) getVariable("wednesday")).get(c.getTime()));
		assertEquals("Wednesday", dayInWeekCopy.get(c.getTime()));
		assertFalse(dayInWeek.equals(dayInWeekCopy));
	}
	
	public void test_type_record() {
		doCompile("test_type_record");

		// expected result
		DataRecord expected = createDefaultRecord(createDefaultMetadata("expected"));

		// simple copy
		assertTrue(recordEquals(expected, inputRecords[0]));
		assertTrue(recordEquals(expected, (DataRecord) getVariable("copy")));

		// copy and modify
		expected.getField("Name").setValue("empty");
		expected.getField("Value").setValue(321);
		Calendar c = Calendar.getInstance();
		c.set(1987, Calendar.NOVEMBER, 13, 0, 0, 0);
		c.set(Calendar.MILLISECOND, 0);
		expected.getField("Born").setValue(c.getTime());
		assertTrue(recordEquals(expected, (DataRecord) getVariable("modified")));

		// 2x modified copy
		expected.getField("Name").setValue("not empty");
		assertTrue(recordEquals(expected, (DataRecord)getVariable("modified2")));
		
		// modified by reference
		expected.getField("Value").setValue(654321);
		assertTrue(recordEquals(expected, (DataRecord)getVariable("modified3")));
		assertTrue(recordEquals(expected, (DataRecord)getVariable("reference")));
		assertTrue(getVariable("modified3") == getVariable("reference"));
		
		// output record
		assertTrue(recordEquals(expected, outputRecords[1]));
		
		// null record
		expected.setToNull();
		assertTrue(recordEquals(expected, (DataRecord)getVariable("nullRecord")));
	}
	
//------------------------ Operator Tests ---------------------------
	public void test_variables() {
		doCompile("test_variables");

		check("b1", true);
		check("b2", true);
		check("b4", "hi");
		check("i", 2);
	}

	public void test_operator_plus() {
		doCompile("test_operator_plus");

		check("iplusj", 10 + 100);
		check("lplusm", Long.valueOf(Integer.MAX_VALUE) + Long.valueOf(Integer.MAX_VALUE / 10));
		check("mplusl", getVariable("lplusm"));
		check("mplusi", Long.valueOf(Integer.MAX_VALUE) + 10);
		check("iplusm", getVariable("mplusi"));
		check("nplusm1", Double.valueOf(0.1D + 0.001D));
		check("nplusj", Double.valueOf(100 + 0.1D));
		check("jplusn", getVariable("nplusj"));
		check("m1plusm", Double.valueOf(Long.valueOf(Integer.MAX_VALUE) + 0.001d));
		check("mplusm1", getVariable("m1plusm"));
		check("dplusd1", new BigDecimal("0.1", MAX_PRECISION).add(new BigDecimal("0.0001", MAX_PRECISION), MAX_PRECISION));
		check("dplusj", new BigDecimal(100, MAX_PRECISION).add(new BigDecimal("0.1", MAX_PRECISION), MAX_PRECISION));
		check("jplusd", getVariable("dplusj"));
		check("dplusm", new BigDecimal(Long.valueOf(Integer.MAX_VALUE), MAX_PRECISION).add(new BigDecimal("0.1", MAX_PRECISION), MAX_PRECISION));
		check("mplusd", getVariable("dplusm"));
		check("dplusn", new BigDecimal("0.1").add(new BigDecimal(0.1D, MAX_PRECISION)));
		check("nplusd", getVariable("dplusn"));
		check("spluss1", "hello world");
		check("splusj", "hello100");
		check("jpluss", "100hello");
		check("splusm", "hello" + Long.valueOf(Integer.MAX_VALUE));
		check("mpluss", Long.valueOf(Integer.MAX_VALUE) + "hello");
		check("splusm1", "hello" + 0.001D);
		check("m1pluss", 0.001D + "hello");
		check("splusd1", "hello" + new BigDecimal("0.0001"));
		check("d1pluss", new BigDecimal("0.0001", MAX_PRECISION) + "hello");
	}

	public void test_operator_minus() {
		doCompile("test_operator_minus");

		check("iminusj", 10 - 100);
		check("lminusm", Long.valueOf(Integer.MAX_VALUE / 10) - Long.valueOf(Integer.MAX_VALUE));
		check("mminusi", Long.valueOf(Integer.MAX_VALUE - 10));
		check("iminusm", 10 - Long.valueOf(Integer.MAX_VALUE));
		check("nminusm1", Double.valueOf(0.1D - 0.001D));
		check("nminusj", Double.valueOf(0.1D - 100));
		check("jminusn", Double.valueOf(100 - 0.1D));
		check("m1minusm", Double.valueOf(0.001D - Long.valueOf(Integer.MAX_VALUE)));
		check("mminusm1", Double.valueOf(Long.valueOf(Integer.MAX_VALUE) - 0.001D));
		check("dminusd1", new BigDecimal("0.1", MAX_PRECISION).subtract(new BigDecimal("0.0001", MAX_PRECISION), MAX_PRECISION));
		check("dminusj", new BigDecimal("0.1", MAX_PRECISION).subtract(new BigDecimal(100, MAX_PRECISION), MAX_PRECISION));
		check("jminusd", new BigDecimal(100, MAX_PRECISION).subtract(new BigDecimal("0.1", MAX_PRECISION), MAX_PRECISION));
		check("dminusm", new BigDecimal("0.1", MAX_PRECISION).subtract(new BigDecimal(Long.valueOf(Integer.MAX_VALUE), MAX_PRECISION), MAX_PRECISION));
		check("mminusd", new BigDecimal(Long.valueOf(Integer.MAX_VALUE), MAX_PRECISION).subtract(new BigDecimal("0.1", MAX_PRECISION), MAX_PRECISION));
		check("dminusn", new BigDecimal("0.1", MAX_PRECISION).subtract(new BigDecimal(0.1D, MAX_PRECISION), MAX_PRECISION));
		check("nminusd", new BigDecimal(0.1D, MAX_PRECISION).subtract(new BigDecimal("0.1", MAX_PRECISION), MAX_PRECISION));
	}

	public void test_operator_multiply() {
		doCompile("test_operator_multiply");

		check("itimesj", 10 * 100);
		check("ltimesm", Long.valueOf(Integer.MAX_VALUE) * (Long.valueOf(Integer.MAX_VALUE / 10)));
		check("mtimesl", getVariable("ltimesm"));
		check("mtimesi", Long.valueOf(Integer.MAX_VALUE) * 10);
		check("itimesm", getVariable("mtimesi"));
		check("ntimesm1", Double.valueOf(0.1D * 0.001D));
		check("ntimesj", Double.valueOf(0.1) * 100);
		check("jtimesn", getVariable("ntimesj"));
		check("m1timesm", Double.valueOf(0.001d * Long.valueOf(Integer.MAX_VALUE)));
		check("mtimesm1", getVariable("m1timesm"));
		check("dtimesd1", new BigDecimal("0.1", MAX_PRECISION).multiply(new BigDecimal("0.0001", MAX_PRECISION), MAX_PRECISION));
		check("dtimesj", new BigDecimal("0.1", MAX_PRECISION).multiply(new BigDecimal(100, MAX_PRECISION)));
		check("jtimesd", getVariable("dtimesj"));
		check("dtimesm", new BigDecimal("0.1", MAX_PRECISION).multiply(new BigDecimal(Long.valueOf(Integer.MAX_VALUE), MAX_PRECISION), MAX_PRECISION));
		check("mtimesd", getVariable("dtimesm"));
		check("dtimesn", new BigDecimal("0.1", MAX_PRECISION).multiply(new BigDecimal(0.1, MAX_PRECISION), MAX_PRECISION));
		check("ntimesd", getVariable("dtimesn"));
	}

	public void test_operator_divide() {
		doCompile("test_operator_divide");

		check("idividej", 10 / 100);
		check("ldividem", Long.valueOf(Integer.MAX_VALUE / 10) / Long.valueOf(Integer.MAX_VALUE));
		check("mdividei", Long.valueOf(Integer.MAX_VALUE / 10));
		check("idividem", 10 / Long.valueOf(Integer.MAX_VALUE));
		check("ndividem1", Double.valueOf(0.1D / 0.001D));
		check("ndividej", Double.valueOf(0.1D / 100));
		check("jdividen", Double.valueOf(100 / 0.1D));
		check("m1dividem", Double.valueOf(0.001D / Long.valueOf(Integer.MAX_VALUE)));
		check("mdividem1", Double.valueOf(Long.valueOf(Integer.MAX_VALUE) / 0.001D));
		check("ddivided1", new BigDecimal("0.1", MAX_PRECISION).divide(new BigDecimal("0.0001", MAX_PRECISION), MAX_PRECISION));
		check("ddividej", new BigDecimal("0.1", MAX_PRECISION).divide(new BigDecimal(100, MAX_PRECISION), MAX_PRECISION));
		check("jdivided", new BigDecimal(100, MAX_PRECISION).divide(new BigDecimal("0.1", MAX_PRECISION), MAX_PRECISION));
		check("ddividem", new BigDecimal("0.1", MAX_PRECISION).divide(new BigDecimal(Long.valueOf(Integer.MAX_VALUE), MAX_PRECISION), MAX_PRECISION));
		check("mdivided", new BigDecimal(Long.valueOf(Integer.MAX_VALUE), MAX_PRECISION).divide(new BigDecimal("0.1", MAX_PRECISION), MAX_PRECISION));
		check("ddividen", new BigDecimal("0.1", MAX_PRECISION).divide(new BigDecimal(0.1D, MAX_PRECISION), MAX_PRECISION));
		check("ndivided", new BigDecimal(0.1D, MAX_PRECISION).divide(new BigDecimal("0.1", MAX_PRECISION), MAX_PRECISION));
	}
	
	public void test_operator_modulus() {
		doCompile("test_operator_modulus");
		check("imoduloj", 10 % 100);
		check("lmodulom", Long.valueOf(Integer.MAX_VALUE / 10) % Long.valueOf(Integer.MAX_VALUE));
		check("mmoduloi", Long.valueOf(Integer.MAX_VALUE % 10));
		check("imodulom", 10 % Long.valueOf(Integer.MAX_VALUE));
		check("nmodulom1", Double.valueOf(0.1D % 0.001D));
		check("nmoduloj", Double.valueOf(0.1D % 100));
		check("jmodulon", Double.valueOf(100 % 0.1D));
		check("m1modulom", Double.valueOf(0.001D % Long.valueOf(Integer.MAX_VALUE)));
		check("mmodulom1", Double.valueOf(Long.valueOf(Integer.MAX_VALUE) % 0.001D));
		check("dmodulod1", new BigDecimal("0.1", MAX_PRECISION).remainder(new BigDecimal("0.0001", MAX_PRECISION), MAX_PRECISION));
		check("dmoduloj", new BigDecimal("0.1", MAX_PRECISION).remainder(new BigDecimal(100, MAX_PRECISION), MAX_PRECISION));
		check("jmodulod", new BigDecimal(100, MAX_PRECISION).remainder(new BigDecimal("0.1", MAX_PRECISION), MAX_PRECISION));
		check("dmodulom", new BigDecimal("0.1", MAX_PRECISION).remainder(new BigDecimal(Long.valueOf(Integer.MAX_VALUE), MAX_PRECISION), MAX_PRECISION));
		check("mmodulod", new BigDecimal(Long.valueOf(Integer.MAX_VALUE), MAX_PRECISION).remainder(new BigDecimal("0.1", MAX_PRECISION), MAX_PRECISION));
		check("dmodulon", new BigDecimal("0.1", MAX_PRECISION).remainder(new BigDecimal(0.1D, MAX_PRECISION), MAX_PRECISION));
		check("nmodulod", new BigDecimal(0.1D, MAX_PRECISION).remainder(new BigDecimal("0.1", MAX_PRECISION), MAX_PRECISION));
	}
	
	public void test_operators_unary() {
		doCompile("test_operators_unary");

		// postfix operators
		// int
		check("intPlusOrig", Integer.valueOf(10));
		check("intPlusPlus", Integer.valueOf(10));
		check("intPlus", Integer.valueOf(11));
		check("intMinusOrig", Integer.valueOf(10));
		check("intMinusMinus", Integer.valueOf(10));
		check("intMinus", Integer.valueOf(9));
		// long
		check("longPlusOrig", Long.valueOf(10));
		check("longPlusPlus", Long.valueOf(10));
		check("longPlus", Long.valueOf(11));
		check("longMinusOrig", Long.valueOf(10));
		check("longMinusMinus", Long.valueOf(10));
		check("longMinus", Long.valueOf(9));
		// double
		check("numberPlusOrig", Double.valueOf(10.1));
		check("numberPlusPlus", Double.valueOf(10.1));
		check("numberPlus", Double.valueOf(11.1));
		check("numberMinusOrig", Double.valueOf(10.1));
		check("numberMinusMinus", Double.valueOf(10.1));
		check("numberMinus", Double.valueOf(9.1));
		// decimal
		check("decimalPlusOrig", new BigDecimal("10.1"));
		check("decimalPlusPlus", new BigDecimal("10.1"));
		check("decimalPlus", new BigDecimal("11.1"));
		check("decimalMinusOrig", new BigDecimal("10.1"));
		check("decimalMinusMinus", new BigDecimal("10.1"));
		check("decimalMinus", new BigDecimal("9.1"));
		// prefix operators
		// integer
		check("plusIntOrig", Integer.valueOf(10));
		check("plusPlusInt", Integer.valueOf(11));
		check("plusInt", Integer.valueOf(11));
		check("minusIntOrig", Integer.valueOf(10));
		check("minusMinusInt", Integer.valueOf(9));
		check("minusInt", Integer.valueOf(9));
		check("unaryInt", Integer.valueOf(-10));
		// long
		check("plusLongOrig", Long.valueOf(10));
		check("plusPlusLong", Long.valueOf(11));
		check("plusLong", Long.valueOf(11));
		check("minusLongOrig", Long.valueOf(10));
		check("minusMinusLong", Long.valueOf(9));
		check("minusLong", Long.valueOf(9));
		check("unaryLong", Long.valueOf(-10));
		// double
		check("plusNumberOrig", Double.valueOf(10.1));
		check("plusPlusNumber", Double.valueOf(11.1));
		check("plusNumber", Double.valueOf(11.1));
		check("minusNumberOrig", Double.valueOf(10.1));
		check("minusMinusNumber", Double.valueOf(9.1));
		check("minusNumber", Double.valueOf(9.1));
		check("unaryNumber", Double.valueOf(-10.1));
		// decimal
		check("plusDecimalOrig", new BigDecimal("10.1"));
		check("plusPlusDecimal", new BigDecimal("11.1"));
		check("plusDecimal", new BigDecimal("11.1"));
		check("minusDecimalOrig", new BigDecimal("10.1"));
		check("minusMinusDecimal", new BigDecimal("9.1"));
		check("minusDecimal", new BigDecimal("9.1"));
		check("unaryDecimal", new BigDecimal("-10.1"));
		
		// record values
		assertEquals(101, ((DataRecord) getVariable("plusPlusRecord")).getField("Value").getValue());
		assertEquals(101, ((DataRecord) getVariable("recordPlusPlus")).getField("Value").getValue());
		assertEquals(101, ((DataRecord) getVariable("modifiedPlusPlusRecord")).getField("Value").getValue());
		assertEquals(101, ((DataRecord) getVariable("modifiedRecordPlusPlus")).getField("Value").getValue());
		
		//record as parameter
		assertEquals(99, ((DataRecord) getVariable("minusMinusRecord")).getField("Value").getValue());
		assertEquals(99, ((DataRecord) getVariable("recordMinusMinus")).getField("Value").getValue());
		assertEquals(99, ((DataRecord) getVariable("modifiedMinusMinusRecord")).getField("Value").getValue());
		assertEquals(99, ((DataRecord) getVariable("modifiedRecordMinusMinus")).getField("Value").getValue());
		
		// logical not
		check("booleanValue", true);
		check("negation", false);
		check("doubleNegation", true);
	}
	
	public void test_operators_unary_record() {
		doCompileExpectErrors("test_operators_unary_record", Arrays.asList(
				"Illegal argument to ++/-- operator",
				"Illegal argument to ++/-- operator",
				"Illegal argument to ++/-- operator",
				"Illegal argument to ++/-- operator"));
	}
	
	public void test_operator_equal() {
		doCompile("test_operator_equal");

		check("eq0", true);
		check("eq1", true);
		check("eq2", true);
		check("eq3", true);
		check("eq4", true);
		check("eq5", true);
		check("eq6", false);
		check("eq7", true);
		check("eq8", false);
		check("eq9", true);
		check("eq10", false);
	}
	
	public void test_operator_non_equal(){
		doCompile("test_operator_non_equal");
		check("inei", false);
		check("inej", true);
		check("jnei", true);
		check("jnej", false);
		check("lnei", false);
		check("inel", false);
		check("lnej", true);
		check("jnel", true);
		check("lnel", false);
		check("dnei", false);
		check("ined", false);
		check("dnej", true);
		check("jned", true);
		check("dnel", false);
		check("lned", false);
		check("dned", false);
	}
	
	public void test_operator_in() {
		doCompile("test_operator_in");

		check("a", Integer.valueOf(1));
		check("haystack", Collections.EMPTY_LIST);
		check("needle", Integer.valueOf(2));
		check("b1", true);
		check("b2", false);
		check("h2", Arrays.asList(2.1D, 2.0D, 2.2D));
		check("b3", true);
		check("h3", Arrays.asList("memento", "mori", "memento mori"));
		check("n3", "memento mori");
		check("b4", true);
	}
	
	public void test_operator_greater_less() {
		doCompile("test_operator_greater_less");

		check("eq1", true);
		check("eq2", true);
		check("eq3", true);
		check("eq4", false);
		check("eq5", true);
		check("eq6", false);
		check("eq7", true);
		check("eq8", true);
		check("eq9", true);
	}
	
	public void test_operator_ternary(){
		doCompile("test_operator_ternary");

		// simple use
		check("trueValue", true);
		check("falseValue", false);
		check("res1", Integer.valueOf(1));
		check("res2", Integer.valueOf(2));
		// nesting in positive branch
		check("res3", Integer.valueOf(1));
		check("res4", Integer.valueOf(2));
		check("res5", Integer.valueOf(3));
		// nesting in negative branch
		check("res6", Integer.valueOf(2));
		check("res7", Integer.valueOf(3));
		// nesting in both branches
		check("res8", Integer.valueOf(1));
		check("res9", Integer.valueOf(1));
		check("res10", Integer.valueOf(2));
		check("res11", Integer.valueOf(3));
		check("res12", Integer.valueOf(2));
		check("res13", Integer.valueOf(4));
		check("res14", Integer.valueOf(3));
		check("res15", Integer.valueOf(4));
	}
	
	public void test_operators_logical(){
		doCompile("test_operators_logical");
		//TODO: please double check this.
		check("res1", false);
		check("res2", false);
		check("res3", true);
		check("res4", true);
		check("res5", false);
		check("res6", false);
		check("res7", true);
		check("res8", false);
	}
	
	public void test_regex(){
		doCompile("test_regex");
		check("eq0", false);
		check("eq1", true);
		check("eq2", false);
		check("eq3", true);
		check("eq4", false);
		check("eq5", true);
	}
	
	public void test_if() {
		doCompile("test_if");

		// if with single statement
		check("cond1", true);
		check("res1", true);

		// if with mutliple statements (block)
		check("cond2", true);
		check("res21", true);
		check("res22", true);

		// else with single statement
		check("cond3", false);
		check("res31", false);
		check("res32", true);

		// else with multiple statements (block)
		check("cond4", false);
		check("res41", false);
		check("res42", true);
		check("res43", true);

		// if with block, else with block
		check("cond5", false);
		check("res51", false);
		check("res52", false);
		check("res53", true);
		check("res54", true);

		// else-if with single statement
		check("cond61", false);
		check("cond62", true);
		check("res61", false);
		check("res62", true);

		// else-if with multiple statements
		check("cond71", false);
		check("cond72", true);
		check("res71", false);
		check("res72", true);
		check("res73", true);

		// if-elseif-else test
		check("cond81", false);
		check("cond82", false);
		check("res81", false);
		check("res82", false);
		check("res83", true);

		// if with single statement + inactive else
		check("cond9", true);
		check("res91", true);
		check("res92", false);

		// if with multiple statements + inactive else with block
		check("cond10", true);
		check("res101", true);
		check("res102", true);
		check("res103", false);
		check("res104", false);

		// if with condition
		check("i", 0);
		check("j", 1);
		check("res11", true);
	}
	
	public void test_switch() {
		doCompile("test_switch");
		// simple switch
		check("cond1", 1);
		check("res11", false);
		check("res12", true);
		check("res13", false);

		// switch, no break
		check("cond2", 1);
		check("res21", false);
		check("res22", true);
		check("res23", true);

		// default branch
		check("cond3", 3);
		check("res31", false);
		check("res32", false);
		check("res33", true);

		// no default branch => no match
		check("cond4", 3);
		check("res41", false);
		check("res42", false);
		check("res43", false);

		// multiple statements in a single case-branch
		check("cond5", 1);
		check("res51", false);
		check("res52", true);
		check("res53", true);
		check("res54", false);

		// single statement shared by several case labels
		check("cond6", 1);
		check("res61", false);
		check("res62", true);
		check("res63", true);
		check("res64", false);
	}
	
	public void test_int_switch(){
		doCompile("test_int_switch");
		
		// simple switch
		check("cond1", 1);
		check("res11", true);
		check("res12", false);
		check("res13", false);

		// first case is not followed by a break
		check("cond2", 1);
		check("res21", true);
		check("res22", true);
		check("res23", false);

		// first and second case have multiple labels
		check("cond3", 12);
		check("res31", false);
		check("res32", true);
		check("res33", false);

		// first and second case have multiple labels and no break after first group
		check("cond4", 11);
		check("res41", true);
		check("res42", true);
		check("res43", false);

		// default case intermixed with other case labels in the second group
		check("cond5", 11);
		check("res51", true);
		check("res52", true);
		check("res53", true);

		// default case intermixed, with break
		check("cond6", 16);
		check("res61", false);
		check("res62", true);
		check("res63", false);

		// continue test
		check("res7", Arrays.asList(
		/* i=0 */false, false, false,
		/* i=1 */true, true, false,
		/* i=2 */true, true, false,
		/* i=3 */false, true, false,
		/* i=4 */false, true, false,
		/* i=5 */false, false, true));

		// return test
		check("res8", Arrays.asList("0123", "123", "23", "3", "4", "3"));
	}
	
	public void test_non_int_switch(){
		doCompile("test_non_int_switch");
		
		// simple switch
		check("cond1", "1");
		check("res11", true);
		check("res12", false);
		check("res13", false);

		// first case is not followed by a break
		check("cond2", "1");
		check("res21", true);
		check("res22", true);
		check("res23", false);

		// first and second case have multiple labels
		check("cond3", "12");
		check("res31", false);
		check("res32", true);
		check("res33", false);

		// first and second case have multiple labels and no break after first group
		check("cond4", "11");
		check("res41", true);
		check("res42", true);
		check("res43", false);

		// default case intermixed with other case labels in the second group
		check("cond5", "11");
		check("res51", true);
		check("res52", true);
		check("res53", true);

		// default case intermixed, with break
		check("cond6", "16");
		check("res61", false);
		check("res62", true);
		check("res63", false);

		// continue test
		check("res7", Arrays.asList(
		/* i=0 */false, false, false,
		/* i=1 */true, true, false,
		/* i=2 */true, true, false,
		/* i=3 */false, true, false,
		/* i=4 */false, true, false,
		/* i=5 */false, false, true));

		// return test
		check("res8", Arrays.asList("0123", "123", "23", "3", "4", "3"));		
	}
	
	public void test_while() {
		doCompile("test_while");
		// simple while
		check("res1", Arrays.asList(0, 1, 2));
		// continue
		check("res2", Arrays.asList(0, 2));
		// break
		check("res3", Arrays.asList(0));
	}

	public void test_do_while() {
		doCompile("test_do_while");
		// simple while
		check("res1", Arrays.asList(0, 1, 2));
		// continue
		check("res2", Arrays.asList(0, null, 2));
		// break
		check("res3", Arrays.asList(0));
	}
	
	public void test_for() {
		doCompile("test_for");
		
		// simple loop
		check("res1", Arrays.asList(0,1,2));
		// continue
		check("res2", Arrays.asList(0,null,2));
		// break
		check("res3", Arrays.asList(0));
		// empty init
		check("res4", Arrays.asList(0,1,2));
		// empty update
		check("res5", Arrays.asList(0,1,2));
		// empty final condition
		check("res6", Arrays.asList(0,1,2));
		// all conditions empty
		check("res7", Arrays.asList(0,1,2));
	}
	
	public void test_foreach() {
		doCompile("test_foreach");
		check("intRes", Arrays.asList(VALUE_VALUE));
		check("longRes", Arrays.asList(BORN_MILLISEC_VALUE));
		check("doubleRes", Arrays.asList(AGE_VALUE));
		check("decimalRes", Arrays.asList(CURRENCY_VALUE));
		check("booleanRes", Arrays.asList(FLAG_VALUE));
		check("stringRes", Arrays.asList(NAME_VALUE, CITY_VALUE));
		check("dateRes", Arrays.asList(BORN_VALUE));
	}
	
	public void test_return(){
		doCompile("test_return");
		check("lhs", Integer.valueOf(1));
		check("rhs", Integer.valueOf(2));
		check("res", Integer.valueOf(3));
	}
	
	public void test_return_incorrect() {
		doCompileExpectError("test_return_incorrect", "Can't convert from 'string' to 'integer'");
	}
	
	public void test_overloading() {
		doCompile("test_overloading");
		check("res1", Integer.valueOf(3));
		check("res2", "Memento mori");
	}
	
	
	public void test_overloading_incorrect() {
		doCompileExpectErrors("test_overloading_incorrect", Arrays.asList(
				"Duplicate function 'integer sum(integer, integer)'",
				"Duplicate function 'integer sum(integer, integer)'"));
	}
	
	//Test case for 4038
	public void test_function_parameter_without_type() {
		doCompileExpectError("test_function_parameter_without_type", "Syntax error on token ')'");
	}
	
	public void test_duplicate_import() {
		URL importLoc = getClass().getResource("test_duplicate_import.ctl");
		String expStr = "import '" + importLoc + "';\n";
		expStr += "import '" + importLoc + "';\n";
		
		doCompile(expStr, "test_duplicate_import");
	}
	
	/*TODO:
	 * public void test_invalid_import() {
		URL importLoc = getClass().getResource("test_duplicate_import.ctl");
		String expStr = "import '/a/b/c/d/e/f/g/h/i/j/k/l/m';\n";
		expStr += expStr;
		
		doCompileExpectError(expStr, "test_invalid_import", Arrays.asList("TODO: Unknown error"));
		//doCompileExpectError(expStr, "test_duplicate_import", Arrays.asList("TODO: Unknown error"));
	}	 */

	
	public void test_built_in_functions(){
		doCompile("test_built_in_functions");
		
		check("notNullValue", Integer.valueOf(1));
		checkNull("nullValue");
		check("isNullRes1", false);
		check("isNullRes2", true);
		assertEquals("nvlRes1", getVariable("notNullValue"), getVariable("nvlRes1"));
		check("nvlRes2", Integer.valueOf(2));
		assertEquals("nvl2Res1", getVariable("notNullValue"), getVariable("nvl2Res1"));
		check("nvl2Res2", Integer.valueOf(2));
		check("iifRes1", Integer.valueOf(2));
		check("iifRes2", Integer.valueOf(1));
	}
	
	public void test_mapping(){
		doCompile("test_mapping");
		// simple mappings
		assertEquals("Name", NAME_VALUE, outputRecords[0].getField("Name").getValue().toString());
		assertEquals("Age", AGE_VALUE, outputRecords[0].getField("Age").getValue());
		assertEquals("City", CITY_VALUE, outputRecords[0].getField("City").getValue().toString());
		assertEquals("Born", BORN_VALUE, outputRecords[0].getField("Born").getValue());
		
		// * mapping
		assertTrue(recordEquals(inputRecords[1], outputRecords[1]));
	}
	
	public void test_sequence(){
		doCompile("test_sequence");
		check("intRes", Arrays.asList(1,2,3));
		check("longRes", Arrays.asList(Long.valueOf(1),Long.valueOf(2),Long.valueOf(3)));
		check("stringRes", Arrays.asList("1","2","3"));
		check("intCurrent", Integer.valueOf(3));
		check("longCurrent", Long.valueOf(3));
		check("stringCurrent", "3");
	}
	
	//TODO: If this test fails please double check whether the test is correct?
	public void test_lookup(){
        doCompile("test_lookup");
		check("alphaResult", Arrays.asList("Andorra la Vella","Andorra la Vella"));
		check("bravoResult", Arrays.asList("Bruxelles","Bruxelles"));
		check("charlieResult", Arrays.asList("Chamonix","Chomutov","Chamonix","Chomutov"));
		check("countResult", Arrays.asList(2,2));
	}
	
//------------------------- ContainerLib Tests---------------------
	
	public void test_containerlib_append() {
		doCompile("test_containerlib_append");
		
		check("appendElem", Integer.valueOf(10));
		check("appendList", Arrays.asList(1, 2, 3, 4, 5, 10));
	}

	@SuppressWarnings("unchecked")
	public void test_containerlib_clear() {
		doCompile("test_containerlib_clear");

		assertTrue(((List<Integer>) getVariable("clearList")).isEmpty());
	}
	
	public void test_containerlib_copy() {
		doCompile("test_containerlib_copy");

		check("copyList", Arrays.asList(1, 2, 3, 4, 5));
		check("returnedList", Arrays.asList(1, 2, 3, 4, 5));
	}

	public void test_containerlib_insert() {
		doCompile("test_containerlib_insert");

		check("insertElem", Integer.valueOf(7));
		check("insertIndex", Integer.valueOf(3));
		check("insertList", Arrays.asList(1, 2, 3, 7, 4, 5));
		check("insertList1", Arrays.asList(7, 8, 11, 10, 11));
		check("insertList2", Arrays.asList(7, 8, 10, 9, 11));
	}
	
	public void test_containerlib_isEmpty() {
		doCompile("test_containerlib_isEmpty");
		
		check("emptyMap", true);
		check("fullMap", false);
		check("emptyList", true);
		check("fullList", false);
	}

	public void test_containerlib_poll() {
		doCompile("test_containerlib_poll");

		check("pollElem", Integer.valueOf(1));
		check("pollList", Arrays.asList(2, 3, 4, 5));
	}

	public void test_containerlib_pop() {
		doCompile("test_containerlib_pop");

		check("popElem", Integer.valueOf(5));
		check("popList", Arrays.asList(1, 2, 3, 4));
	}

	public void test_containerlib_push() {
		doCompile("test_containerlib_push");

		check("pushElem", Integer.valueOf(6));
		check("pushList", Arrays.asList(1, 2, 3, 4, 5, 6));
	}

	public void test_containerlib_remove() {
		doCompile("test_containerlib_remove");

		check("removeElem", Integer.valueOf(3));
		check("removeIndex", Integer.valueOf(2));
		check("removeList", Arrays.asList(1, 2, 4, 5));
	}

	public void test_containerlib_reverse() {
		doCompile("test_containerlib_reverse");

		check("reverseList", Arrays.asList(5, 4, 3, 2, 1));
	}

	public void test_containerlib_sort() {
		doCompile("test_containerlib_sort");

		check("sortList", Arrays.asList(1, 1, 2, 3, 5));
	}
//---------------------- StringLib Tests ------------------------	
	
	public void test_stringlib_cache() {
		doCompile("test_stringlib_cache");
		check("rep1", "The cat says meow. All cats say meow.");
		check("rep2", "The cat says meow. All cats say meow.");
		check("rep3", "The cat says meow. All cats say meow.");
		
		check("find1", Arrays.asList("to", "to", "to", "tro", "to"));
		check("find2", Arrays.asList("to", "to", "to", "tro", "to"));
		check("find3", Arrays.asList("to", "to", "to", "tro", "to"));
		
		check("split1", Arrays.asList("one", "two", "three", "four", "five"));
		check("split2", Arrays.asList("one", "two", "three", "four", "five"));
		check("split3", Arrays.asList("one", "two", "three", "four", "five"));
		
		check("chop01", "ting soming choping function");
		check("chop02", "ting soming choping function");
		check("chop03", "ting soming choping function");
		
		check("chop11", "testing end of lines cutting");
		check("chop12", "testing end of lines cutting");
		
	}
	
	public void test_stringlib_charAt() {
		doCompile("test_stringlib_charAt");
		String input = "The QUICk !!$  broWn fox 	juMPS over the lazy DOG	";
		String[] expected = new String[input.length()];
		for (int i = 0; i < expected.length; i++) {
			expected[i] = String.valueOf(input.charAt(i));
		}
		check("chars", Arrays.asList(expected));
	}
	
	public void test_stringlib_concat() {
		doCompile("test_stringlib_concat");
		
		final SimpleDateFormat format = new SimpleDateFormat();
		format.applyPattern("yyyy MMM dd");
				
		check("concat", "");
		check("concat1", "ello hi   ELLO 2,today is " + format.format(new Date()));
	}
	
	public void test_stringlib_countChar() {
		doCompile("test_stringlib_countChar");
		check("charCount", 3);
	}
	
	public void test_stringlib_cut() {
		doCompile("test_stringlib_cut");
		check("cutInput", Arrays.asList("a", "1edf", "h3ijk"));
	}
	
	public void test_stringlib_editDistance() {
		doCompile("test_stringlib_editDistance");
		check("dist", 1);
		check("dist1", 1);
		check("dist2", 0);
		check("dist5", 1);
		check("dist3", 1);
		check("dist4", 0);
		check("dist6", 4);
		check("dist7", 5);
		check("dist8", 0);
		check("dist9", 0);
	}
	
	public void test_stringlib_find() {
		doCompile("test_stringlib_find");
		check("findList", Arrays.asList("The quick br", "wn f", "x jumps ", "ver the lazy d", "g"));
	}
	
	public void test_stringlib_join() {
		doCompile("test_stringlib_join");
		//check("joinedString", "Bagr,3,3.5641,-87L,CTL2");
		check("joinedString1", "3=0.1\"80=5455.987\"-5=5455.987");
		check("joinedString2", "5.0♫54.65♫67.0♫231.0");
		//check("joinedString3", "5☺54☺65☺67☺231☺80=5455.987☺-5=5455.987☺3=0.1☺CTL2☺42");
	}
	
	public void test_stringlib_left() {
		doCompile("test_stringlib_left");
		check("lef", "The q");
	}
	
	public void test_stringlib_length() {
		doCompile("test_stringlib_length");
		check("lenght1", new BigDecimal(50));
		check("lenghtByte", 18);
		
		check("stringLength", 8);
		check("listLength", 8);
		check("mapLength", 3);
		check("recordLength", 9);
	}
	
	public void test_stringlib_lowerCase() {
		doCompile("test_stringlib_lowerCase");
		check("lower", "the quick !!$  brown fox jumps over the lazy dog bagr  ");
	}
	
	public void test_stringlib_matches() {
		doCompile("test_stringlib_matches");
		check("matches1", true);
		check("matches2", true);
		check("matches3", false);
		check("matches4", true);
		check("matches5", false);
	}
	
	public void test_stringlib_metaphone() {
		doCompile("test_stringlib_metaphone");
		check("metaphone1", "XRS");
		check("metaphone2", "KWNTLN");
		check("metaphone3", "KWNT");
	}
	
	public void test_stringlib_nysiis() {
		doCompile("test_stringlib_nysiis");
		check("nysiis1", "CAP");
		check("nysiis2", "CAP");
	}
	
	public void test_stringlib_replace() {
		doCompile("test_stringlib_replace");
		
		final SimpleDateFormat format = new SimpleDateFormat();
		format.applyPattern("yyyy MMM dd");
		
		check("rep", format.format(new Date()).replaceAll("[lL]", "t"));
		check("rep1", "The cat says meow. All cats say meow.");
		
	}
	
	public void test_stringlib_right() {
		doCompile("test_stringlib_right");
		check("righ", "y dog");
	}
	
	public void test_stringlib_soundex() {
		doCompile("test_stringlib_soundex");
		check("soundex1", "W630");
		check("soundex2", "W643");
	}
	
	public void test_stringlib_split() {
		doCompile("test_stringlib_split");
		check("split1", Arrays.asList("The quick br", "wn f", "", " jumps " , "ver the lazy d", "g"));
	}
	
	public void test_stringlib_substring() {
		doCompile("test_stringlib_substring");
		check("subs", "UICk ");
	}
	
	public void test_stringlib_trim() {
		doCompile("test_stringlib_trim");
		check("trim1", "im  The QUICk !!$  broWn fox juMPS over the lazy DOG");
	}
	
	public void test_stringlib_upperCase() {
		doCompile("test_stringlib_upperCase");
		check("upper", "THE QUICK !!$  BROWN FOX JUMPS OVER THE LAZY DOG BAGR	");
	}

	public void test_stringlib_isFormat() {
		doCompile("test_stringlib_isFormat");
		check("test", "test");
		check("isBlank", Boolean.FALSE);
		check("blank", "");
		checkNull("nullValue");
		check("isBlank2", true);
		check("isAscii1", true);
		check("isAscii2", false);
		check("isNumber", false);
		check("isNumber1", false);
		check("isNumber2", true);
		check("isNumber3", true);
		check("isNumber4", false);
		check("isNumber5", true);
		check("isNumber6", true);
		check("isInteger", false);
		check("isInteger1", false);
		check("isInteger2", false);
		check("isInteger3", true);
		check("isLong", true);
		check("isLong1", false);
		check("isLong2", false);
		check("isDate", true);
		check("isDate1", false);
		// "kk" allows hour to be 1-24 (as opposed to HH allowing hour to be 0-23)
		check("isDate2", true);
		check("isDate3", true);
		check("isDate4", false);
		check("isDate5", true);
		check("isDate6", true);
		check("isDate7", false);
		// illegal month: 15
		check("isDate9", false);
		check("isDate10", false);
		check("isDate11", false);
		check("isDate12", true);
		check("isDate13", false);
		// 24 is an illegal value for pattern HH (it allows only 0-23)
		check("isDate14", false);
		// empty string: invalid
		check("isDate15", false);
	}
	
	public void test_stringlib_removeBlankSpace() {
		String expStr = 
			"string r1;\n" +
			"function integer transform() {\n" +
				"r1=removeBlankSpace(\"" + StringUtils.specCharToString(" a	b\nc\rd   e \u000Cf\r\n") +	"\");\n" +
				"printErr(r1);\n" +
				"return 0;\n" +
			"}\n";
		doCompile(expStr, "test_removeBlankSpace");
		check("r1", "abcdef");
	}
	
	public void test_stringlib_removeNonPrintable() {
		doCompile("test_stringlib_removeNonPrintable");
		check("nonPrintableRemoved", "AHOJ");
	}
	
	public void test_stringlib_getAlphanumericChars() {
		String expStr = 
			"string an1;\n" +
			"string an2;\n" +
			"string an3;\n" +
			"string an4;\n" +
			"function integer transform() {\n" +
				"an1=getAlphanumericChars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\");\n" +
				"printErr(an1);\n" +
				"an2=getAlphanumericChars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\",true,true);\n" +
				"printErr(an2);\n" +
				"an3=getAlphanumericChars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\",true,false);\n" +
				"printErr(an3);\n" +
				"an4=getAlphanumericChars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\",false,true);\n" +
				"printErr(an4);\n" +
				"return 0;\n" +
			"}\n";
		doCompile(expStr, "test_getAlphanumericChars");

		check("an1", "a1bcde2f");
		check("an2", "a1bcde2f");
		check("an3", "abcdef");
		check("an4", "12");
	}
	
	public void test_stringlib_indexOf(){
        doCompile("test_stringlib_indexOf");
        check("index",2);
		check("index1",9);
		check("index2",0);
		check("index3",-1);
		check("index4",6);
	}
	
	public void test_stringlib_removeDiacritic(){
        doCompile("test_stringlib_removeDiacritic");
        check("test","tescik");
    	check("test1","zabicka");
	}
	
	public void test_stringlib_translate(){
        doCompile("test_stringlib_translate");
        check("trans","hippi");
		check("trans1","hipp");
		check("trans2","hippi");
		check("trans3","");
		check("trans4","y lanuaX nXXd thX lXttXr X");
	}
    
	public void test_stringlib_chop() {
		doCompile("test_stringlib_chop");
		check("s1", "hello");
		check("s6", "hello");
		check("s5", "hello");
		check("s2", "hello");
		check("s7", "helloworld");
		check("s3", "hello ");
		check("s4", "hello");
	}
	
//-------------------------- MathLib Tests ------------------------
	public void test_bitwise_or() {
		doCompile("test_bitwise_or");
		check("resultInt1", 1);
		check("resultInt2", 1);
		check("resultInt3", 3);
		check("resultInt4", 3);
		check("resultLong1", 1l);
		check("resultLong2", 1l);
		check("resultLong3", 3l);
		check("resultLong4", 3l);
	}

	public void test_bitwise_and() {
		doCompile("test_bitwise_and");
		check("resultInt1", 0);
		check("resultInt2", 1);
		check("resultInt3", 0);
		check("resultInt4", 1);
		check("resultLong1", 0l);
		check("resultLong2", 1l);
		check("resultLong3", 0l);
		check("resultLong4", 1l);
	}

	public void test_bitwise_xor() {
		doCompile("test_bitwise_xor");
		check("resultInt1", 1);
		check("resultInt2", 0);
		check("resultInt3", 3);
		check("resultInt4", 2);
		check("resultLong1", 1l);
		check("resultLong2", 0l);
		check("resultLong3", 3l);
		check("resultLong4", 2l);
	}

	public void test_bitwise_lshift() {
		doCompile("test_bitwise_lshift");
		check("resultInt1", 2);
		check("resultInt2", 4);
		check("resultInt3", 10);
		check("resultInt4", 20);
		check("resultLong1", 2l);
		check("resultLong2", 4l);
		check("resultLong3", 10l);
		check("resultLong4", 20l);
	}

	public void test_bitwise_rshift() {
		doCompile("test_bitwise_rshift");
		check("resultInt1", 2);
		check("resultInt2", 0);
		check("resultInt3", 4);
		check("resultInt4", 2);
		check("resultLong1", 2l);
		check("resultLong2", 0l);
		check("resultLong3", 4l);
		check("resultLong4", 2l);
	}

	public void test_bitwise_negate() {
		doCompile("test_bitwise_negate");
		check("resultInt", -59081717);
		check("resultLong", -3321654987654105969L);
	}

	public void test_set_bit() {
		doCompile("test_set_bit");
		check("resultInt1", 0x2FF);
		check("resultInt2", 0xFB);
		check("resultLong1", 0x4000000000000l);
		check("resultLong2", 0xFFDFFFFFFFFFFFFl);
		check("resultBool1", true);
		check("resultBool2", false);
		check("resultBool3", true);
		check("resultBool4", false);
	}	
	
	public void test_mathlib_abs() {
		doCompile("test_mathlib_abs");
		check("absIntegerPlus", new Integer(10));
		check("absIntegerMinus", new Integer(1));
		check("absLongPlus", new Long(10));
		check("absLongMinus", new Long(1));
		check("absDoublePlus", new Double(10.0));
		check("absDoubleMinus", new Double(1.0));
		check("absDecimalPlus", new BigDecimal(5.0));
		check("absDecimalMinus", new BigDecimal(5.0));
	}
	
	public void test_mathlib_ceil() {
		doCompile("test_mathlib_ceil");
		check("ceil1", -3.0);
		
		check("intResult", Arrays.asList(2.0, 3.0));
		check("longResult", Arrays.asList(2.0, 3.0));
		check("doubleResult", Arrays.asList(3.0, -3.0));
		check("decimalResult", Arrays.asList(3.0, -3.0));
	}
	
	public void test_mathlib_e() {
		doCompile("test_mathlib_e");
		check("varE", Math.E);
	}
	
	public void test_mathlib_exp() {
		doCompile("test_mathlib_exp");
		check("ex", Math.exp(1.123));
	}
	
	public void test_mathlib_floor() {
		doCompile("test_mathlib_floor");
		check("floor1", -4.0);
		
		check("intResult", Arrays.asList(2.0, 3.0));
		check("longResult", Arrays.asList(2.0, 3.0));
		check("doubleResult", Arrays.asList(2.0, -4.0));
		check("decimalResult", Arrays.asList(2.0, -4.0));
	}
	
	public void test_mathlib_log() {
		doCompile("test_mathlib_log");
		check("ln", Math.log(3));
	}
	
	public void test_mathlib_log10() {
		doCompile("test_mathlib_log10");
		check("varLog10", Math.log10(3));
	}
	
	public void test_mathlib_pi() {
		doCompile("test_mathlib_pi");
		check("varPi", Math.PI);
	}
	
	public void test_mathlib_pow() {
		doCompile("test_mathlib_pow");
		check("power1", Math.pow(3,1.2));
		check("power2", Double.NaN);
		
		check("intResult", Arrays.asList(8d, 8d, 8d, 8d));
		check("longResult", Arrays.asList(8d, 8d, 8d, 8d));
		check("doubleResult", Arrays.asList(8d, 8d, 8d, 8d));
		check("decimalResult", Arrays.asList(8d, 8d, 8d, 8d));
	}
	
	public void test_mathlib_round() {
		doCompile("test_mathlib_round");
		check("round1", -4l);
		
		check("intResult", Arrays.asList(2l, 3l));
		check("longResult", Arrays.asList(2l, 3l));
		check("doubleResult", Arrays.asList(2l, 4l));
		check("decimalResult", Arrays.asList(2l, 4l));
	}
	
	public void test_mathlib_sqrt() {
		doCompile("test_mathlib_sqrt");
		check("sqrtPi", Math.sqrt(Math.PI));
		check("sqrt9", Math.sqrt(9));
	}

//-------------------------DateLib tests----------------------
	
	public void test_datelib_cache() {
		doCompile("test_datelib_cache");
		
		check("b11", true);
		check("b12", true);
		check("b21", true);
		check("b22", true);
		check("b31", true);
		check("b32", true);
		check("b41", true);
		check("b42", true);		
		
		checkEquals("date3", "date3d");
		checkEquals("date4", "date4d");
		checkEquals("date7", "date7d");
		checkEquals("date8", "date8d");
	}
	
	public void test_datelib_trunc() {
		doCompile("test_datelib_trunc");
		check("truncDate", new GregorianCalendar(2004, 00, 02).getTime());
	}
	
	public void test_datelib_truncDate() {
		doCompile("test_datelib_truncDate");
		Calendar cal = Calendar.getInstance();
		cal.setTime(BORN_VALUE);
		int[] portion = new int[]{cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND),cal.get(Calendar.MILLISECOND)};
    	cal.clear();
    	cal.set(Calendar.HOUR_OF_DAY, portion[0]);
    	cal.set(Calendar.MINUTE, portion[1]);
    	cal.set(Calendar.SECOND, portion[2]);
    	cal.set(Calendar.MILLISECOND, portion[3]);
        check("truncBornDate", cal.getTime());
	}
	
	public void test_datelib_today() {
		doCompile("test_datelib_today");
		check("todayDate", new Date());
	}
	
	public void test_datelib_zeroDate() {
		doCompile("test_datelib_zeroDate");
		check("zeroDate", new Date(0));
	}
	
	public void test_datelib_dateDiff() {
		doCompile("test_datelib_dateDiff");
		Calendar cal = Calendar.getInstance();
		cal.setTime(BORN_VALUE);
		long diff = cal.get(Calendar.YEAR);
		cal.setTime(new Date());
		diff -= cal.get(Calendar.YEAR);
		check("ddiff", diff);
	}
	
	public void test_datelib_dateAdd() {
		doCompile("test_datelib_dateAdd");
		check("datum", new Date(BORN_MILLISEC_VALUE + 100));
	}
	
	public void test_datelib_extractTime() {
		doCompile("test_datelib_extractTime");
		Calendar cal = Calendar.getInstance();
		cal.setTime(BORN_VALUE);
		int[] portion = new int[]{cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND),cal.get(Calendar.MILLISECOND)};
    	cal.clear();
    	cal.set(Calendar.HOUR_OF_DAY, portion[0]);
    	cal.set(Calendar.MINUTE, portion[1]);
    	cal.set(Calendar.SECOND, portion[2]);
    	cal.set(Calendar.MILLISECOND, portion[3]);
    	check("bornExtractTime", cal.getTime());
	}
	
	public void test_datelib_extractDate() {
		doCompile("test_datelib_extractDate");
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(BORN_VALUE);
		int[] portion = new int[]{cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.MONTH), cal.get(Calendar.YEAR)};
    	cal.clear();
    	cal.set(Calendar.DAY_OF_MONTH, portion[0]);
    	cal.set(Calendar.MONTH, portion[1]);
    	cal.set(Calendar.YEAR, portion[2]);
    	check("bornExtractDate", cal.getTime());
	}
//-----------------Convert Lib tests-----------------------
	public void test_convertlib_cache() {
		// set default locale to en.US so the date is formatted uniformly on all systems
		Locale.setDefault(Locale.US);

		doCompile("test_convertlib_cache");
		Calendar cal = Calendar.getInstance();
		cal.set(2000, 6, 20, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);
		
		Date checkDate = cal.getTime();
		
		final SimpleDateFormat format = new SimpleDateFormat();
		format.applyPattern("yyyy MMM dd");
		
		check("sdate1", format.format(new Date()));
		check("sdate2", format.format(new Date()));
		
		check("date01", checkDate);
		check("date02", checkDate);
		check("date03", checkDate);
		check("date04", checkDate);
		check("date11", checkDate);
		check("date12", checkDate);
		check("date13", checkDate);
	}
	
	public void test_convertlib_base64byte() {
		doCompile("test_convertlib_base64byte");
		assertTrue(Arrays.equals((byte[])getVariable("base64input"), Base64.decode("The quick brown fox jumps over the lazy dog")));
	}
	
	public void test_convertlib_bits2str() {
		doCompile("test_convertlib_bits2str");
		check("bitsAsString1", "00000000");
		check("bitsAsString2", "11111111");
		check("bitsAsString3", "010100000100110110100000");
	}
	
	public void test_convertlib_bool2num() {
		doCompile("test_convertlib_bool2num");
		check("resultTrue", 1);
		check("resultFalse", 0);
	}
	
	public void test_convertlib_byte2base64() {
		doCompile("test_convertlib_byte2base64");
		check("inputBase64", Base64.encodeBytes("Abeceda zedla deda".getBytes()));
	}
	
	public void test_convertlib_byte2hex() {
		doCompile("test_convertlib_byte2hex");
		check("hexResult", "41626563656461207a65646c612064656461");
	}
	
	public void test_convertlib_date2long() {
		doCompile("test_convertlib_date2long");
		check("bornDate", BORN_MILLISEC_VALUE);
		check("zeroDate", 0l);
	}
	
	public void test_convertlib_date2num() {
		doCompile("test_convertlib_date2num");
		Calendar cal = Calendar.getInstance();
		cal.setTime(BORN_VALUE);
		check("yearDate", 1987);
		check("monthDate", 5);
		check("secondDate", 0);
		check("yearBorn", cal.get(Calendar.YEAR));
		check("monthBorn", cal.get(Calendar.MONTH) + 1); //Calendar enumerates months from 0, not 1;
		check("secondBorn", cal.get(Calendar.SECOND));
		check("yearMin", 1970);
		check("monthMin", 1);
		check("weekMin", 1);
		check("dayMin", 1);
		check("hourMin", 1); //TODO: check!
		check("minuteMin", 0);
		check("secondMin", 0);
		check("millisecMin", 0);
	}
	
	public void test_convertlib_date2str() {
		doCompile("test_convertlib_date2str");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy:MM:dd");
		check("inputDate", "1987:05:12");
		check("bornDate", sdf.format(BORN_VALUE));
	}
	
	public void test_convertlib_decimal2double() {
		doCompile("test_convertlib_decimal2double");
		check("toDouble", 0.007d);
	}
	
	public void test_convertlib_decimal2integer() {
		doCompile("test_convertlib_decimal2integer");
		check("toInteger", 0);
		check("toInteger2", -500);
		check("toInteger3", 1000000);
	}
	
	public void test_convertlib_decimal2long() {
		doCompile("test_convertlib_decimal2long");
		check("toLong", 0l);
		check("toLong2", -500l);
		check("toLong3", 10000000000l);
	}
	
	public void test_convertlib_double2integer() {
		doCompile("test_convertlib_double2integer");
		check("toInteger", 0);
		check("toInteger2", -500);
		check("toInteger3", 1000000);
	}
	
	public void test_convertlib_double2long() {
		doCompile("test_convertlib_double2long");
		check("toLong", 0l);
		check("toLong2", -500l);
		check("toLong3", 10000000000l);
	}

	public void test_convertlib_getFieldName() {
		doCompile("test_convertlib_getFieldName");
		check("fieldNames",Arrays.asList("Name", "Age", "City", "Born", "BornMillisec", "Value", "Flag", "ByteArray", "Currency"));
	}

	public void test_convertlib_getFieldType() {
		doCompile("test_convertlib_getFieldType");
		check("fieldTypes",Arrays.asList(DataFieldMetadata.STRING_TYPE, DataFieldMetadata.NUMERIC_TYPE, DataFieldMetadata.STRING_TYPE,
				DataFieldMetadata.DATE_TYPE, DataFieldMetadata.LONG_TYPE, DataFieldMetadata.INTEGER_TYPE, DataFieldMetadata.BOOLEAN_TYPE,
				DataFieldMetadata.BYTE_TYPE, DataFieldMetadata.DECIMAL_TYPE));
	}
	
	public void test_convertlib_hex2byte() {
		doCompile("test_convertlib_hex2byte");
		assertTrue(Arrays.equals((byte[])getVariable("fromHex"), BYTEARRAY_VALUE));
	}
	
	public void test_convertlib_long2date() {
		doCompile("test_convertlib_long2date");
		check("fromLong1", new Date(0));
		check("fromLong2", new Date(50000000000L));
		check("fromLong3", new Date(-5000L));
	}
	
	public void test_convertlib_long2integer() {
		doCompile("test_convertlib_long2integer");
		check("fromLong1", 10);
		check("fromLong2", -10);
	}
	
	
	public void test_convertlib_long2packDecimal() {
		doCompile("test_convertlib_long2packDecimal");
		assertTrue(Arrays.equals((byte[])getVariable("packedLong"), new byte[] {5, 0, 12}));
	}
	
	public void test_convertlib_md5() {
		doCompile("test_convertlib_md5");
		assertTrue(Arrays.equals((byte[])getVariable("md5Hash1"), Digest.digest(DigestType.MD5, "The quick brown fox jumps over the lazy dog")));
		assertTrue(Arrays.equals((byte[])getVariable("md5Hash2"), Digest.digest(DigestType.MD5, BYTEARRAY_VALUE)));
	}

	public void test_convertlib_num2bool() {
		doCompile("test_convertlib_num2bool");
		check("integerTrue", true);
		check("integerFalse", false);
		check("longTrue", true);
		check("longFalse", false);
		check("doubleTrue", true);
		check("doubleFalse", false);
		check("decimalTrue", true);
		check("decimalFalse", false);
	}
	
	public void test_convertlib_num2str() {
		System.out.println("num2str() test:");
		doCompile("test_convertlib_num2str");

		check("intOutput", Arrays.asList("16", "10000", "20", "10", "1.235E3", "12 350 001 Kcs"));
		check("longOutput", Arrays.asList("16", "10000", "20", "10", "1.235E13", "12 350 001 Kcs"));
		check("doubleOutput", Arrays.asList("16.16", "0x1.028f5c28f5c29p4", "1.23548E3", "12 350 001,1 Kcs"));
		check("decimalOutput", Arrays.asList("16.16", "1235.44", "12 350 001,1 Kcs"));
	}

	public void test_convertlib_packdecimal2long() {
		doCompile("test_convertlib_packDecimal2long");
		check("unpackedLong", PackedDecimal.parse(BYTEARRAY_VALUE));
	}

	public void test_convertlib_sha() {
		doCompile("test_convertlib_sha");
		assertTrue(Arrays.equals((byte[])getVariable("shaHash1"), Digest.digest(DigestType.SHA, "The quick brown fox jumps over the lazy dog")));
		assertTrue(Arrays.equals((byte[])getVariable("shaHash2"), Digest.digest(DigestType.SHA, BYTEARRAY_VALUE)));
	}

	public void test_convertlib_str2bits() {
		doCompile("test_convertlib_str2bits");
		//TODO: uncomment -> test will pass, but is that correct?
		assertTrue(Arrays.equals((byte[]) getVariable("textAsBits1"), new byte[] {0/*, 0, 0, 0, 0, 0, 0, 0*/}));
		assertTrue(Arrays.equals((byte[]) getVariable("textAsBits2"), new byte[] {-1/*, 0, 0, 0, 0, 0, 0, 0*/}));
		assertTrue(Arrays.equals((byte[]) getVariable("textAsBits3"), new byte[] {10, -78, 5/*, 0, 0, 0, 0, 0*/}));
	}

	public void test_convertlib_str2bool() {
		doCompile("test_convertlib_str2bool");
		check("fromTrueString", true);
		check("fromFalseString", false);
	}

	public void test_convertlib_str2date() {
		doCompile("test_convertlib_str2date");
		
		Calendar cal = Calendar.getInstance();
		cal.set(2050, 4, 19, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);
		
		Date checkDate = cal.getTime();
		
		check("date1", checkDate);
		check("date2", checkDate);
	}

	public void test_convertlib_str2decimal() {
		doCompile("test_convertlib_str2decimal");
		check("parsedDecimal1", new BigDecimal("100.13"));
		check("parsedDecimal2", new BigDecimal("123123123.123"));
		check("parsedDecimal3", new BigDecimal("-350000.01"));
	}

	public void test_convertlib_str2double() {
		doCompile("test_convertlib_str2double");
		check("parsedDouble1", 100.13);
		check("parsedDouble2", 123123123.123);
		check("parsedDouble3", -350000.01);
	}

	public void test_convertlib_str2integer() {
		doCompile("test_convertlib_str2integer");
		check("parsedInteger1", 123456789);
		check("parsedInteger2", 123123);
		check("parsedInteger3", -350000);
		check("parsedInteger4", 419);
	}

	public void test_convertlib_str2long() {
		doCompile("test_convertlib_str2long");
		check("parsedLong1", 1234567890123L);
		check("parsedLong2", 123123123456789L);
		check("parsedLong3", -350000L);
		check("parsedLong4", 133L);
	}

	public void test_convertlib_toString() {
		doCompile("test_convertlib_toString");
		check("integerString", "10");
		check("longString", "110654321874");
		check("doubleString", "1.547874E-14");
		check("decimalString", "-6847521431.1545874");
		check("listString", "[not ALI A, not ALI B, not ALI D..., but, ALI H!]");
		check("mapString", "{1=Testing, 2=makes, 3=me, 4=crazy :-)}");
	}
	
	public void test_confitional_fail() {
		doCompile("test_conditional_fail");
		check("result", 3);
	}
	
	
	public void test_expression_statement(){
		// test case for issue 4174
        doCompileExpectErrors("test_expression_statement", Arrays.asList("Syntax error, statement expected","Syntax error, statement expected"));
	}

}
