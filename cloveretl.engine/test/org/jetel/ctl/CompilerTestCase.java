package org.jetel.ctl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.AssertionFailedError;

import org.jetel.component.CTLRecordTransform;
import org.jetel.component.CTLRecordTransformAdapter;
import org.jetel.component.RecordTransform;
import org.jetel.ctl.ASTnode.CLVFStart;
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
	
	protected void checkNull(String varName) {
		assertNull(getVariable(varName));
	}

	protected void setUp() {
		initEngine();
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

		// duplicate entry lkpRecord.getField("Name").setValue("Charlie"); lkpRecord.getField("Value").setValue(3);
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
			expStr = "//#CTL:COMPILE\n" + expStr;
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
			expStr = "//#CTL:COMPILE\n" + expStr;
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

	protected static <E> List<E> createList(E... values) {
		final ArrayList<E> ret = new ArrayList<E>();
		for (int i = 0; i < values.length; i++) {
			ret.add(values[i]);
		}
		return ret;
	}

	
//----------------------------- TESTS -----------------------------

	//TODO test case for issue
	/*public void test_list_concatenate() {
		doCompile("test_list_concatenate");
	}*/

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

		String expStr = "/*#TL:COMPILED\n*/\n" + "// this is other comment\n" + "for (integer i=0; i<5; i++) \n" + "  if (i%2 == 0) {\n" + "		continue;\n" + "  }\n";
		TransformationGraph graph = createDefaultGraph();
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };

		print_code(expStr);
		ITLCompiler compiler = TLCompilerFactory.createCompiler(graph, inMetadata, outMetadata, "UTF-8");
		List<ErrorMessage> messages = compiler.compile(expStr, CTLRecordTransform.class, "parser_test");
		printMessages(messages);

		if (messages.size() > 0) {
			throw new AssertionFailedError("Error in execution. Check standard output for details.");
		}

		CLVFStart parseTree = compiler.getStart();
		parseTree.dump("");

	}

	public void test_import() {
		System.out.println("\nImport test:");

		URL importLoc = getClass().getResource("import.ctl");
		String expStr = "import '" + importLoc + "';\n";
		importLoc = getClass().getResource("other.ctl");
		expStr += "import '" + importLoc + "';\n";
		expStr += "if (a1) { a2++; };\n";

		TransformationGraph graph = createDefaultGraph();
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };

		print_code(expStr);
		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
		List<ErrorMessage> messages = compiler.validate(expStr);
		printMessages(messages);

		CLVFStart parseTree = compiler.getStart();
		parseTree.dump("");
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
		// "	print_err(computeSomething(10));\n" +
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

		TransformationGraph graph = createDefaultGraph();
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };

		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };

		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };

		print_code(expStr);
		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
		List<ErrorMessage> messages = compiler.compile(expStr, CTLRecordTransform.class, "test_scope");
		printMessages(messages);

		if (messages.size() > 0) {
			throw new AssertionFailedError("Error in execution. Check standard output for details.");
		}

		CTLRecordTransformAdapter executor = new CTLRecordTransformAdapter((TransformLangExecutor) compiler.getCompiledCode(), graph.getLogger());
		executor.init(null, inMetadata, outMetadata);
		executor.transform(inputRecords, outputRecords);

	}

//-------------------------- Data Types Tests ---------------------

	public void test_void() {
		doCompileExpectErrors("test_void", Arrays.asList("Syntax error on token 'void'",
				"Variable 'voidVar' is not declared",
				"Variable 'voidVar' is not declared",
				"Syntax error on token 'void'"));
	}
	
	public void test_int() {
		doCompile("test_int");
		check("i", 0);
		check("j", -1);
		check("field", VALUE_VALUE);
		checkNull("nullValue");
		check("varWithInitializer", 123);
		checkNull("varWithNullInitializer");
	}
	
	public void test_int_edge() {
		String testExpression = 
			"integer minInt;\n"+
			"integer maxInt;\n"+			
			"function integer transform() {\n" +
				"minInt=" + Integer.MIN_VALUE + ";\n" +
				"print_err(minInt, true);\n" + 
				"maxInt=" + Integer.MAX_VALUE + ";\n" +
				"print_err(maxInt, true);\n" + 
				"return 0;\n" +
			"}\n";
		doCompile(testExpression, "test_int_edge");
		check("minInt", Integer.MIN_VALUE);
		check("maxInt", Integer.MAX_VALUE);
	}
	
	public void test_long() {
		doCompile("test_long");
		check("i", Long.valueOf(0));
		check("j", Long.valueOf(-1));
		check("field", BORN_MILLISEC_VALUE);
		check("def", Long.valueOf(0));
		checkNull("nullValue");
		check("varWithInitializer", 123L);
		checkNull("varWithNullInitializer");
	}
	
	public void test_long_edge() {
		String expStr = 
			"long minLong;\n"+
			"long maxLong;\n"+			
			"function integer transform() {\n" +
				"minLong=" + (Long.MIN_VALUE) + "L;\n" +
				"print_err(minLong);\n" +
				"maxLong=" + (Long.MAX_VALUE) + "L;\n" +
				"print_err(maxLong);\n" +
				"return 0;\n" +
			"}\n";

		doCompile(expStr,"test_long_edge");
		check("minLong", Long.MIN_VALUE);
		check("maxLong", Long.MAX_VALUE);
	}
	
	public void test_decimal() {
		doCompile("test_decimal");
		check("i", new BigDecimal(0, MAX_PRECISION));
		check("j", new BigDecimal(-1, MAX_PRECISION));
		check("field", CURRENCY_VALUE);
		check("def", new BigDecimal(0, MAX_PRECISION));
		checkNull("nullValue");
		check("varWithInitializer", new BigDecimal("123.35", MAX_PRECISION));
		checkNull("varWithNullInitializer");
		check("varWithInitializerNoDist", new BigDecimal(123.35, MAX_PRECISION));
	}
	
	public void test_decimal_edge() {
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
				"print_err(minLong);\n" + 
				"maxLong=" + String.valueOf(Long.MAX_VALUE) + "d;\n" +
				"print_err(maxLong);\n" + 
				"minLongNoDist=" + String.valueOf(Long.MIN_VALUE) + "L;\n" +
				"print_err(minLongNoDist);\n" + 
				"maxLongNoDist=" + String.valueOf(Long.MAX_VALUE) + "L;\n" +
				"print_err(maxLongNoDist);\n" +
				// distincter will cause the double-string be parsed into exact representation within BigDecimal
				"minDouble=" + String.valueOf(Double.MIN_VALUE) + "D;\n" +
				"print_err(minDouble);\n" + 
				"maxDouble=" + String.valueOf(Double.MAX_VALUE) + "D;\n" +
				"print_err(maxDouble);\n" +
				// no distincter will cause the double-string to be parsed into inexact representation within double
				// then to be assigned into BigDecimal (which will extract only MAX_PRECISION digits)
				"minDoubleNoDist=" + String.valueOf(Double.MIN_VALUE) + ";\n" +
				"print_err(minDoubleNoDist);\n" + 
				"maxDoubleNoDist=" + String.valueOf(Double.MAX_VALUE) + ";\n" +
				"print_err(maxDoubleNoDist);\n" +
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
	
	public void test_number() {
		doCompile("test_number");
		
		check("i", Double.valueOf(0));
		check("j", Double.valueOf(-1));
		check("field", AGE_VALUE);
		check("def", Double.valueOf(0));
		checkNull("nullValue");
		checkNull("varWithNullInitializer");
	}
	
	public void test_number_edge() {
		String testExpression = 
			"number minDouble;\n" +
			"number maxDouble;\n"+		
			"function integer transform() {\n" +
				"minDouble=" + Double.MIN_VALUE + ";\n" +
				"print_err(minDouble);\n" +
				"maxDouble=" + Double.MAX_VALUE + ";\n" +
				"print_err(maxDouble);\n" +
				"return 0;\n" +
			"}\n";
		doCompile(testExpression, "test_number_edge");
		check("minDouble", Double.valueOf(Double.MIN_VALUE));
		check("maxDouble", Double.valueOf(Double.MAX_VALUE));
	}

	public void test_string() {
		doCompile("test_string");
		check("i","0");
		check("helloEscaped", "hello\\nworld");
		check("helloExpanded", "hello\nworld");
		check("fieldName", NAME_VALUE);
		check("fieldCity", CITY_VALUE);
		check("escapeChars", "a\u0101\u0102A");
		check("doubleEscapeChars", "a\u0101\u0102A");
		check("specialChars", "špeciálne značky s mäkčeňom môžu byť");
		check("dQescapeChars", "a\u0101\u0102A");
		//TODO:Is next test correct?
		check("dQdoubleEscapeChars", "a\u0101\u0102A");
		check("dQspecialChars", "špeciálne značky s mäkčeňom môžu byť");
		check("empty", "");
		check("def", "");
		checkNull("varWithNullInitializer");
}
	
	public void test_string_long() {
		int length = 1000;
		StringBuilder tmp = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			tmp.append(i % 10);
		}
		String testExpression = 
			"string longString;\n" +
			"function integer transform() {\n" +
				"longString=\"" + tmp + "\";\n" +
				"print_err(longString);\n" +
				"return 0;\n" +
			"}\n";
		doCompile(testExpression, "test_string_long");
		
		check("longString", String.valueOf(tmp));
	}
	
	public void test_date() {
		doCompile("test_date");
		check("d3", new GregorianCalendar(2006, GregorianCalendar.AUGUST, 1).getTime());
		check("d2", new GregorianCalendar(2006, GregorianCalendar.AUGUST, 2, 15, 15, 3).getTime());
		check("d1", new GregorianCalendar(2006, GregorianCalendar.JANUARY, 1, 1, 2, 3).getTime());
		check("field", BORN_VALUE);
		checkNull("nullValue");
		check("minValue", new GregorianCalendar(1970, GregorianCalendar.JANUARY, 1, 1, 0, 0).getTime());
		checkNull("varWithNullInitializer");
	}
	
	public void test_boolean() {
		doCompile("test_boolean");
		check("b1", true);
		check("b2", false);
		check("b3", false);
		checkNull("nullValue");
		checkNull("varWithNullInitializer");
	}
	
	public void test_boolean_compare() {
		doCompileExpectErrors("test_boolean_compare", Arrays.asList(
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
	public void test_list() {
		doCompile("test_list");
		List<Integer> intList = (List<Integer>) getVariable("intList");
		assertEquals(Integer.valueOf(1), intList.get(0));
		assertEquals(Integer.valueOf(2), intList.get(1));
		assertEquals(Integer.valueOf(3), intList.get(2));
		assertEquals(Integer.valueOf(4), intList.get(3));
		assertEquals(4, intList.size());
		List<String> stringList = (List<String>) getVariable("stringList");
		assertEquals("first", stringList.get(0));
		assertEquals("replaced", stringList.get(1));
		assertEquals("third", stringList.get(2));
		assertEquals("fourth", stringList.get(3));
		assertEquals("fifth", stringList.get(4));
		assertEquals(stringList, (List<String>) getVariable("stringListCopy"));
	}
	
	@SuppressWarnings("unchecked")
	public void test_map() {
		doCompile("test_map");
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

		c.set(2009, Calendar.MARCH, 3, 0, 0, 0);
		c.set(Calendar.MILLISECOND, 0);
		assertEquals("Tuesday", ((Map<Date, String>) getVariable("tuesday")).get(c.getTime()));
		assertEquals("Tuesday", dayInWeek.get(c.getTime()));

		c.set(2009, Calendar.MARCH, 4, 0, 0, 0);
		c.set(Calendar.MILLISECOND, 0);
		assertEquals("Wednesday", ((Map<Date, String>) getVariable("wednesday")).get(c.getTime()));
		assertEquals("Wednesday", dayInWeek.get(c.getTime()));
		assertEquals(dayInWeek, getVariable("dayInWeekCopy"));
	}
	
	public void test_record() {
		doCompile("test_record");

		// expected result
		DataRecord expected = createDefaultRecord(createDefaultMetadata("expected"));

		// simple copy
		// TODO: is this necessary?
		// assertTrue(recordEquals(expected,inputRecords[0]));
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
		// TODO: is this necessary?
		// assertTrue(recordEquals(expected, outputRecords[1]));
		
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

	public void test_plus() {
		doCompile("test_plus");

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

	public void test_minus() {
		doCompile("test_minus");

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

	public void test_multiply() {
		doCompile("test_multiply");

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

	public void test_divide() {
		doCompile("test_divide");

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
	
	public void test_modulus() {
		doCompile("test_modulus");
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
	
	public void test_unary_operators() {
		doCompile("test_unary_operators");

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
	
	public void test_unary_operators_record() {
		doCompileExpectErrors("test_unary_operators_record", Arrays.asList(
				"Illegal argument to ++/-- operator",
				"Illegal argument to ++/-- operator",
				"Illegal argument to ++/-- operator",
				"Illegal argument to ++/-- operator"));
	}
	
	public void test_equal() {
		doCompile("test_equal");

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
	
	public void test_non_equal(){
		doCompile("test_non_equal");
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
	
	public void test_in_operator() {
		doCompile("test_in_operator");

		check("a", Integer.valueOf(1));
		check("haystack", Collections.EMPTY_LIST);
		check("needle", Integer.valueOf(2));
		check("b1", true);
		check("b2", false);
		check("h2", createList(2.1D, 2.0D, 2.2D));
		check("b3", true);
		check("h3", createList("memento", "mori", "memento mori"));
		check("n3", "memento mori");
		check("b4", true);
	}
	
	public void test_greater_less() {
		doCompile("test_greater_less");

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
	
	public void test_ternary_operator(){
		doCompile("test_ternary_operator");

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
	
	public void test_logical_operators(){
		doCompile("test_logical_operators");
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
		check("res7", createList(
		/* i=0 */false, false, false,
		/* i=1 */true, true, false,
		/* i=2 */true, true, false,
		/* i=3 */false, true, false,
		/* i=4 */false, true, false,
		/* i=5 */false, false, true));

		// return test
		check("res8", createList("0123", "123", "23", "3", "4", "3"));
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
		check("res7", createList(
		/* i=0 */false, false, false,
		/* i=1 */true, true, false,
		/* i=2 */true, true, false,
		/* i=3 */false, true, false,
		/* i=4 */false, true, false,
		/* i=5 */false, false, true));

		// return test
		check("res8", createList("0123", "123", "23", "3", "4", "3"));		
	}
	
	public void test_while() {
		doCompile("test_while");
		// simple while
		check("res1", createList(0, 1, 2));
		// continue
		check("res2", createList(0, 2));
		// break
		check("res3", createList(0));
	}

	public void test_do_while() {
		doCompile("test_do_while");
		// simple while
		check("res1", createList(0, 1, 2));
		// continue
		check("res2", createList(0, null, 2));
		// break
		check("res3", createList(0));
	}
	
	public void test_for() {
		doCompile("test_for");
		
		// simple loop
		check("res1", createList(0,1,2));
		// continue
		check("res2", createList(0,null,2));
		// break
		check("res3", createList(0));
		// empty init
		check("res4", createList(0,1,2));
		// empty update
		check("res5", createList(0,1,2));
		// empty final condition
		check("res6", createList(0,1,2));
		// all conditions empty
		check("res7", createList(0,1,2));
	}
	
	public void test_foreach() {
		doCompile("test_foreach");
		check("intRes", createList(VALUE_VALUE));
		check("longRes", createList(BORN_MILLISEC_VALUE));
		check("doubleRes", createList(AGE_VALUE));
		check("decimalRes", createList(CURRENCY_VALUE));
		check("booleanRes", createList(FLAG_VALUE));
		check("stringRes", createList(NAME_VALUE, CITY_VALUE));
		check("dateRes", createList(BORN_VALUE));
	}
	
	public void test_return(){
		doCompile("test_return");
		check("lhs", Integer.valueOf(1));
		check("rhs", Integer.valueOf(2));
		check("res", Integer.valueOf(3));
	}
	
	public void test_overloading() {
		doCompile("test_overloading");
		check("res1", Integer.valueOf(3));
		check("res2", "Memento mori");
	}
	
	//Test case for 4038
	public void test_function_parameter_without_type() {
		doCompileExpectError("test_function_parameter_without_type", "Syntax error on token ')'");
	}
	
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
//TODO: somehow adapt		
//		// simple mappings
//		assertEquals(NAME_VALUE, ((StringBuilder)((StringDataField)outputRecords[0].getField("Name")).getValue()).toString());
//		assertEquals(AGE_VALUE, ((NumericDataField)outputRecords[0].getField("Age")).getValue());
//		assertEquals(CITY_VALUE, ((StringBuilder)((StringDataField)outputRecords[0].getField("City")).getValue()).toString());
//		assertEquals(BORN_VALUE, ((DateDataField)outputRecords[0].getField("Born")).getValue());
//		
//		// * mapping
//		assertTrue(recordEquals(inputRecords[1],outputRecords[1]));
	}
	
	public void test_sequence(){
		doCompile("test_sequence");
		check("intRes", createList(1,2,3));
		check("longRes", createList(Long.valueOf(1),Long.valueOf(2),Long.valueOf(3)));
		check("stringRes", createList("1","2","3"));
		check("intCurrent", Integer.valueOf(3));
		check("longCurrent", Long.valueOf(3));
		check("stringCurrent", "3");
	}
	
	//TODO: default lookup table doesn't work/is not populated?
	public void test_lookup(){
        doCompile("test_lookup");
		check("alphaResult", createList("Andorra la Vella","Andorra la Vella"));
		check("bravoResult", createList("Bruxelles","Bruxelles"));
		check("charlieResult", createList("Chamonix","Chomutov","Chamonix","Chomutov"));
		check("countResult", createList(2,2));
	}
	
//------------------------- ContainerLib Tests---------------------
	
	
	// TODO: distribute functions into separate tests
	@SuppressWarnings("unchecked")
	public void test_container_lib() {
		doCompile("test_container_lib");

		// copy
		check("copyList", createList(1, 2, 3, 4, 5));
		// pop
		check("popElem", Integer.valueOf(5));
		check("popList", createList(1, 2, 3, 4));
		// poll
		check("pollElem", Integer.valueOf(1));
		check("pollList", createList(2, 3, 4));
		// push
		check("pushElem", Integer.valueOf(6));
		check("pushList", createList(2, 3, 4, 6));
		// insert
		check("insertElem", Integer.valueOf(7));
		check("insertIndex", Integer.valueOf(1));
		check("insertList", createList(2, 7, 3, 4, 6));
		// remove
		check("removeElem", Integer.valueOf(3));
		check("removeIndex", Integer.valueOf(2));
		check("removeList", createList(2, 7, 4, 6));
		// sort
		check("sortList", createList(2, 4, 6, 7));
		// reverse
		check("reverseList", createList(7, 6, 4, 2));
		// remove_all
		assertTrue(((List<Integer>) getVariable("removeAllList")).isEmpty());
	}
	
//---------------------- StringLib Tests ------------------------

	public void test_stringlib() {
		doCompile("test_stringlib");
		check("subs", "ello ");
		check("upper", "ELLO ");
		check("lower", "ello hi   ");
		check("t", "im  ello hi");
		check("l", new BigDecimal(5));
		//check("c", "ello hi   ELLO 2,today is " + new Date());
		//TODO: enable
		//check("datum", BORN_VALUE);
		//check("ddiff", -1);
		check("isn", false);
		check("s1", Double.valueOf(6));
		//check("rep", ("etto hi   EttO 2,today is " + new Date()).replaceAll("[lL]", "t"));
		check("stdecimal", 0.25125);
		check("stdouble", 0.25125);
		check("stlong", 805421451215l);
		check("stint", -152456);
		check("i", 1234);
		check("nts", "22");
		check("dtn", 11.0);
		check("ii", 21);
		check("dts", "02.12.24");
		check("lef", "02.12");
		check("righ", "12.24");
		check("charCount", 3);
	}

	public void test_is_format() {
		doCompile("test_is_format");
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
		check("isDate11", true);
		check("isDate12", true);
		check("isDate13", false);
		check("isDate14", true);
		check("isDate15", false);
		// 24 is an illegal value for pattern HH (it allows only 0-23)
		check("isDate16", false);
		// empty string in strict mode: invalid
		check("isDate17", false);
		// empty string in lenient mode: valid
		check("isDate18", true);
	}
	
	public void test_remove_blank_space() {
		String expStr = 
			"string r1;\n" +
			"function integer transform() {\n" +
				"r1=remove_blank_space(\"" + StringUtils.specCharToString(" a	b\nc\rd   e \u000Cf\r\n") +	"\");\n" +
				"print_err(r1);\n" +
				"return 0;\n" +
			"}\n";
		doCompile(expStr, "test_remove_blank_space");
		check("r1", "abcdef");
	}
	
	public void test_get_alphanumeric_chars() {
		String expStr = 
			"string an1;\n" +
			"string an2;\n" +
			"string an3;\n" +
			"string an4;\n" +
			"function integer transform() {\n" +
				"an1=get_alphanumeric_chars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\");\n" +
				"print_err(an1);\n" +
				"an2=get_alphanumeric_chars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\",true,true);\n" +
				"print_err(an2);\n" +
				"an3=get_alphanumeric_chars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\",true,false);\n" +
				"print_err(an3);\n" +
				"an4=get_alphanumeric_chars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\",false,true);\n" +
				"print_err(an4);\n" +
				"return 0;\n" +
			"}\n";
		doCompile(expStr, "test_get_alphanumeric_chars");

		check("an1", "a1bcde2f");
		check("an2", "a1bcde2f");
		check("an3", "abcdef");
		check("an4", "12");
	}
	
    public void test_stringlib2(){
        doCompile("test_stringlib2");
        
    	check("test","tescik");
    	check("test1","zabicka");
		check("t","hippi");
		check("t1","hipp");
		check("t2","hippi");
		check("t3","");
		check("t4","y lanuaX nXXd thX lXttXr X");
		check("index",2);
		check("index1",9);
		check("index2",0);
		check("index3",-1);
		check("index4",6);              
    }

	public void test_chop() {
		doCompile("test_chop");
		check("s1", "hello");
		check("s6", "hello");
		check("s5", "hello");
		check("s2", "hello");
		check("s7", "hello\nworld");
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

	public void test_num2str_function() {
		System.out.println("num2str() test:");
		doCompile("test_num2str_function");

		check("intOutput", createList("16", "10000", "20", "10"));
		check("longOutput", createList("16", "10000", "20", "10"));
		check("doubleOutput", createList("16.16", "0x1.028f5c28f5c29p4"));
		check("decimalOutput", createList("16.16"));
	}

	public void test_pow_function() {
		System.out.println("pow() test:");
		doCompile("test_pow_function");
		
		check("intResult", createList(8d, 8d, 8d, 8d));
		check("longResult", createList(8d, 8d, 8d, 8d));
		check("doubleResult", createList(8d, 8d, 8d, 8d));
		check("decimalResult", createList(8d, 8d, 8d, 8d));
	}

	public void test_round_function() {
		System.out.println("round() test:");
		doCompile("test_round_function");
		
		check("intResult", createList(2l, 3l));
		check("longResult", createList(2l, 3l));
		check("doubleResult", createList(2l, 4l));
		check("decimalResult", createList(2l, 4l));
	}

	public void test_length_function() {
		System.out.println("length() test:");
		doCompile("test_length_function");

		check("stringLength", 8);
		check("listLength", 8);
		check("mapLength", 3);
		check("recordLength", 9);
	}
	
	public void test_math_functions() { // TODO: should be distributed into separate tests.
		doCompile("test_math_functions");
		check("varPi", Double.valueOf(Math.PI));
		check("varE", Double.valueOf(Math.E));
		check("sqrtPi", Double.valueOf(Math.sqrt(Math.PI)));
		check("sqrt9", Double.valueOf(3));
		check("ln", Double.valueOf(Math.log(3)));
		check("log10Var", Double.valueOf(Math.log10(3)));
		check("ex", Double.valueOf(Math.exp(Math.log10(3))));
		check("po", Double.valueOf(Math.pow(3, 1.2)));
		check("p", Double.valueOf(Math.pow(-10, -0.3)));
		check("r", Long.parseLong("-4"));
		check("t", Long.parseLong("-3"));
		check("truncDate", new GregorianCalendar(2004, 00, 02).getTime());
	}	
}
