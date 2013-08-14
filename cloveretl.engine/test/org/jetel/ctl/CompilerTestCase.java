package org.jetel.ctl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

import junit.framework.AssertionFailedError;

import org.jetel.component.CTLRecordTransform;
import org.jetel.component.RecordTransform;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.SetVal;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.lookup.LookupTableFactory;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.ContextProvider.Context;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.MiscUtils;
import org.jetel.util.bytes.PackedDecimal;
import org.jetel.util.crypto.Base64;
import org.jetel.util.crypto.Digest;
import org.jetel.util.crypto.Digest.DigestType;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.string.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Years;

import sun.misc.Cleaner;

public abstract class CompilerTestCase extends CloverTestCase {

	// ---------- RECORD NAMES -----------
	protected static final String INPUT_1 = "firstInput";
	protected static final String INPUT_2 = "secondInput";
	protected static final String INPUT_3 = "thirdInput";
	protected static final String INPUT_4 = "multivalueInput";
	protected static final String OUTPUT_1 = "firstOutput";
	protected static final String OUTPUT_2 = "secondOutput";
	protected static final String OUTPUT_3 = "thirdOutput";
	protected static final String OUTPUT_4 = "fourthOutput";
	protected static final String OUTPUT_5 = "firstMultivalueOutput";
	protected static final String OUTPUT_6 = "secondMultivalueOutput";
	protected static final String OUTPUT_7 = "thirdMultivalueOutput";
	protected static final String LOOKUP = "lookupMetadata";

	protected static final String NAME_VALUE = "  HELLO  ";
	protected static final Double AGE_VALUE = 20.25;
	protected static final String CITY_VALUE = "Chong'La";

	protected static final Date BORN_VALUE;
	protected static final Long BORN_MILLISEC_VALUE;
	static {
		Calendar c = Calendar.getInstance();
		c.set(2008, 12, 25, 13, 25, 55);
		c.set(Calendar.MILLISECOND, 333);
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
	
	protected TransformationGraph graph;

	public CompilerTestCase(boolean compileToJava) {
		this.compileToJava = compileToJava;
	}

	/**
	 * Method to execute tested CTL code in a way specific to testing scenario.
	 * 
	 * Assumes that
	 * {@link #graph}, {@link #inputRecords} and {@link #outputRecords}
	 * have already been set.
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

	private void checkArray(String varName, byte[] expected) {
		byte[] actual = (byte[]) getVariable(varName);
		assertTrue("Arrays do not match; expected: " + byteArrayAsString(expected) + " but was " + byteArrayAsString(actual), Arrays.equals(actual, expected));
	}

	private static String byteArrayAsString(byte[] array) {
		final StringBuilder sb = new StringBuilder("[");
		for (final byte b : array) {
			sb.append(b);
			sb.append(", ");
		}
		sb.delete(sb.length() - 2, sb.length());
		sb.append(']');
		return sb.toString();
	}

	@Override
	protected void setUp() {
		// set default locale to English to prevent various parsing errors
		Locale.setDefault(Locale.ENGLISH);
		initEngine();
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		inputRecords = null;
		outputRecords = null;
		graph = null;
	}

	protected TransformationGraph createEmptyGraph() {
		return new TransformationGraph();
	}

	protected TransformationGraph createDefaultGraph() {
		TransformationGraph g = createEmptyGraph();
		// set the context URL, so that imports can be used
		g.getRuntimeContext().setContextURL(CompilerTestCase.class.getResource("."));
		final HashMap<String, DataRecordMetadata> metadataMap = new HashMap<String, DataRecordMetadata>();
		metadataMap.put(INPUT_1, createDefaultMetadata(INPUT_1));
		metadataMap.put(INPUT_2, createDefaultMetadata(INPUT_2));
		metadataMap.put(INPUT_3, createDefaultMetadata(INPUT_3));
		metadataMap.put(INPUT_4, createDefaultMultivalueMetadata(INPUT_4));
		metadataMap.put(OUTPUT_1, createDefaultMetadata(OUTPUT_1));
		metadataMap.put(OUTPUT_2, createDefaultMetadata(OUTPUT_2));
		metadataMap.put(OUTPUT_3, createDefaultMetadata(OUTPUT_3));
		metadataMap.put(OUTPUT_4, createDefault1Metadata(OUTPUT_4));
		metadataMap.put(OUTPUT_5, createDefaultMultivalueMetadata(OUTPUT_5));
		metadataMap.put(OUTPUT_6, createDefaultMultivalueMetadata(OUTPUT_6));
		metadataMap.put(OUTPUT_7, createDefaultMultivalueMetadata(OUTPUT_7));
		metadataMap.put(LOOKUP, createDefaultMetadata(LOOKUP));
		g.addDataRecordMetadata(metadataMap);
		g.addSequence(createDefaultSequence(g, "TestSequence"));
		g.addLookupTable(createDefaultLookup(g, "TestLookup"));
		Properties properties = new Properties();
		properties.put("PROJECT", ".");
		properties.put("DATAIN_DIR", "${PROJECT}/data-in");
		properties.put("COUNT", "`1+2`");
		properties.put("NEWLINE", "\\n");
		g.setGraphProperties(properties);
		initDefaultDictionary(g);
		return g;
	}

	private void initDefaultDictionary(TransformationGraph g) {
		try {
			g.getDictionary().init();
			g.getDictionary().setValue("s", "string", null);
			g.getDictionary().setValue("i", "integer", null);
			g.getDictionary().setValue("l", "long", null);
			g.getDictionary().setValue("d", "decimal", null);
			g.getDictionary().setValue("n", "number", null);
			g.getDictionary().setValue("a", "date", null);
			g.getDictionary().setValue("b", "boolean", null);
			g.getDictionary().setValue("y", "byte", null);
			g.getDictionary().setValue("i211", "integer", new Integer(211));
			g.getDictionary().setValue("sVerdon", "string", "Verdon");
			g.getDictionary().setValue("l452", "long", new Long(452));
			g.getDictionary().setValue("d621", "decimal", new BigDecimal(621));
			g.getDictionary().setValue("n9342", "number", new Double(934.2));
			g.getDictionary().setValue("a1992", "date", new GregorianCalendar(1992, GregorianCalendar.AUGUST, 1).getTime());
			g.getDictionary().setValue("bTrue", "boolean", Boolean.TRUE);
			g.getDictionary().setValue("yFib", "byte", new byte[]{1,2,3,5,8,13,21,34,55,89} );
			g.getDictionary().setValue("stringList", "list", Arrays.asList("aa", "bb", null, "cc"));
			g.getDictionary().setContentType("stringList", "string");
			g.getDictionary().setValue("dateList", "list", Arrays.asList(new Date(12000), new Date(34000), null, new Date(56000)));
			g.getDictionary().setContentType("dateList", "date");
			g.getDictionary().setValue("byteList", "list", Arrays.asList(new byte[] {0x12}, new byte[] {0x34, 0x56}, null, new byte[] {0x78}));
			g.getDictionary().setContentType("byteList", "byte");
		} catch (ComponentNotReadyException e) {
			throw new RuntimeException("Error init default dictionary", e);
		}
		
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
		URL dataFile = getClass().getSuperclass().getResource("TestLookup.dat");
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
		ret.addField(new DataFieldMetadata("Name", DataFieldType.STRING, "|"));
		ret.addField(new DataFieldMetadata("Age", DataFieldType.NUMBER, "|"));
		ret.addField(new DataFieldMetadata("City", DataFieldType.STRING, "|"));

		DataFieldMetadata dateField = new DataFieldMetadata("Born", DataFieldType.DATE, "|");
		dateField.setFormatStr("yyyy-MM-dd HH:mm:ss");
		ret.addField(dateField);

		ret.addField(new DataFieldMetadata("BornMillisec", DataFieldType.LONG, "|"));
		ret.addField(new DataFieldMetadata("Value", DataFieldType.INTEGER, "|"));
		ret.addField(new DataFieldMetadata("Flag", DataFieldType.BOOLEAN, "|"));
		ret.addField(new DataFieldMetadata("ByteArray", DataFieldType.BYTE, "|"));

		DataFieldMetadata decimalField = new DataFieldMetadata("Currency", DataFieldType.DECIMAL, "\n");
		decimalField.setProperty(DataFieldMetadata.LENGTH_ATTR, String.valueOf(DECIMAL_PRECISION));
		decimalField.setProperty(DataFieldMetadata.SCALE_ATTR, String.valueOf(DECIMAL_SCALE));
		ret.addField(decimalField);
		
		return ret;
	}

	/**
	 * Creates records with default structure
	 * 
	 * @param name
	 *            name for the record to use
	 * @return metadata with default structure
	 */
	protected DataRecordMetadata createDefault1Metadata(String name) {
		DataRecordMetadata ret = new DataRecordMetadata(name);
		ret.addField(new DataFieldMetadata("Field1", DataFieldType.STRING, "|"));
		ret.addField(new DataFieldMetadata("Age", DataFieldType.NUMBER, "|"));
		ret.addField(new DataFieldMetadata("City", DataFieldType.STRING, "|"));

		return ret;
	}
	
	/**
	 * Creates records with default structure
	 * containing multivalue fields.
	 * 
	 * @param name
	 *            name for the record to use
	 * @return metadata with default structure
	 */
	protected DataRecordMetadata createDefaultMultivalueMetadata(String name) {
		DataRecordMetadata ret = new DataRecordMetadata(name);

		DataFieldMetadata stringListField = new DataFieldMetadata("stringListField", DataFieldType.STRING, "|");
		stringListField.setContainerType(DataFieldContainerType.LIST);
		ret.addField(stringListField);
		
		DataFieldMetadata dateField = new DataFieldMetadata("dateField", DataFieldType.DATE, "|");
		ret.addField(dateField);

		DataFieldMetadata byteField = new DataFieldMetadata("byteField", DataFieldType.BYTE, "|");
		ret.addField(byteField);

		DataFieldMetadata dateListField = new DataFieldMetadata("dateListField", DataFieldType.DATE, "|");
		dateListField.setContainerType(DataFieldContainerType.LIST);
		ret.addField(dateListField);

		DataFieldMetadata byteListField = new DataFieldMetadata("byteListField", DataFieldType.BYTE, "|");
		byteListField.setContainerType(DataFieldContainerType.LIST);
		ret.addField(byteListField);
		
		DataFieldMetadata stringField = new DataFieldMetadata("stringField", DataFieldType.STRING, "|");
		ret.addField(stringField);

		DataFieldMetadata integerMapField = new DataFieldMetadata("integerMapField", DataFieldType.INTEGER, "|");
		integerMapField.setContainerType(DataFieldContainerType.MAP);
		ret.addField(integerMapField);

		DataFieldMetadata stringMapField = new DataFieldMetadata("stringMapField", DataFieldType.STRING, "|");
		stringMapField.setContainerType(DataFieldContainerType.MAP);
		ret.addField(stringMapField);

		DataFieldMetadata dateMapField = new DataFieldMetadata("dateMapField", DataFieldType.DATE, "|");
		dateMapField.setContainerType(DataFieldContainerType.MAP);
		ret.addField(dateMapField);

		DataFieldMetadata byteMapField = new DataFieldMetadata("byteMapField", DataFieldType.BYTE, "|");
		byteMapField.setContainerType(DataFieldContainerType.MAP);
		ret.addField(byteMapField);

		DataFieldMetadata integerListField = new DataFieldMetadata("integerListField", DataFieldType.INTEGER, "|");
		integerListField.setContainerType(DataFieldContainerType.LIST);
		ret.addField(integerListField);
		
		DataFieldMetadata decimalListField = new DataFieldMetadata("decimalListField", DataFieldType.DECIMAL, "|");
		decimalListField.setContainerType(DataFieldContainerType.LIST);
		ret.addField(decimalListField);
		
		DataFieldMetadata decimalMapField = new DataFieldMetadata("decimalMapField", DataFieldType.DECIMAL, "|");
		decimalMapField.setContainerType(DataFieldContainerType.MAP);
		ret.addField(decimalMapField);
		
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
	protected DataRecord createDefaultMultivalueRecord(DataRecordMetadata dataRecordMetadata) {
		final DataRecord ret = DataRecordFactory.newRecord(dataRecordMetadata);
		ret.init();

		for (int i = 0; i < ret.getNumFields(); i++) {
			DataField field = ret.getField(i);
			DataFieldMetadata fieldMetadata = field.getMetadata();
			
			switch (fieldMetadata.getContainerType()) {
			case SINGLE:
				switch (fieldMetadata.getDataType()) {
				case STRING:
					field.setValue("John");
					break;
				case DATE:
					field.setValue(new Date(10000));
					break;
				case BYTE:
					field.setValue(new byte[] { 0x12, 0x34, 0x56, 0x78 } );
					break;
				default:
					throw new UnsupportedOperationException("Not implemented.");
				}
				break;
			case LIST:
				{
					List<Object> value = new ArrayList<Object>();
					switch (fieldMetadata.getDataType()) {
					case STRING:
						value.addAll(Arrays.asList("John", "Doe", "Jersey"));
						break;
					case INTEGER:
						value.addAll(Arrays.asList(123, 456, 789));
						break;
					case DATE:
						value.addAll(Arrays.asList(new Date (12000), new Date(34000)));
						break;
					case BYTE:
						value.addAll(Arrays.asList(new byte[] {0x12, 0x34}, new byte[] {0x56, 0x78}));
						break;
					case DECIMAL:
						value.addAll(Arrays.asList(12.34, 56.78));
						break;
					default:
						throw new UnsupportedOperationException("Not implemented.");
					}
					field.setValue(value);
				}
				break;
			case MAP:
				{
					Map<String, Object> value = new HashMap<String, Object>();
					switch (fieldMetadata.getDataType()) {
					case STRING:
						value.put("firstName", "John");
						value.put("lastName", "Doe");
						value.put("address", "Jersey");
						break;
					case INTEGER:
						value.put("count", 123);
						value.put("max", 456);
						value.put("sum", 789);
						break;
					case DATE:
						value.put("before", new Date (12000));
						value.put("after", new Date(34000));
						break;
					case BYTE:
						value.put("hash", new byte[] {0x12, 0x34});
						value.put("checksum", new byte[] {0x56, 0x78});
						break;
					case DECIMAL:
						value.put("asset", 12.34);
						value.put("liability", 56.78);
						break;
					default:
						throw new UnsupportedOperationException("Not implemented.");
					}
					field.setValue(value);
				}
				break;
			default:
				throw new IllegalArgumentException(fieldMetadata.getContainerType().toString());
			}
		}

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
		final DataRecord ret = DataRecordFactory.newRecord(dataRecordMetadata);
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
		DataRecord ret = DataRecordFactory.newRecord(metadata);
		ret.init();

		for (int i = 0; i < ret.getNumFields(); i++) {
			SetVal.setNull(ret, i);
		}

		return ret;
	}
	
	/**
	 * Executes the code using the default graph and records.
	 */
	protected void doCompile(String expStr, String testIdentifier) {
		TransformationGraph graph = createDefaultGraph(); 
		
		DataRecord[] inRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)), createEmptyRecord(graph.getDataRecordMetadata(INPUT_3)), createDefaultMultivalueRecord(graph.getDataRecordMetadata(INPUT_4)) };
		DataRecord[] outRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_3)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_4)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_5)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_6)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_7)) };
		
		doCompile(expStr, testIdentifier, graph, inRecords, outRecords);
	}

	/**
	 * This method should be used to execute a test with a custom graph and custom input and output records.
	 * 
	 * To execute a test with the default graph, 
	 * use {@link #doCompile(String)} 
	 * or {@link #doCompile(String, String)} instead.
	 * 
	 * @param expStr
	 * @param testIdentifier
	 * @param graph
	 * @param inRecords
	 * @param outRecords
	 */
	protected void doCompile(String expStr, String testIdentifier, TransformationGraph graph, DataRecord[] inRecords, DataRecord[] outRecords) {
		this.graph = graph;
		this.inputRecords = inRecords;
		this.outputRecords = outRecords;

		// prepend the compilation mode prefix
		if (compileToJava) {
			expStr = "//#CTL2:COMPILE\n" + expStr;
		}

		print_code(expStr);
		
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[inRecords.length];
		for (int i = 0; i < inRecords.length; i++) {
			inMetadata[i] = inRecords[i].getMetadata();
		}
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[outRecords.length];
		for (int i = 0; i < outRecords.length; i++) {
			outMetadata[i] = outRecords[i].getMetadata();
		}

		ITLCompiler compiler = TLCompilerFactory.createCompiler(graph, inMetadata, outMetadata, "UTF-8");
		// *** NOTE: please don't remove this commented code. It is used for debugging
		// ***       Uncomment the code to get the compiled Java code during test execution.
		// ***       Please don't commit this code uncommited.
	
//		try {
//			System.out.println(compiler.convertToJava(expStr, CTLRecordTransform.class, testIdentifier));
//		} catch (ErrorMessageException e) {
//			System.out.println("Error parsing CTL code. Unable to output Java translation.");
//		}
		
		List<ErrorMessage> messages = compiler.compile(expStr, CTLRecordTransform.class, testIdentifier);
		printMessages(messages);
		if (compiler.errorCount() > 0) {
			throw new AssertionFailedError("Error in execution. Check standard output for details.");
		}

		// *** NOTE: please don't remove this commented code. It is used for debugging
		// ***       Uncomment the code to get the compiled Java code during test execution.
		// ***       Please don't commit this code uncommited.

//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");


		executeCode(compiler);
	}

	protected void doCompileExpectError(String expStr, String testIdentifier, List<String> errCodes) {
		graph = createDefaultGraph();
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2), graph.getDataRecordMetadata(INPUT_3) };
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2), graph.getDataRecordMetadata(OUTPUT_3), graph.getDataRecordMetadata(OUTPUT_4) };

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
	protected String loadSourceCode(String testIdentifier) {
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
		
		return sourceCode.toString();
	}

	/**
	 * Method loads and compiles tested CTL code from a file with the name <code>testIdentifier.ctl</code> The CTL code files should
	 * be stored in the same directory as this class.
	 * 
	 * The default graph and records are used for the execution.
	 * 
	 * @param Test
	 *            identifier defining CTL file to load code from
	 */
	protected void doCompile(String testIdentifier) {
		String sourceCode = loadSourceCode(testIdentifier);
		doCompile(sourceCode, testIdentifier);
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

	@SuppressWarnings("unchecked")
	public void test_operators_unary_record_allowed() {
		doCompile("test_operators_unary_record_allowed");
		check("value", Arrays.asList(14, 16, 16, 65, 63, 63));
		check("bornMillisec", Arrays.asList(14L, 16L, 16L, 65L, 63L, 63L));
		List<Double> actualAge = (List<Double>) getVariable("age");
		double[] expectedAge = {14.123, 16.123, 16.123, 65.789, 63.789, 63.789};
		for (int i = 0; i < actualAge.size(); i++) {
			assertEquals("age[" + i + "]", expectedAge[i], actualAge.get(i), 0.0001);
		}
		check("currency", Arrays.asList(
				new BigDecimal(BigInteger.valueOf(12500), 3), 
				new BigDecimal(BigInteger.valueOf(14500), 3),
				new BigDecimal(BigInteger.valueOf(14500), 3),
				new BigDecimal(BigInteger.valueOf(65432), 3),
				new BigDecimal(BigInteger.valueOf(63432), 3),
				new BigDecimal(BigInteger.valueOf(63432), 3)
		));
	}

	@SuppressWarnings("unchecked")
	public void test_dynamic_compare() {
		doCompile("test_dynamic_compare");
		
		String varName = "compare";
		List<Integer> compareResult = (List<Integer>) getVariable(varName);
		for (int i = 0; i < compareResult.size(); i++) {
			if ((i % 3) == 0) {
				assertTrue(varName + "[" + i + "]", compareResult.get(i) > 0);
			} else if ((i % 3) == 1) {
				assertEquals(varName + "[" + i + "]", Integer.valueOf(0), compareResult.get(i));
			} else if ((i % 3) == 2) {
				assertTrue(varName + "[" + i + "]", compareResult.get(i) < 0);
			}
		}
		
		varName = "compareBooleans";
		compareResult = (List<Integer>) getVariable(varName);
		assertEquals(varName + "[0]", Integer.valueOf(0), compareResult.get(0));
		assertTrue(varName + "[1]", compareResult.get(1) > 0);
		assertTrue(varName + "[2]", compareResult.get(2) < 0);
		assertEquals(varName + "[3]", Integer.valueOf(0), compareResult.get(3));
	}
	
	public void test_dynamiclib_compare_expect_error(){
		try {
			doCompile("function integer transform(){"
					+ "firstInput myRec; myRec = $out.0; "
					+ "firstOutput myRec2; myRec2 = $out.1;"
					+ "integer i = compare(myRec, 'Flagxx', myRec2, 'Flag'); return 0;}","test_dynamiclib_compare_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "firstInput myRec; myRec = $out.0; "
					+ "firstOutput myRec2; myRec2 = $out.1;"
					+ "integer i = compare(myRec, 'Flag', myRec2, 'Flagxx'); return 0;}","test_dynamiclib_compare_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "firstInput myRec; myRec = null; "
					+ "firstOutput myRec2; myRec2 = $out.1;"
					+ "integer i = compare(myRec, 'Flag', myRec2, 'Flag'); return 0;}","test_dynamiclib_compare_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "$out.0.Flag = true; "
					+ "$out.1.Age = 11;"
					+ "integer i = compare($out.0, 'Flag', $out.1, 'Age'); return 0;}","test_dynamiclib_compare_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "firstInput myRec; myRec = $out.0; "
					+ "firstOutput myRec2; myRec2 = $out.1;"
					+ "integer i = compare(myRec, -1, myRec2, -1 ); return 0;}","test_dynamiclib_compare_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "firstInput myRec; myRec = $out.0; "
					+ "firstOutput myRec2; myRec2 = $out.1;"
					+ "integer i = compare(myRec, 2, myRec2, 2 ); return 0;}","test_dynamiclib_compare_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "$out.0.8 = 12.4d; "
					+ "$out.1.8 = 12.5d;"
					+ "integer i = compare($out.0, 9, $out.1, 9 ); return 0;}","test_dynamiclib_compare_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "$out.0.0 = null; "
					+ "$out.1.0 = null;"
					+ "integer i = compare($out.0, 0, $out.1, 0 ); return 0;}","test_dynamiclib_compare_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	private void test_dynamic_get_set_loop(String testIdentifier) {
		doCompile(testIdentifier);
		
		check("recordLength", 9);
		
		check("value", Arrays.asList(654321, 777777, 654321, 654323, 123456, 112567, 112233));
		check("type", Arrays.asList("string", "number", "string", "date", "long", "integer", "boolean", "byte", "decimal"));
		check("asString", Arrays.asList("1000", "1001.0", "1002", "Thu Jan 01 01:00:01 CET 1970", "1004", "1005", "true", null, "1008.000"));
		check("isNull", Arrays.asList(false, false, false, false, false, false, false, true, false));
		check("fieldName", Arrays.asList("Name", "Age", "City", "Born", "BornMillisec", "Value", "Flag", "ByteArray", "Currency"));
		Integer[] indices = new Integer[9];
		for (int i = 0; i < indices.length; i++) {
			indices[i] = i;
		}
		check("fieldIndex", Arrays.asList(indices));
		
		// check dynamic write and read with all data types
		check("booleanVar", true);
		assertTrue("byteVar", Arrays.equals(new BigInteger("1234567890abcdef", 16).toByteArray(), (byte[]) getVariable("byteVar")));
		check("decimalVar", new BigDecimal(BigInteger.valueOf(1000125), 3));
		check("integerVar", 1000);
		check("longVar", 1000000000000L);
		check("numberVar", 1000.5);
		check("stringVar", "hello");
		check("dateVar", new Date(5000));
		
		// null value
		Boolean[] someValue = new Boolean[graph.getDataRecordMetadata(INPUT_1).getNumFields()];
		Arrays.fill(someValue, Boolean.FALSE);
		check("someValue", Arrays.asList(someValue));
		
		Boolean[] nullValue = new Boolean[graph.getDataRecordMetadata(INPUT_1).getNumFields()];
		Arrays.fill(nullValue, Boolean.TRUE);
		check("nullValue", Arrays.asList(nullValue));

		String[] asString2 = new String[graph.getDataRecordMetadata(INPUT_1).getNumFields()];
		check("asString2", Arrays.asList(asString2));

		Boolean[] isNull2 = new Boolean[graph.getDataRecordMetadata(INPUT_1).getNumFields()];
		Arrays.fill(isNull2, Boolean.TRUE);
		check("isNull2", Arrays.asList(isNull2));
	}

	public void test_dynamic_get_set_loop() {
		test_dynamic_get_set_loop("test_dynamic_get_set_loop");
	}

	public void test_dynamic_get_set_loop_alternative() {
		test_dynamic_get_set_loop("test_dynamic_get_set_loop_alternative");
	}

	public void test_dynamic_invalid() {
		doCompileExpectErrors("test_dynamic_invalid", Arrays.asList(
				"Input record cannot be assigned to",
				"Input record cannot be assigned to"
		));
	}
	
	public void test_dynamiclib_getBoolValue(){
		doCompile("test_dynamiclib_getBoolValue");
		check("ret1", true);
		check("ret2", true);
		check("ret3", false);
		check("ret4", false);
		check("ret5", null);
		check("ret6", null);
	}
	
	public void test_dynamiclib_getBoolValue_expect_error(){
		try {
			doCompile("function integer transform(){boolean b = getBoolValue($in.0, 2);return 0;}","test_dynamiclib_getBoolValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){boolean b = getBoolValue($in.0, 'Age');return 0;}","test_dynamiclib_getBoolValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){firstInput fi; fi = null; boolean b = getBoolValue(fi, 6); return 0;}","test_dynamiclib_getBoolValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){firstInput fi; fi = null; boolean b = getBoolValue(fi, 'Flag'); return 0;}","test_dynamiclib_getBoolValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_getByteValue(){
		doCompile("test_dynamiclib_getByteValue");
		checkArray("ret1",CompilerTestCase.BYTEARRAY_VALUE);
		checkArray("ret2",CompilerTestCase.BYTEARRAY_VALUE);
		check("ret3", null);
		check("ret4", null);
	}
	
	public void test_dynamiclib_getByteValue_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; byte b = getByteValue(fi,7); return 0;}","test_dynamiclib_getByteValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){firstInput fi = null; byte b = fi.getByteValue('ByteArray'); return 0;}","test_dynamiclib_getByteValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){byte b = $in.0.getByteValue('Age'); return 0;}","test_dynamiclib_getByteValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){byte b = getByteValue($in.0, 0); return 0;}","test_dynamiclib_getByteValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_getDateValue(){
		doCompile("test_dynamiclib_getDateValue");
		check("ret1", CompilerTestCase.BORN_VALUE);
		check("ret2", CompilerTestCase.BORN_VALUE);
		check("ret3", null);
		check("ret4", null);
	}
	
	public void test_dynamiclib_getDateValue_expect_error(){
		try {
			doCompile("function integer transform(){date d = getDateValue($in.0,1); return 0;}","test_dynamiclib_getDateValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){date d = getDateValue($in.0,'Age'); return 0;}","test_dynamiclib_getDateValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){firstInput fi = null; date d = getDateValue(null,'Born'); return 0;}","test_dynamiclib_getDateValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){firstInput fi = null; date d = getDateValue(null,3); return 0;}","test_dynamiclib_getDateValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_getDecimalValue(){
		doCompile("test_dynamiclib_getDecimalValue");
		check("ret1", CompilerTestCase.CURRENCY_VALUE);
		check("ret2", CompilerTestCase.CURRENCY_VALUE);
		check("ret3", null);
		check("ret4", null);
	}
	
	public void test_dynamiclib_getDecimalValue_expect_error(){
		try {
			doCompile("function integer transform(){decimal d = getDecimalValue($in.0, 1); return 0;}","test_dynamiclib_getDecimalValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal d = getDecimalValue($in.0, 'Age'); return 0;}","test_dynamiclib_getDecimalValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){firstInput fi = null; decimal d = getDecimalValue(fi,8); return 0;}","test_dynamiclib_getDecimalValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){firstInput fi = null; decimal d = getDecimalValue(fi,'Currency'); return 0;}","test_dynamiclib_getDecimalValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_getFieldIndex(){
		doCompile("test_dynamiclib_getFieldIndex");
		check("ret1", 1);
		check("ret2", 1);
		check("ret3", -1);
	}
	
	public void test_dynamiclib_getFieldIndex_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; integer int = fi.getFieldIndex('Age'); return 0;}","test_dynamiclib_getFieldIndex_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_getFieldLabel(){
		doCompile("test_dynamiclib_getFieldLabel");		
		check("ret1", "Age");
		check("ret2","Name");
	}
	
	public void test_dynamiclib_getFieldLable_expect_error(){
		try {
			doCompile("function integer transform(){string name = getFieldLabel($in.0, -5);return 0;}","test_dynamiclib_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string name = getFieldLabel($in.0, 12);return 0;}","test_dynamiclib_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){firstInput fi = null; string name = fi.getFieldLabel(2);return 0;}","test_dynamiclib_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_getFieldName(){
		doCompile("test_dynamiclib_getFieldName");
		check("ret1", "Age");
		check("ret2", "Name");
	}
	
	public void test_dynamiclib_getFieldName_expect_error(){
		try {
			doCompile("function integer transform(){string str = getFieldName($in.0, -5); return 0;}","test_dynamiclib_getFieldName_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string str = getFieldName($in.0, 15); return 0;}","test_dynamiclib_getFieldName_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){firstInput fi = null; string str = fi.getFieldName(2); return 0;}","test_dynamiclib_getFieldName_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_getFieldType(){
		doCompile("test_dynamiclib_getFieldType");
		check("ret1", "string");
		check("ret2", "number");
	}
	
	public void test_dynamiclib_getFieldType_expect_error(){
		try {
			doCompile("function integer transform(){string str = getFieldType($in.0, -5); return 0;}","test_dynamiclib_getFieldType_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string str = getFieldType($in.0, 12); return 0;}","test_dynamiclib_getFieldType_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){firstInput fi = null; string str = fi.getFieldType(5); return 0;}","test_dynamiclib_getFieldType_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_getIntValue(){
		doCompile("test_dynamiclib_getIntValue");
		check("ret1", CompilerTestCase.VALUE_VALUE);
		check("ret2", CompilerTestCase.VALUE_VALUE);
		check("ret3", CompilerTestCase.VALUE_VALUE);
		check("ret4", CompilerTestCase.VALUE_VALUE);
		check("ret5", null);
	}
	
	public void test_dynamiclib_getIntValue_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; integer i = fi.getIntValue(5); return 0;}","test_dynamiclib_getIntValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer i = getIntValue($in.0, 1); return 0;}","test_dynamiclib_getIntValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer i = getIntValue($in.0, 'Born'); return 0;}","test_dynamiclib_getIntValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_getLongValue(){
		doCompile("test_dynamiclib_getLongValue");
		check("ret1", CompilerTestCase.BORN_MILLISEC_VALUE);
		check("ret2", CompilerTestCase.BORN_MILLISEC_VALUE);
		check("ret3", null);
	}
	
	public void test_dynamiclib_getLongValue_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; long l = getLongValue(fi, 4);return 0;} ","test_dynamiclib_getLongValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long l = getLongValue($in.0, 7);return 0;} ","test_dynamiclib_getLongValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long l = getLongValue($in.0, 'Age');return 0;} ","test_dynamiclib_getLongValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_getNumValue(){
		doCompile("test_dynamiclib_getNumValue");
		check("ret1", CompilerTestCase.AGE_VALUE);
		check("ret2", CompilerTestCase.AGE_VALUE);
		check("ret3", null);
	}	

	public void test_dynamiclib_getNumValue_expectValue(){
		try {
			doCompile("function integer transform(){firstInput fi = null; number n = getNumValue(fi, 1); return 0;}","test_dynamiclib_getNumValue_expectValue");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){number n = getNumValue($in.0, 4); return 0;}","test_dynamiclib_getNumValue_expectValue");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){number n = $in.0.getNumValue('Name'); return 0;}","test_dynamiclib_getNumValue_expectValue");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_getStringValue(){
		doCompile("test_dynamiclib_getStringValue");
		check("ret1", CompilerTestCase.NAME_VALUE);
		check("ret2", CompilerTestCase.NAME_VALUE);
		check("ret3", null);
	}
	
	public void test_dynamiclib_getStringValue_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; string str = getStringValue(fi, 0); return 0;}","test_dynamiclib_getStringValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string str = getStringValue($in.0, 5); return 0;}","test_dynamiclib_getStringValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string str = $in.0.getStringValue('Age'); return 0;}","test_dynamiclib_getStringValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_getValueAsString(){
		doCompile("test_dynamiclib_getValueAsString");
		check("ret1", "  HELLO  ");
		check("ret2", "20.25");
		check("ret3", "Chong'La");
		check("ret4", "Sun Jan 25 13:25:55 CET 2009");
		check("ret5", "1232886355333");
		check("ret6", "2147483637");
		check("ret7", "true");
		check("ret8", "41626563656461207a65646c612064656461");
		check("ret9", "133.525");
		check("ret10", null);
	}
	
	public void test_dynamiclib_getValueAsString_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; string str = getValueAsString(fi, 1); return 0;}","test_dynamiclib_getValueAsString_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string str = getValueAsString($in.0, -1); return 0;}","test_dynamiclib_getValueAsString_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string str = $in.0.getValueAsString(10); return 0;}","test_dynamiclib_getValueAsString_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_isNull(){
		doCompile("test_dynamiclib_isNull");
		check("ret1", false);
		check("ret2", false);
		check("ret3", true);
	}
	
	public void test_dynamiclib_isNull_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; boolean b = fi.isNull(1); return 0;}","test_dynamiclib_isNull_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){boolean b = $in.0.isNull(-5); return 0;}","test_dynamiclib_isNull_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){boolean b = isNull($in.0,12); return 0;}","test_dynamiclib_isNull_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_setBoolValue(){
		doCompile("test_dynamiclib_setBoolValue");
		check("ret1", null);
		check("ret2", true);
		check("ret3", false);
	}
	
	public void test_dynamiclib_setBoolValue_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; setBoolValue(fi,6,true); return 0;}","test_dynamiclib_setBoolValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setBoolValue($out.0,1,true); return 0;}","test_dynamiclib_setBoolValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){$out.0.setBoolValue(15,true); return 0;}","test_dynamiclib_setBoolValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){$out.0.setBoolValue(-1,true); return 0;}","test_dynamiclib_setBoolValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_setByteValue() throws UnsupportedEncodingException{
		doCompile("test_dynamiclib_setByteValue");
		checkArray("ret1", "Urgot".getBytes("UTF-8"));
		check("ret2", null);
	}
	
	public void test_dynamiclib_setByteValue_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi =null; setByteValue(fi,7,str2byte('Sion', 'utf-8')); return 0;}","test_dynamiclib_setByteValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setByteValue($out.0, 1, str2byte('Sion', 'utf-8')); return 0;}","test_dynamiclib_setByteValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setByteValue($out.0, 12, str2byte('Sion', 'utf-8')); return 0;}","test_dynamiclib_setByteValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){$out.0.setByteValue(-2, str2byte('Sion', 'utf-8')); return 0;}","test_dynamiclib_setByteValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_setDateValue(){
		doCompile("test_dynamiclib_setDateValue");
		Calendar cal = Calendar.getInstance();
		cal.set(2006,10,12,0,0,0);
		cal.set(Calendar.MILLISECOND, 0);
		check("ret1", cal.getTime());
		check("ret2", null);
	}
	
	public void test_dynamiclib_setDateValue_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; setDateValue(fi,'Born', null);return 0;}","test_dynamiclib_setDateValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setDateValue($out.0,1, null);return 0;}","test_dynamiclib_setDateValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setDateValue($out.0,-2, null);return 0;}","test_dynamiclib_setDateValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){$out.0.setDateValue(12, null);return 0;}","test_dynamiclib_setDateValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_setDecimalValue(){
		doCompile("test_dynamiclib_setDecimalValue");
		check("ret1", new BigDecimal("12.300"));
		check("ret2", null);
	}
	
	public void test_dynamiclib_setDecimalValue_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; setDecimalValue(fi, 'Currency', 12.8d);return 0;}","test_dynamiclib_setDecimalValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setDecimalValue($out.0, 'Name', 12.8d);return 0;}","test_dynamiclib_setDecimalValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){$out.0.setDecimalValue(-1, 12.8d);return 0;}","test_dynamiclib_setDecimalValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){$out.0.setDecimalValue(15, 12.8d);return 0;}","test_dynamiclib_setDecimalValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_setIntValue(){
		doCompile("test_dynamiclib_setIntValue");
		check("ret1", 90);
		check("ret2", null);
	}
	
	public void test_dynamiclib_setIntValue_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi =null; setIntValue(fi,5,null);return 0;}","test_dynamiclib_setIntValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setIntValue($out.0,4,90);return 0;}","test_dynamiclib_setIntValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setIntValue($out.0,-2,90);return 0;}","test_dynamiclib_setIntValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){$out.0.setIntValue(15,90);return 0;}","test_dynamiclib_setIntValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_setLongValue(){
		doCompile("test_dynamiclib_setLongValue");
		check("ret1", 1565486L);
		check("ret2", null);
	}
	
	public void test_dynamiclib_setLongValue_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; setLongValue(fi, 4, 51231L); return 0;}","test_dynamiclib_setLongValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setLongValue($out.0, 0, 51231L); return 0;}","test_dynamiclib_setLongValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setLongValue($out.0, -1, 51231L); return 0;}","test_dynamiclib_setLongValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){$out.0.setLongValue(12, 51231L); return 0;}","test_dynamiclib_setLongValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_setNumValue(){
		doCompile("test_dynamiclib_setNumValue");
		check("ret1", 12.5d);
		check("ret2", null);
	}
	
	public void test_dynamiclib_setNumValue_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; setNumValue(fi, 'Age', 15.3); return 0;}","test_dynamiclib_setNumValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setNumValue($out.0, 'Name', 15.3); return 0;}","test_dynamiclib_setNumValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setNumValue($out.0, -1, 15.3); return 0;}","test_dynamiclib_setNumValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){$out.0.setNumValue(11, 15.3); return 0;}","test_dynamiclib_setNumValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_dynamiclib_setStringValue(){
		doCompile("test_dynamiclib_setStringValue");
		check("ret1", "Zac");
		check("ret2", null);
	}
	
	public void test_dynamiclib_setStringValue_expect_error(){
		try {
			doCompile("function integer transform(){firstInput fi = null; setStringValue(fi, 'Name', 'Draven'); return 0;}","test_dynamiclib_setStringValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setStringValue($out.0, 'Age', 'Soraka'); return 0;}","test_dynamiclib_setStringValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){setStringValue($out.0, -1, 'Rengar'); return 0;}","test_dynamiclib_setStringValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){$out.0.setStringValue(11, 'Vaigar'); return 0;}","test_dynamiclib_setStringValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_return_constants() {
		// test case for issue 2257
		System.out.println("Return constants test:");
		doCompile("test_return_constants");

		check("skip", RecordTransform.SKIP);
		check("all", RecordTransform.ALL);
		check("ok", NORMALIZE_RETURN_OK);
		check("stop", RecordTransform.STOP);
	}

	public void test_ambiguous() {
		// built-in toString function
		doCompileExpectError("test_ambiguous_toString", "Function 'toString' is ambiguous");
		// built-in join function
		doCompileExpectError("test_ambiguous_join", "Function 'join' is ambiguous");
		// locally defined functions
		doCompileExpectError("test_ambiguous_localFunctions", "Function 'local' is ambiguous");
		// locally overloaded built-in getUrlPath() function 
		doCompileExpectError("test_ambiguous_combined", "Function 'getUrlPath' is ambiguous");
		// swapped arguments - non null ambiguity
		doCompileExpectError("test_ambiguous_swapped", "Function 'swapped' is ambiguous");
		// primitive type widening; the test depends on specific values of the type distance function, can be removed
		doCompileExpectError("test_ambiguous_widening", "Function 'widening' is ambiguous");
	}
	
	public void test_raise_error_terminal() {
		// test case for issue 2337
		doCompile("test_raise_error_terminal");
	}

	public void test_raise_error_nonliteral() {
		// test case for issue CL-2071
		doCompile("test_raise_error_nonliteral");
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

	public void test_global_scope() {
		// test case for issue 5006
		doCompile("test_global_scope");
		
		check("len", "Kokon".length());
	}
	
	//TODO Implement
	/*public void test_new() {
		doCompile("test_new");
	}*/

	public void test_parser() {
		System.out.println("\nParser test:");
		doCompile("test_parser");		
	}

	public void test_ref_res_import() {
		System.out.println("\nSpecial character resolving (import) test:");

		URL importLoc = getClass().getSuperclass().getResource("test_ref_res.ctl");
		String expStr = "import '" + importLoc + "';\n";
		doCompile(expStr, "test_ref_res_import");
	}

	public void test_ref_res_noimport() {
		System.out.println("\nSpecial character resolving (no import) test:");
		doCompile("test_ref_res");		
	}
	
	public void test_import() {
		System.out.println("\nImport test:");

		URL importLoc = getClass().getSuperclass().getResource("import.ctl");
		String expStr = "import '" + importLoc + "';\n";
		importLoc = getClass().getSuperclass().getResource("other.ctl");
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
		URL importLoc = getClass().getSuperclass().getResource("samplecode.ctl");
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
	
	public void test_type_date() throws Exception {
		doCompile("test_type_date");
		check("d3", new GregorianCalendar(2006, GregorianCalendar.AUGUST, 1).getTime());
		check("d2", new GregorianCalendar(2006, GregorianCalendar.AUGUST, 2, 15, 15, 3).getTime());
		check("d1", new GregorianCalendar(2006, GregorianCalendar.JANUARY, 1, 1, 2, 3).getTime());
		check("field", BORN_VALUE);
		checkNull("nullValue");
		check("minValue", new GregorianCalendar(1970, GregorianCalendar.JANUARY, 1, 1, 0, 0).getTime());
		checkNull("varWithNullInitializer");
		
		// test with a default time zone set on the GraphRuntimeContext
		Context context = null;
		try {
			tearDown();
			setUp();
			
			TransformationGraph graph = new TransformationGraph();
			graph.getRuntimeContext().setTimeZone("GMT+8");
			context = ContextProvider.registerGraph(graph);

			doCompile("test_type_date");
			
			Calendar calendar = new GregorianCalendar(2006, GregorianCalendar.AUGUST, 2, 15, 15, 3);
			calendar.setTimeZone(TimeZone.getTimeZone("GMT+8"));
			check("d2", calendar.getTime());
			calendar.set(2006, 0, 1, 1, 2, 3);
			check("d1", calendar.getTime());
		} finally {
			ContextProvider.unregister(context);
		}
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
	
	public void test_type_list() {
		doCompile("test_type_list");
		check("intList", Arrays.asList(1, 2, 3, 4, 5, 6));
		check("intList2", Arrays.asList(1, 2, 3));
		check("stringList", Arrays.asList(
				"first", "replaced", "third", "fourth",
				"fifth", "sixth", "extra"));
		check("stringListCopy", Arrays.asList(
				"first", "second", "third", "fourth",
				"fifth", "seventh"));
		check("stringListCopy2", Arrays.asList(
				"first", "replaced", "third", "fourth",
				"fifth", "sixth", "extra"));
		assertTrue(getVariable("stringList") != getVariable("stringListCopy"));
		assertEquals(getVariable("stringList"), getVariable("stringListCopy2"));
		
		assertEquals(Arrays.asList(false, null, true), getVariable("booleanList"));
		assertDeepEquals(Arrays.asList(new byte[] {(byte) 0xAB}, null), getVariable("byteList"));
		assertDeepEquals(Arrays.asList(null, new byte[] {(byte) 0xCD}), getVariable("cbyteList"));
		assertEquals(Arrays.asList(new Date(12000), null, new Date(34000)), getVariable("dateList"));
		assertEquals(Arrays.asList(null, new BigDecimal(BigInteger.valueOf(1234), 2)), getVariable("decimalList"));
		assertEquals(Arrays.asList(12, null, 34), getVariable("intList3"));
		assertEquals(Arrays.asList(12l, null, 98l), getVariable("longList"));
		assertEquals(Arrays.asList(12.34, null, 56.78), getVariable("numberList"));
		assertEquals(Arrays.asList("aa", null, "bb"), getVariable("stringList2"));
		
		List<?> decimalList2 = (List<?>) getVariable("decimalList2");
		for (Object o: decimalList2) {
			assertTrue(o instanceof BigDecimal);
		}
		
		List<?> intList4 = (List<?>) getVariable("intList4");
		Set<Object> intList4Set = new HashSet<Object>(intList4);
		assertEquals(3, intList4Set.size());
	}
	
	public void test_type_list_field() {
		doCompile("test_type_list_field");
		check("copyByValueTest1", "2");
		check("copyByValueTest2", "test");
	}

	public void test_type_map_field() {
		doCompile("test_type_map_field");
		
		Integer copyByValueTest1 = (Integer) getVariable("copyByValueTest1");
		assertEquals(new Integer(2), copyByValueTest1);
		
		Integer copyByValueTest2 = (Integer) getVariable("copyByValueTest2");
		assertEquals(new Integer(100), copyByValueTest2);
	}
	
	/**
	 * The structure of the objects must be exactly the same!
	 * 
	 * @param o1
	 * @param o2
	 */
	private static void assertDeepCopy(Object o1, Object o2) {
		if (o1 instanceof DataRecord) {
			assertFalse(o1 == o2);
			DataRecord r1 = (DataRecord) o1;
			DataRecord r2 = (DataRecord) o2;
			for (int i = 0; i < r1.getNumFields(); i++) {
				assertDeepCopy(r1.getField(i).getValue(), r2.getField(i).getValue());
			}
		} else if (o1 instanceof Map) {
			assertFalse(o1 == o2);
			Map<?, ?> m1 = (Map<?, ?>) o1;
			Map<?, ?> m2 = (Map<?, ?>) o2;
			for (Object key: m1.keySet()) {
				assertDeepCopy(m1.get(key), m2.get(key));
			}
		} else if (o1 instanceof List) {
			assertFalse(o1 == o2);
			List<?> l1 = (List<?>) o1;
			List<?> l2 = (List<?>) o2;
			for (int i = 0; i < l1.size(); i++) {
				assertDeepCopy(l1.get(i), l2.get(i));
			}
		} else if (o1 instanceof Date) {
			assertFalse(o1 == o2);
//		} else if (o1 instanceof byte[]) { // not required anymore
//			assertFalse(o1 == o2);
		}
	}
	
	/**
	 * The structure of the objects must be exactly the same!
	 * 
	 * @param o1
	 * @param o2
	 */
	private static void assertDeepEquals(Object o1, Object o2) {
		if ((o1 == null) && (o2 == null)) {
			return;
		}
		assertTrue((o1 == null) == (o2 == null));
		if (o1 instanceof DataRecord) {
			DataRecord r1 = (DataRecord) o1;
			DataRecord r2 = (DataRecord) o2;
			assertEquals(r1.getNumFields(), r2.getNumFields());
			for (int i = 0; i < r1.getNumFields(); i++) {
				assertDeepEquals(r1.getField(i).getValue(), r2.getField(i).getValue());
			}
		} else if (o1 instanceof Map) {
			Map<?, ?> m1 = (Map<?, ?>) o1;
			Map<?, ?> m2 = (Map<?, ?>) o2;
			assertTrue(m1.keySet().equals(m2.keySet()));
			for (Object key: m1.keySet()) {
				assertDeepEquals(m1.get(key), m2.get(key));
			}
		} else if (o1 instanceof List) {
			List<?> l1 = (List<?>) o1;
			List<?> l2 = (List<?>) o2;
			assertEquals("size", l1.size(), l2.size());
			for (int i = 0; i < l1.size(); i++) {
				assertDeepEquals(l1.get(i), l2.get(i));
			}
		} else if (o1 instanceof byte[]) {
			byte[] b1 = (byte[]) o1;
			byte[] b2 = (byte[]) o2;
			if (b1 != b2) {
				if (b1 == null || b2 == null) {
					assertEquals(b1, b2);
				}
				assertEquals("length", b1.length, b2.length);
				for (int i = 0; i < b1.length; i++) {
					assertEquals(String.format("[%d]", i), b1[i], b2[i]);
				}
			}
		} else if (o1 instanceof CharSequence) {
			String s1 = ((CharSequence) o1).toString();
			String s2 = ((CharSequence) o2).toString();
			assertEquals(s1, s2);
		} else if ((o1 instanceof Decimal) || (o1 instanceof BigDecimal)) {
			BigDecimal d1 = o1 instanceof Decimal ? ((Decimal) o1).getBigDecimalOutput() : (BigDecimal) o1;
			BigDecimal d2 = o2 instanceof Decimal ? ((Decimal) o2).getBigDecimalOutput() : (BigDecimal) o2;
			assertEquals(d1, d2);
		} else {
			assertEquals(o1, o2);
		}
	}
	
	private void check_assignment_deepcopy_variable_declaration() {
		Date testVariableDeclarationDate1 = (Date) getVariable("testVariableDeclarationDate1");
		Date testVariableDeclarationDate2 = (Date) getVariable("testVariableDeclarationDate2");
		byte[] testVariableDeclarationByte1 = (byte[]) getVariable("testVariableDeclarationByte1");
		byte[] testVariableDeclarationByte2 = (byte[]) getVariable("testVariableDeclarationByte2");
		
		assertDeepEquals(testVariableDeclarationDate1, testVariableDeclarationDate2);
		assertDeepEquals(testVariableDeclarationByte1, testVariableDeclarationByte2);
		
		assertDeepCopy(testVariableDeclarationDate1, testVariableDeclarationDate2);
		assertDeepCopy(testVariableDeclarationByte1, testVariableDeclarationByte2);
	}
	
	
	@SuppressWarnings("unchecked")
	private void check_assignment_deepcopy_array_access_expression() {
		{
			// JJTARRAYACCESSEXPRESSION - List
			List<String> stringListField1 = (List<String>) getVariable("stringListField1");
			DataRecord recordInList1 = (DataRecord) getVariable("recordInList1");
			List<DataRecord> recordList1 = (List<DataRecord>) getVariable("recordList1");
			List<DataRecord> recordList2 = (List<DataRecord>) getVariable("recordList2");
			
			assertDeepEquals(stringListField1, recordInList1.getField("stringListField").getValue());
			assertDeepEquals(recordInList1, recordList1.get(0));
			assertDeepEquals(recordList1, recordList2);
			
			assertDeepCopy(stringListField1, recordInList1.getField("stringListField").getValue());
			assertDeepCopy(recordInList1, recordList1.get(0));
			assertDeepCopy(recordList1, recordList2);
		}

		{
			// map of records
			Date testDate1 = (Date) getVariable("testDate1");
			Map<Integer, DataRecord> recordMap1 = (Map<Integer, DataRecord>) getVariable("recordMap1");
			DataRecord recordInMap1 = (DataRecord) getVariable("recordInMap1");
			DataRecord recordInMap2 = (DataRecord) getVariable("recordInMap2");
			Map<Integer, DataRecord> recordMap2 = (Map<Integer, DataRecord>) getVariable("recordMap2");

			assertDeepEquals(testDate1, recordInMap1.getField("dateField").getValue());
			assertDeepEquals(recordInMap1, recordMap1.get(0));
			assertDeepEquals(recordInMap2, recordMap1.get(0));
			assertDeepEquals(recordMap1, recordMap2);
			
			assertDeepCopy(testDate1, recordInMap1.getField("dateField").getValue());
			assertDeepCopy(recordInMap1, recordMap1.get(0));
			assertDeepCopy(recordInMap2, recordMap1.get(0));
			assertDeepCopy(recordMap1, recordMap2);
		}

		{
			// map of dates
			Map<Integer, Date> dateMap1 = (Map<Integer, Date>) getVariable("dateMap1");
			Date date1 = (Date) getVariable("date1");
			Date date2 = (Date) getVariable("date2");

			assertDeepCopy(date1, dateMap1.get(0));
			assertDeepCopy(date2, dateMap1.get(1));
		}
		
		{
			// map of byte arrays
			Map<Integer, byte[]> byteMap1 = (Map<Integer, byte[]>) getVariable("byteMap1");
			byte[] byte1 = (byte[]) getVariable("byte1");
			byte[] byte2 = (byte[]) getVariable("byte2");
			
			assertDeepCopy(byte1, byteMap1.get(0));
			assertDeepCopy(byte2, byteMap1.get(1));
		}

		{
			// JJTARRAYACCESSEXPRESSION - Function call
			List<String> testArrayAccessFunctionCallStringList = (List<String>) getVariable("testArrayAccessFunctionCallStringList");
			DataRecord testArrayAccessFunctionCall = (DataRecord) getVariable("testArrayAccessFunctionCall");
			Map<String, DataRecord> function_call_original_map = (Map<String, DataRecord>) getVariable("function_call_original_map");
			Map<String, DataRecord> function_call_copied_map = (Map<String, DataRecord>) getVariable("function_call_copied_map");
			List<DataRecord> function_call_original_list = (List<DataRecord>) getVariable("function_call_original_list");
			List<DataRecord> function_call_copied_list = (List<DataRecord>) getVariable("function_call_copied_list");
			
			assertDeepEquals(testArrayAccessFunctionCallStringList, testArrayAccessFunctionCall.getField("stringListField").getValue());

			assertEquals(1, function_call_original_map.size());
			assertEquals(2, function_call_copied_map.size());
			assertDeepEquals(Arrays.asList(null, testArrayAccessFunctionCall), function_call_original_list);
			assertDeepEquals(Arrays.asList(null, testArrayAccessFunctionCall, testArrayAccessFunctionCall), function_call_copied_list);
			
			assertDeepEquals(testArrayAccessFunctionCall, function_call_original_map.get("1"));
			assertDeepEquals(testArrayAccessFunctionCall, function_call_copied_map.get("1"));
			assertDeepEquals(testArrayAccessFunctionCall, function_call_copied_map.get("2"));
			assertDeepEquals(testArrayAccessFunctionCall, function_call_original_list.get(1));
			assertDeepEquals(testArrayAccessFunctionCall, function_call_copied_list.get(1));
			assertDeepEquals(testArrayAccessFunctionCall, function_call_copied_list.get(2));
			
			assertDeepCopy(testArrayAccessFunctionCall, function_call_original_map.get("1"));
			assertDeepCopy(testArrayAccessFunctionCall, function_call_copied_map.get("1"));
			assertDeepCopy(testArrayAccessFunctionCall, function_call_copied_map.get("2"));
			assertDeepCopy(testArrayAccessFunctionCall, function_call_original_list.get(1));
			assertDeepCopy(testArrayAccessFunctionCall, function_call_copied_list.get(1));
			assertDeepCopy(testArrayAccessFunctionCall, function_call_copied_list.get(2));
		}
	}
	
	@SuppressWarnings("unchecked")
	private void check_assignment_deepcopy_field_access_expression() {
		// field access
		Date testFieldAccessDate1 = (Date) getVariable("testFieldAccessDate1");
		String testFieldAccessString1 = (String) getVariable("testFieldAccessString1");
		List<Date> testFieldAccessDateList1 = (List<Date>) getVariable("testFieldAccessDateList1");
		List<String> testFieldAccessStringList1 = (List<String>) getVariable("testFieldAccessStringList1");
		Map<String, Date> testFieldAccessDateMap1 = (Map<String, Date>) getVariable("testFieldAccessDateMap1");
		Map<String, String> testFieldAccessStringMap1 = (Map<String, String>) getVariable("testFieldAccessStringMap1");
		DataRecord testFieldAccessRecord1 = (DataRecord) getVariable("testFieldAccessRecord1");
		DataRecord firstMultivalueOutput = outputRecords[4];
		DataRecord secondMultivalueOutput = outputRecords[5];
		DataRecord thirdMultivalueOutput = outputRecords[6];
		
		assertDeepEquals(testFieldAccessDate1, firstMultivalueOutput.getField("dateField").getValue());
		assertDeepEquals(testFieldAccessDate1, ((List<?>) firstMultivalueOutput.getField("dateListField").getValue()).get(0));
		assertDeepEquals(testFieldAccessString1, ((List<?>) firstMultivalueOutput.getField("stringListField").getValue()).get(0));
		assertDeepEquals(testFieldAccessDate1, ((Map<?, ?>) firstMultivalueOutput.getField("dateMapField").getValue()).get("first"));
		assertDeepEquals(testFieldAccessString1, ((Map<?, ?>) firstMultivalueOutput.getField("stringMapField").getValue()).get("first"));
		
		assertDeepEquals(testFieldAccessDateList1, secondMultivalueOutput.getField("dateListField").getValue());
		assertDeepEquals(testFieldAccessStringList1, secondMultivalueOutput.getField("stringListField").getValue());
		assertDeepEquals(testFieldAccessDateMap1, secondMultivalueOutput.getField("dateMapField").getValue());
		assertDeepEquals(testFieldAccessStringMap1, secondMultivalueOutput.getField("stringMapField").getValue());
		assertDeepEquals(testFieldAccessRecord1, thirdMultivalueOutput);
		
		assertDeepCopy(testFieldAccessDate1, firstMultivalueOutput.getField("dateField").getValue());
		assertDeepCopy(testFieldAccessDate1, ((List<?>) firstMultivalueOutput.getField("dateListField").getValue()).get(0));
		assertDeepCopy(testFieldAccessString1, ((List<?>) firstMultivalueOutput.getField("stringListField").getValue()).get(0));
		assertDeepCopy(testFieldAccessDate1, ((Map<?, ?>) firstMultivalueOutput.getField("dateMapField").getValue()).get("first"));
		assertDeepCopy(testFieldAccessString1, ((Map<?, ?>) firstMultivalueOutput.getField("stringMapField").getValue()).get("first"));
		assertDeepCopy(testFieldAccessDateList1, secondMultivalueOutput.getField("dateListField").getValue());
		assertDeepCopy(testFieldAccessStringList1, secondMultivalueOutput.getField("stringListField").getValue());
		assertDeepCopy(testFieldAccessDateMap1, secondMultivalueOutput.getField("dateMapField").getValue());
		assertDeepCopy(testFieldAccessStringMap1, secondMultivalueOutput.getField("stringMapField").getValue());
		assertDeepCopy(testFieldAccessRecord1, thirdMultivalueOutput);
	}
	
	@SuppressWarnings("unchecked")
	private void check_assignment_deepcopy_member_access_expression() {
		{
			// member access - record
			Date testMemberAccessDate1 = (Date) getVariable("testMemberAccessDate1");
			byte[] testMemberAccessByte1 = (byte[]) getVariable("testMemberAccessByte1");
			List<Date> testMemberAccessDateList1 = (List<Date>) getVariable("testMemberAccessDateList1");
			List<byte[]> testMemberAccessByteList1 = (List<byte[]>) getVariable("testMemberAccessByteList1");
			DataRecord testMemberAccessRecord1 = (DataRecord) getVariable("testMemberAccessRecord1");
			DataRecord testMemberAccessRecord2 = (DataRecord) getVariable("testMemberAccessRecord2");
			
			assertDeepEquals(testMemberAccessDate1, testMemberAccessRecord1.getField("dateField").getValue());
			assertDeepEquals(testMemberAccessByte1, testMemberAccessRecord1.getField("byteField").getValue());
			assertDeepEquals(testMemberAccessDate1, ((List<?>) testMemberAccessRecord1.getField("dateListField").getValue()).get(0));
			assertDeepEquals(testMemberAccessByte1, ((List<?>) testMemberAccessRecord1.getField("byteListField").getValue()).get(0));
			assertDeepEquals(testMemberAccessDateList1, testMemberAccessRecord2.getField("dateListField").getValue());
			assertDeepEquals(testMemberAccessByteList1, testMemberAccessRecord2.getField("byteListField").getValue());
			
			assertDeepCopy(testMemberAccessDate1, testMemberAccessRecord1.getField("dateField").getValue());
			assertDeepCopy(testMemberAccessByte1, testMemberAccessRecord1.getField("byteField").getValue());
			assertDeepCopy(testMemberAccessDate1, ((List<?>) testMemberAccessRecord1.getField("dateListField").getValue()).get(0));
			assertDeepCopy(testMemberAccessByte1, ((List<?>) testMemberAccessRecord1.getField("byteListField").getValue()).get(0));
			assertDeepCopy(testMemberAccessDateList1, testMemberAccessRecord2.getField("dateListField").getValue());
			assertDeepCopy(testMemberAccessByteList1, testMemberAccessRecord2.getField("byteListField").getValue());
		}

		{
			// member access - record
			Date testMemberAccessDate1 = (Date) getVariable("testMemberAccessDate1");
			byte[] testMemberAccessByte1 = (byte[]) getVariable("testMemberAccessByte1");
			List<Date> testMemberAccessDateList1 = (List<Date>) getVariable("testMemberAccessDateList1");
			List<byte[]> testMemberAccessByteList1 = (List<byte[]>) getVariable("testMemberAccessByteList1");
			DataRecord testMemberAccessRecord1 = (DataRecord) getVariable("testMemberAccessRecord1");
			DataRecord testMemberAccessRecord2 = (DataRecord) getVariable("testMemberAccessRecord2");
			DataRecord testMemberAccessRecord3 = (DataRecord) getVariable("testMemberAccessRecord3");
			
			assertDeepEquals(testMemberAccessDate1, testMemberAccessRecord1.getField("dateField").getValue());
			assertDeepEquals(testMemberAccessByte1, testMemberAccessRecord1.getField("byteField").getValue());
			assertDeepEquals(testMemberAccessDate1, ((List<?>) testMemberAccessRecord1.getField("dateListField").getValue()).get(0));
			assertDeepEquals(testMemberAccessByte1, ((List<?>) testMemberAccessRecord1.getField("byteListField").getValue()).get(0));
			assertDeepEquals(testMemberAccessDateList1, testMemberAccessRecord2.getField("dateListField").getValue());
			assertDeepEquals(testMemberAccessByteList1, testMemberAccessRecord2.getField("byteListField").getValue());
			assertDeepEquals(testMemberAccessRecord3, testMemberAccessRecord2);
			
			assertDeepCopy(testMemberAccessDate1, testMemberAccessRecord1.getField("dateField").getValue());
			assertDeepCopy(testMemberAccessByte1, testMemberAccessRecord1.getField("byteField").getValue());
			assertDeepCopy(testMemberAccessDate1, ((List<?>) testMemberAccessRecord1.getField("dateListField").getValue()).get(0));
			assertDeepCopy(testMemberAccessByte1, ((List<?>) testMemberAccessRecord1.getField("byteListField").getValue()).get(0));
			assertDeepCopy(testMemberAccessDateList1, testMemberAccessRecord2.getField("dateListField").getValue());
			assertDeepCopy(testMemberAccessByteList1, testMemberAccessRecord2.getField("byteListField").getValue());
			assertDeepCopy(testMemberAccessRecord3, testMemberAccessRecord2);

			// dictionary
			Date dictionaryDate = (Date) graph.getDictionary().getEntry("a").getValue();
			byte[] dictionaryByte = (byte[]) graph.getDictionary().getEntry("y").getValue();
			List<String> testMemberAccessStringList1 = (List<String>) getVariable("testMemberAccessStringList1");
			List<Date> testMemberAccessDateList2 = (List<Date>) getVariable("testMemberAccessDateList2");
			List<byte[]> testMemberAccessByteList2 = (List<byte[]>) getVariable("testMemberAccessByteList2");
			List<String> dictionaryStringList = (List<String>) graph.getDictionary().getValue("stringList");
			List<Date> dictionaryDateList = (List<Date>) graph.getDictionary().getValue("dateList");
			List<byte[]> dictionaryByteList = (List<byte[]>) graph.getDictionary().getValue("byteList");
			
			assertDeepEquals(dictionaryDate, testMemberAccessDate1);
			assertDeepEquals(dictionaryByte, testMemberAccessByte1);
			assertDeepEquals(dictionaryStringList, testMemberAccessStringList1);
			assertDeepEquals(dictionaryDateList, testMemberAccessDateList2);
			assertDeepEquals(dictionaryByteList, testMemberAccessByteList2);
			
			assertDeepCopy(dictionaryDate, testMemberAccessDate1);
			assertDeepCopy(dictionaryByte, testMemberAccessByte1);
			assertDeepCopy(dictionaryStringList, testMemberAccessStringList1);
			assertDeepCopy(dictionaryDateList, testMemberAccessDateList2);
			assertDeepCopy(dictionaryByteList, testMemberAccessByteList2);
			
			// member access - array of records
			List<DataRecord> testMemberAccessRecordList1 = (List<DataRecord>) getVariable("testMemberAccessRecordList1");
			
			assertDeepEquals(testMemberAccessDate1, testMemberAccessRecordList1.get(0).getField("dateField").getValue());
			assertDeepEquals(testMemberAccessByte1, testMemberAccessRecordList1.get(0).getField("byteField").getValue());
			assertDeepEquals(testMemberAccessDate1, ((List<Date>) testMemberAccessRecordList1.get(0).getField("dateListField").getValue()).get(0));
			assertDeepEquals(testMemberAccessByte1, ((List<byte[]>) testMemberAccessRecordList1.get(0).getField("byteListField").getValue()).get(0));
			assertDeepEquals(testMemberAccessDateList1, testMemberAccessRecordList1.get(1).getField("dateListField").getValue());
			assertDeepEquals(testMemberAccessByteList1, testMemberAccessRecordList1.get(1).getField("byteListField").getValue());
			assertDeepEquals(testMemberAccessRecordList1.get(1), testMemberAccessRecordList1.get(2));

			assertDeepCopy(testMemberAccessDate1, testMemberAccessRecordList1.get(0).getField("dateField").getValue());
			assertDeepCopy(testMemberAccessByte1, testMemberAccessRecordList1.get(0).getField("byteField").getValue());
			assertDeepCopy(testMemberAccessDate1, ((List<Date>) testMemberAccessRecordList1.get(0).getField("dateListField").getValue()).get(0));
			assertDeepCopy(testMemberAccessByte1, ((List<byte[]>) testMemberAccessRecordList1.get(0).getField("byteListField").getValue()).get(0));
			assertDeepCopy(testMemberAccessDateList1, testMemberAccessRecordList1.get(1).getField("dateListField").getValue());
			assertDeepCopy(testMemberAccessByteList1, testMemberAccessRecordList1.get(1).getField("byteListField").getValue());
			assertDeepCopy(testMemberAccessRecordList1.get(1), testMemberAccessRecordList1.get(2));

			// member access - map of records
			Map<Integer, DataRecord> testMemberAccessRecordMap1 = (Map<Integer, DataRecord>) getVariable("testMemberAccessRecordMap1");
			
			assertDeepEquals(testMemberAccessDate1, testMemberAccessRecordMap1.get(0).getField("dateField").getValue());
			assertDeepEquals(testMemberAccessByte1, testMemberAccessRecordMap1.get(0).getField("byteField").getValue());
			assertDeepEquals(testMemberAccessDate1, ((List<Date>) testMemberAccessRecordMap1.get(0).getField("dateListField").getValue()).get(0));
			assertDeepEquals(testMemberAccessByte1, ((List<byte[]>) testMemberAccessRecordMap1.get(0).getField("byteListField").getValue()).get(0));
			assertDeepEquals(testMemberAccessDateList1, testMemberAccessRecordMap1.get(1).getField("dateListField").getValue());
			assertDeepEquals(testMemberAccessByteList1, testMemberAccessRecordMap1.get(1).getField("byteListField").getValue());
			assertDeepEquals(testMemberAccessRecordMap1.get(1), testMemberAccessRecordMap1.get(2));
			
			assertDeepCopy(testMemberAccessDate1, testMemberAccessRecordMap1.get(0).getField("dateField").getValue());
			assertDeepCopy(testMemberAccessByte1, testMemberAccessRecordMap1.get(0).getField("byteField").getValue());
			assertDeepCopy(testMemberAccessDate1, ((List<Date>) testMemberAccessRecordMap1.get(0).getField("dateListField").getValue()).get(0));
			assertDeepCopy(testMemberAccessByte1, ((List<byte[]>) testMemberAccessRecordMap1.get(0).getField("byteListField").getValue()).get(0));
			assertDeepCopy(testMemberAccessDateList1, testMemberAccessRecordMap1.get(1).getField("dateListField").getValue());
			assertDeepCopy(testMemberAccessByteList1, testMemberAccessRecordMap1.get(1).getField("byteListField").getValue());
			assertDeepCopy(testMemberAccessRecordMap1.get(1), testMemberAccessRecordMap1.get(2));
			
		}
	}


	@SuppressWarnings("unchecked")
	public void test_assignment_deepcopy() {
		doCompile("test_assignment_deepcopy");
		
		List<DataRecord> secondRecordList = (List<DataRecord>) getVariable("secondRecordList"); 
		assertEquals("before", secondRecordList.get(0).getField("Name").getValue().toString());

		List<DataRecord> firstRecordList = (List<DataRecord>) getVariable("firstRecordList");
		assertEquals("after", firstRecordList.get(0).getField("Name").getValue().toString());
		
		check_assignment_deepcopy_variable_declaration();
		
		check_assignment_deepcopy_array_access_expression();
		
		check_assignment_deepcopy_field_access_expression();
		
		check_assignment_deepcopy_member_access_expression();
		
	}
	
	public void test_assignment_deepcopy_field_access_expression() {
		doCompile("test_assignment_deepcopy_field_access_expression");
		
		DataRecord testFieldAccessRecord1 = (DataRecord) getVariable("testFieldAccessRecord1");
		DataRecord firstMultivalueOutput = outputRecords[4];
		DataRecord secondMultivalueOutput = outputRecords[5];
		DataRecord thirdMultivalueOutput = outputRecords[6];
		DataRecord multivalueInput = inputRecords[3];

		assertDeepEquals(firstMultivalueOutput, testFieldAccessRecord1);
		assertDeepEquals(secondMultivalueOutput, multivalueInput);
		assertDeepEquals(thirdMultivalueOutput, secondMultivalueOutput);

		assertDeepCopy(firstMultivalueOutput, testFieldAccessRecord1);
		assertDeepCopy(secondMultivalueOutput, multivalueInput);
		assertDeepCopy(thirdMultivalueOutput, secondMultivalueOutput);
	}
	
	public void test_assignment_array_access_function_call() {
		doCompile("test_assignment_array_access_function_call");
		Map<String, String> originalMap = new HashMap<String, String>();
		originalMap.put("a", "b");
		
		Map<String, String> copiedMap = new HashMap<String, String>(originalMap);
		copiedMap.put("c", "d");
		
		check("originalMap", originalMap);
		check("copiedMap", copiedMap);
	}

	public void test_assignment_array_access_function_call_wrong_type() {
		doCompileExpectErrors("test_assignment_array_access_function_call_wrong_type", 
				Arrays.asList(
						"Expression is not a composite type but is resolved to 'string'",
						"Type mismatch: cannot convert from 'integer' to 'string'",
						"Cannot convert from 'integer' to string"
				));
	}

	@SuppressWarnings("unchecked")
	public void test_assignment_returnvalue() {
		doCompile("test_assignment_returnvalue");
		
		{
			List<String> stringList1 = (List<String>) getVariable("stringList1");
			List<String> stringList2 = (List<String>) getVariable("stringList2");
			List<String> stringList3 = (List<String>) getVariable("stringList3");
			List<DataRecord> recordList1 = (List<DataRecord>) getVariable("recordList1");
			Map<Integer, DataRecord> recordMap1 = (Map<Integer, DataRecord>) getVariable("recordMap1");
			List<String> stringList4 = (List<String>) getVariable("stringList4");
			Map<String, Integer> integerMap1 = (Map<String, Integer>) getVariable("integerMap1");
			DataRecord record1 = (DataRecord) getVariable("record1");
			DataRecord record2 = (DataRecord) getVariable("record2");
			DataRecord firstMultivalueOutput = outputRecords[4];
			DataRecord secondMultivalueOutput = outputRecords[5];
			DataRecord thirdMultivalueOutput = outputRecords[6];
			Date dictionaryDate1 = (Date) getVariable("dictionaryDate1");
			Date dictionaryDate = (Date) graph.getDictionary().getValue("a");
			Date zeroDate = new Date(0);
			List<String> testReturnValueDictionary2 = (List<String>) getVariable("testReturnValueDictionary2");
			List<String> dictionaryStringList = (List<String>) graph.getDictionary().getValue("stringList");
			List<String> testReturnValue10 = (List<String>) getVariable("testReturnValue10");
			DataRecord testReturnValue11 = (DataRecord) getVariable("testReturnValue11");
			List<String> testReturnValue12 = (List<String>) getVariable("testReturnValue12");
			List<String> testReturnValue13 = (List<String>) getVariable("testReturnValue13");
			Map<Integer, DataRecord> function_call_original_map = (Map<Integer, DataRecord>) getVariable("function_call_original_map");
			Map<Integer, DataRecord> function_call_copied_map = (Map<Integer, DataRecord>) getVariable("function_call_copied_map");
			DataRecord function_call_map_newrecord = (DataRecord) getVariable("function_call_map_newrecord");
			List<DataRecord> function_call_original_list = (List<DataRecord>) getVariable("function_call_original_list");
			List<DataRecord> function_call_copied_list = (List<DataRecord>) getVariable("function_call_copied_list");
			DataRecord function_call_list_newrecord = (DataRecord) getVariable("function_call_list_newrecord");
			
			// identifier
			assertFalse(stringList1.isEmpty());
			assertTrue(stringList2.isEmpty());
			assertTrue(stringList3.isEmpty());
			
			// array access expression - list
			assertDeepEquals("unmodified", recordList1.get(0).getField("stringField").getValue());
			assertDeepEquals("modified", recordList1.get(1).getField("stringField").getValue());

			// array access expression - map
			assertDeepEquals("unmodified", recordMap1.get(0).getField("stringField").getValue());
			assertDeepEquals("modified", recordMap1.get(1).getField("stringField").getValue());
			
			// array access expression - function call
			assertDeepEquals(null, function_call_original_map.get(2));
			assertDeepEquals("unmodified", function_call_map_newrecord.getField("stringField"));
			assertDeepEquals("modified", function_call_copied_map.get(2).getField("stringField"));
			assertDeepEquals(Arrays.asList(null, function_call_list_newrecord), function_call_original_list);
			assertDeepEquals("unmodified", function_call_list_newrecord.getField("stringField"));
			assertDeepEquals("modified", function_call_copied_list.get(2).getField("stringField"));

			// field access expression
			assertFalse(stringList4.isEmpty());
			assertTrue(((List<?>) firstMultivalueOutput.getField("stringListField").getValue()).isEmpty());
			assertFalse(integerMap1.isEmpty());
			assertTrue(((Map<?, ?>) firstMultivalueOutput.getField("integerMapField").getValue()).isEmpty());
			assertDeepEquals("unmodified", record1.getField("stringField"));
			assertDeepEquals("modified", secondMultivalueOutput.getField("stringField").getValue());
			assertDeepEquals("unmodified", record2.getField("stringField"));
			assertDeepEquals("modified", thirdMultivalueOutput.getField("stringField").getValue());
			
			// member access expression - dictionary
			// There is no function that could modify a date
//			assertEquals(zeroDate, dictionaryDate);  
//			assertFalse(zeroDate.equals(testReturnValueDictionary1));
			assertFalse(testReturnValueDictionary2.isEmpty());  
			assertTrue(dictionaryStringList.isEmpty());
			
			// member access expression - record
			assertFalse(testReturnValue10.isEmpty());
			assertTrue(((List<?>) testReturnValue11.getField("stringListField").getValue()).isEmpty());
			
			// member access expression - list of records
			assertFalse(testReturnValue12.isEmpty());
			assertTrue(((List<?>) recordList1.get(2).getField("stringListField").getValue()).isEmpty());

			// member access expression - map of records
			assertFalse(testReturnValue13.isEmpty());
			assertTrue(((List<?>) recordMap1.get(2).getField("stringListField").getValue()).isEmpty());
			
			
		}
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
		
		{
			Map<?, ?> preservedOrder = (Map<?, ?>) getVariable("preservedOrder");
			assertEquals(100, preservedOrder.size());
			int i = 0;
			for (Map.Entry<?, ?> entry: preservedOrder.entrySet()) {
				assertEquals("key" + i, entry.getKey());
				assertEquals("value" + i, entry.getValue());
				i++;
			}
		}
	}
	
	public void test_type_record_list() {
		doCompile("test_type_record_list");
		
		check("resultInt", 6);
		check("resultString", "string");
		check("resultInt2", 10);
		check("resultString2", "string2");
	}

	public void test_type_record_list_global() {
		doCompile("test_type_record_list_global");
		
		check("resultInt", 6);
		check("resultString", "string");
		check("resultInt2", 10);
		check("resultString2", "string2");
	}

	public void test_type_record_map() {
		doCompile("test_type_record_map");
		
		check("resultInt", 6);
		check("resultString", "string");
		check("resultInt2", 10);
		check("resultString2", "string2");
	}

	public void test_type_record_map_global() {
		doCompile("test_type_record_map_global");
		
		check("resultInt", 6);
		check("resultString", "string");
		check("resultInt2", 10);
		check("resultString2", "string2");
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
		
		// no modification by reference is possible
		assertTrue(recordEquals(expected, (DataRecord)getVariable("modified3")));
		expected.getField("Value").setValue(654321);
		assertTrue(recordEquals(expected, (DataRecord)getVariable("reference")));
		assertTrue(getVariable("modified3") != getVariable("reference"));
		
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
		check("splusm1", "hello" + Double.valueOf(0.001D));
		check("m1pluss", Double.valueOf(0.001D) + "hello");
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
				"Illegal argument to ++/-- operator",
				"Input record cannot be assigned to",
				"Input record cannot be assigned to",
				"Input record cannot be assigned to",
				"Input record cannot be assigned to"
		));
	}
	
	public void test_operator_equal() {
		doCompile("test_operator_equal");

		check("eq0", true);
		check("eq1", true);
		check("eq1a", true);
		check("eq1b", true);
		check("eq1c", false);
		check("eq2", true);
		check("eq3", true);
		check("eq4", true);
		check("eq5", true);
		check("eq6", false);
		check("eq7", true);
		check("eq8", false);
		check("eq9", true);
		check("eq10", false);
		check("eq11", true);
		check("eq12", false);
		check("eq13", true);
		check("eq14", false);
		check("eq15", false);
		check("eq16", true);
		check("eq17", false);
		check("eq18", false);
		check("eq19", false);
		// byte
		check("eq20", true);
		check("eq21", true);
		check("eq22", false);
		check("eq23", false);
		check("eq24", true);
		check("eq25", false);
		check("eq20c", true);
		check("eq21c", true);
		check("eq22c", false);
		check("eq23c", false);
		check("eq24c", true);
		check("eq25c", false);
		check("eq26", true);
		check("eq27", true);
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
		check("dned_different_scale", false);
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

	public void test_for1() {
		//5125: CTL2: "for" cycle is EXTREMELY memory consuming
		doCompile("test_for1");

		checkEquals("counter", "COUNT");
	}

	@SuppressWarnings("unchecked")
	public void test_foreach() {
		doCompile("test_foreach");
		check("intRes", Arrays.asList(VALUE_VALUE));
		check("longRes", Arrays.asList(BORN_MILLISEC_VALUE));
		check("doubleRes", Arrays.asList(AGE_VALUE));
		check("decimalRes", Arrays.asList(CURRENCY_VALUE));
		check("booleanRes", Arrays.asList(FLAG_VALUE));
		check("stringRes", Arrays.asList(NAME_VALUE, CITY_VALUE));
		check("dateRes", Arrays.asList(BORN_VALUE));
		
		List<?> integerStringMapResTmp = (List<?>) getVariable("integerStringMapRes");
		List<String> integerStringMapRes = new ArrayList<String>(integerStringMapResTmp.size());
		for (Object o: integerStringMapResTmp) {
			integerStringMapRes.add(String.valueOf(o));
		}
		List<Integer> stringIntegerMapRes = (List<Integer>) getVariable("stringIntegerMapRes");
		List<DataRecord> stringRecordMapRes = (List<DataRecord>) getVariable("stringRecordMapRes");
		
		Collections.sort(integerStringMapRes);
		Collections.sort(stringIntegerMapRes);
		
		assertEquals(Arrays.asList("0", "1", "2", "3", "4"), integerStringMapRes);
		assertEquals(Arrays.asList(0, 1, 2, 3, 4), stringIntegerMapRes);
		
		final int N = 5;
		assertEquals(N, stringRecordMapRes.size());
		int equalRecords = 0;
		for (int i = 0; i < N; i++) {
			for (DataRecord r: stringRecordMapRes) {
				if (Integer.valueOf(i).equals(r.getField("Value").getValue())
						&& "A string".equals(String.valueOf(r.getField("Name").getValue()))) {
					equalRecords++;
					break;
				}
			}
		}
		assertEquals(N, equalRecords);
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
	
	public void test_return_void() {
		doCompile("test_return_void");
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
		URL importLoc = getClass().getSuperclass().getResource("test_duplicate_import.ctl");
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
		
		check("len", 2);
	}

	public void test_mapping_null_values() {
		doCompile("test_mapping_null_values");

		assertTrue(recordEquals(inputRecords[2], outputRecords[0]));
	}

	public void test_copyByName() {
		doCompile("test_copyByName");
		assertEquals("Field1", null, outputRecords[3].getField("Field1").getValue());
		assertEquals("Age", AGE_VALUE, outputRecords[3].getField("Age").getValue());
		assertEquals("City", CITY_VALUE, outputRecords[3].getField("City").getValue().toString());
	}

	public void test_copyByName_assignment() {
		doCompile("test_copyByName_assignment");
		assertEquals("Field1", null, outputRecords[3].getField("Field1").getValue());
		assertEquals("Age", AGE_VALUE, outputRecords[3].getField("Age").getValue());
		assertEquals("City", CITY_VALUE, outputRecords[3].getField("City").getValue().toString());
	}

	public void test_copyByName_assignment1() {
		doCompile("test_copyByName_assignment1");
		assertEquals("Field1", null, outputRecords[3].getField("Field1").getValue());
		assertEquals("Age", null, outputRecords[3].getField("Age").getValue());
		assertEquals("City", null, outputRecords[3].getField("City").getValue());
	}

	public void test_sequence(){
		doCompile("test_sequence");
		check("intRes", Arrays.asList(0,1,2));
		check("longRes", Arrays.asList(Long.valueOf(0),Long.valueOf(1),Long.valueOf(2)));
		check("stringRes", Arrays.asList("0","1","2"));
		check("intCurrent", Integer.valueOf(2));
		check("longCurrent", Long.valueOf(2));
		check("stringCurrent", "2");
	}
	
	//TODO: If this test fails please double check whether the test is correct?
	public void test_lookup(){
        doCompile("test_lookup");
		check("alphaResult", Arrays.asList("Andorra la Vella","Andorra la Vella"));
		check("bravoResult", Arrays.asList("Bruxelles","Bruxelles"));
		check("charlieResult", Arrays.asList("Chamonix","Chodov","Chomutov","Chamonix","Chodov","Chomutov"));
		check("countResult", Arrays.asList(3,3));
		check("charlieUpdatedCount", 5);
		check("charlieUpdatedResult", Arrays.asList("Chamonix", "Cheb", "Chodov", "Chomutov", "Chrudim"));
		check("putResult", true);
		check("meta", null);
		check("meta2", null);
		check("meta3", null);
		check("meta4", null);
		check("strRet", "Bratislava");
		check("strRet2","Andorra la Vella");
		check("intRet", 0);
		check("intRet2", 1);
		check("meta7", null);
	}
	
	public void test_lookup_expect_error(){
		//CLO-1582
//		try {
//			doCompile("function integer transform(){string str = lookup(TestLookup).get('Alpha',2).City; return 0;}","test_lookup_expect_error");
//			fail();
//		} catch (Exception e) {
//			// do nothing
//		}
//		try {
//			doCompile("function integer transform(){lookup(TestLookup).count('Alpha',1); lookup(TestLookup).next(); lookup(TestLookup).next().City; return 0;}","test_lookup_expect_error");
//			fail();
//		} catch (Exception e) {
//			// do nothing
//		}
		try {
			doCompile("function integer transform(){lookupMetadata meta = null; lookup(TestLookup).put(meta); return 0;}","test_lookup_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){lookup(TestLookup).put(null); return 0;}","test_lookup_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
//------------------------- ContainerLib Tests---------------------
	
	public void test_containerlib_append() {
		doCompile("test_containerlib_append");
		check("appendElem", Integer.valueOf(10));
		check("appendList", Arrays.asList(1, 2, 3, 4, 5, 10));
		check("stringList", Arrays.asList("horse","is","pretty","scary"));
		check("stringList2", Arrays.asList("horse", null));
		check("stringList3", Arrays.asList("horse", ""));
		check("integerList1", Arrays.asList(1,2,3,4));
		check("integerList2", Arrays.asList(1,2,null));
		check("numberList1", Arrays.asList(0.21,1.1,2.2));
		check("numberList2", Arrays.asList(1.1,null));
		check("longList1", Arrays.asList(1l,2l,3L));
		check("longList2", Arrays.asList(9L,null));
		check("decList1", Arrays.asList(new BigDecimal("2.3"),new BigDecimal("4.5"),new BigDecimal("6.7")));
		check("decList2",Arrays.asList(new BigDecimal("1.1"), null));
	}

	public void test_containerlib_append_expect_error(){
		try {
			doCompile("function integer transform(){string[] listInput = null; append(listInput,'aa'); return 0;}","test_containerlib_append_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){byte[] listInput = null; append(listInput,str2byte('third', 'utf-8')); return 0;}","test_containerlib_append_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long[] listInput = null; append(listInput,15L); return 0;}","test_containerlib_append_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer[] listInput = null; append(listInput,12); return 0;}","test_containerlib_append_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal[] listInput = null; append(listInput,12.5d); return 0;}","test_containerlib_append_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){number[] listInput = null; append(listInput,12.36); return 0;}","test_containerlib_append_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	@SuppressWarnings("unchecked")
	public void test_containerlib_clear() {
		doCompile("test_containerlib_clear");

		assertTrue(((List<Integer>) getVariable("integerList")).isEmpty());
		assertTrue(((List<Integer>) getVariable("strList")).isEmpty());
		assertTrue(((List<Integer>) getVariable("longList")).isEmpty());
		assertTrue(((List<Integer>) getVariable("decList")).isEmpty());
		assertTrue(((List<Integer>) getVariable("numList")).isEmpty());
		assertTrue(((List<Integer>) getVariable("byteList")).isEmpty());
		assertTrue(((List<Integer>) getVariable("dateList")).isEmpty());
		assertTrue(((List<Integer>) getVariable("boolList")).isEmpty());
		assertTrue(((List<Integer>) getVariable("emptyList")).isEmpty());
		assertTrue(((Map<String,Integer>) getVariable("myMap")).isEmpty());
	}
	
	public void test_container_clear_expect_error(){
		try {
			doCompile("function integer transform(){boolean[] nullList = null; clear(nullList); return 0;}","test_container_clear_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){map[integer,string] myMap = null; clear(myMap); return 0;}","test_container_clear_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_containerlib_copy() {
		doCompile("test_containerlib_copy");

		check("copyIntList", Arrays.asList(1, 2, 3, 4, 5));
		check("returnedIntList", Arrays.asList(1, 2, 3, 4, 5));
		check("copyLongList", Arrays.asList(21L,15L, null, 10L));
		check("returnedLongList", Arrays.asList(21l, 15l, null, 10L));
		check("copyBoolList", Arrays.asList(false,false,null,true));
		check("returnedBoolList", Arrays.asList(false,false,null,true));
		Calendar cal = Calendar.getInstance();
		cal.set(2006, 10, 12, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Calendar cal2 = Calendar.getInstance();
		cal2.set(2002, 03, 12, 0, 0, 0);
		cal2.set(Calendar.MILLISECOND, 0);
		check("copyDateList",Arrays.asList(cal2.getTime(), null, cal.getTime()));
		check("returnedDateList",Arrays.asList(cal2.getTime(), null, cal.getTime()));
		check("copyStrList", Arrays.asList("Ashe", "Jax", null, "Rengar"));
		check("returnedStrList", Arrays.asList("Ashe", "Jax", null, "Rengar"));
		check("copyNumList", Arrays.asList(12.65d, 458.3d, null, 15.65d));
		check("returnedNumList", Arrays.asList(12.65d, 458.3d, null, 15.65d));
		check("copyDecList", Arrays.asList(new BigDecimal("2.3"),new BigDecimal("5.9"), null, new BigDecimal("15.3")));
		check("returnedDecList", Arrays.asList(new BigDecimal("2.3"),new BigDecimal("5.9"), null, new BigDecimal("15.3")));
		
		Map<String, String> expectedMap = new HashMap<String, String>();
		expectedMap.put("a", "a");
		expectedMap.put("b", "b");
		expectedMap.put("c", "c");
		expectedMap.put("d", "d");
		check("copyStrMap", expectedMap);
		check("returnedStrMap", expectedMap);
		
		Map<Integer, Integer> intMap = new HashMap<Integer, Integer>();
		intMap.put(1,12);
		intMap.put(2,null);
		intMap.put(3,15);
		check("copyIntMap", intMap);
		check("returnedIntMap", intMap);
		
		Map<Long, Long> longMap = new HashMap<Long, Long>();
		longMap.put(10L, 453L);
		longMap.put(11L, null);
		longMap.put(12L, 54755L);
		check("copyLongMap", longMap);
		check("returnedLongMap", longMap);

		Map<BigDecimal, BigDecimal> decMap = new HashMap<BigDecimal, BigDecimal>();
		decMap.put(new BigDecimal("2.2"), new BigDecimal("12.3"));
		decMap.put(new BigDecimal("2.3"), new BigDecimal("45.6"));
		check("copyDecMap", decMap);
		check("returnedDecMap", decMap);
		
		Map<Double, Double> doubleMap = new HashMap<Double, Double>();
		doubleMap.put(new Double(12.3d), new Double(11.2d));
		doubleMap.put(new Double(13.4d), new Double(78.9d));
		check("copyNumMap",doubleMap);	
		check("returnedNumMap", doubleMap);
		
		List<String> myList = new ArrayList<String>();
		check("copyEmptyList", myList);
		check("returnedEmptyList", myList);
		assertTrue(((List<String>)(getVariable("copyEmptyList"))).isEmpty());
		assertTrue(((List<String>)(getVariable("returnedEmptyList"))).isEmpty());
		Map<String, String> emptyMap = new HashMap<String, String>();
		check("copyEmptyMap", emptyMap);
		check("returnedEmptyMap", emptyMap);
		assertTrue(((HashMap<String,String>)(getVariable("copyEmptyMap"))).isEmpty());
		assertTrue(((HashMap<String,String>)(getVariable("returnedEmptyMap"))).isEmpty());
	}

	public void test_containerlib_copy_expect_error(){
		try {
			doCompile("function integer transform(){string[] origList = null; string[] copyList; string[] ret = copy(copyList, origList); return 0;}","test_containerlib_copy_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string[] origList; string[] copyList = null; string[] ret = copy(copyList, origList); return 0;}","test_containerlib_copy_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){map[string, string] orig = null; map[string, string] copy; map[string, string] ret = copy(copy, orig); return 0;}","test_containerlib_copy_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){map[string, string] orig; map[string, string] copy = null; map[string, string] ret = copy(copy, orig); return 0;}","test_containerlib_copy_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_containerlib_insert() {
		doCompile("test_containerlib_insert");
		check("copyStrList",Arrays.asList("Elise","Volibear","Garen","Jarvan IV"));
		check("retStrList",Arrays.asList("Elise","Volibear","Garen","Jarvan IV"));
		check("copyStrList2", Arrays.asList("Jax", "Aatrox", "Lisandra", "Ashe"));
		check("retStrList2", Arrays.asList("Jax", "Aatrox", "Lisandra", "Ashe"));
		Calendar cal = Calendar.getInstance();
		cal.set(2009, 10, 12, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Calendar cal1 = Calendar.getInstance();
		cal1.set(2008, 2, 7, 0, 0, 0);
		cal1.set(Calendar.MILLISECOND, 0);
		Calendar cal2 = Calendar.getInstance();
		cal2.set(2003, 01, 1, 0, 0, 0);
		cal2.set(Calendar.MILLISECOND, 0);
		check("copyDateList", Arrays.asList(cal1.getTime(),cal.getTime()));
		check("retDateList", Arrays.asList(cal1.getTime(),cal.getTime()));
		check("copyDateList2", Arrays.asList(cal2.getTime(),cal1.getTime(),cal.getTime()));
		check("retDateList2", Arrays.asList(cal2.getTime(),cal1.getTime(),cal.getTime()));
		check("copyIntList", Arrays.asList(1,2,3,12,4,5,6,7));
		check("retIntList", Arrays.asList(1,2,3,12,4,5,6,7));
		check("copyLongList", Arrays.asList(14L,15l,16l,17l));
		check("retLongList", Arrays.asList(14L,15l,16l,17l));
		check("copyLongList2", Arrays.asList(20L,21L,22L,23l));
		check("retLongList2", Arrays.asList(20L,21L,22L,23l));
		check("copyNumList", Arrays.asList(12.3d,11.1d,15.4d));
		check("retNumList", Arrays.asList(12.3d,11.1d,15.4d));
		check("copyNumList2", Arrays.asList(22.2d,44.4d,55.5d,33.3d));
		check("retNumList2", Arrays.asList(22.2d,44.4d,55.5d,33.3d));
		check("copyDecList", Arrays.asList(new BigDecimal("11.1"), new BigDecimal("22.2"), new BigDecimal("33.3")));
		check("retDecList", Arrays.asList(new BigDecimal("11.1"), new BigDecimal("22.2"), new BigDecimal("33.3")));
		check("copyDecList2",Arrays.asList(new BigDecimal("3.3"), new BigDecimal("4.4"), new BigDecimal("1.1"), new BigDecimal("2.2")));
		check("retDecList2",Arrays.asList(new BigDecimal("3.3"), new BigDecimal("4.4"), new BigDecimal("1.1"), new BigDecimal("2.2")));
		check("copyEmpty", Arrays.asList(11));
		check("retEmpty", Arrays.asList(11));
		check("copyEmpty2", Arrays.asList(12,13));
		check("retEmpty2", Arrays.asList(12,13));
		check("copyEmpty3", Arrays.asList());
		check("retEmpty3", Arrays.asList());
	}

	public void test_containerlib_insert_expect_error(){
		try {
			doCompile("function integer transform(){integer[] tmp = null; integer[] ret = insert(tmp,0,12); return 0;}","test_containerlib_insert_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer[] tmp; integer[] toAdd = null; integer[] ret = insert(tmp,0,toAdd); return 0;}","test_containerlib_insert_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer[] tmp = [11,12]; integer[] ret = insert(tmp,-1,12); return 0;}","test_containerlib_insert_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer[] tmp = [11,12]; integer[] ret = insert(tmp,10,12); return 0;}","test_containerlib_insert_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_containerlib_isEmpty() {
		doCompile("test_containerlib_isEmpty");
		check("emptyMap", true);
		check("emptyMap1", true);
		check("fullMap", false);
		check("fullMap1", false);
		check("emptyList", true);
		check("emptyList1", true);
		check("fullList", false);
		check("fullList1", false);
	}

	public void test_containerlib_isEmpty_expect_error(){
		try {
			doCompile("function integer transform(){integer[] i = null; boolean boo = i.isEmpty(); return 0;}","test_containerlib_isEmpty_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){map[string, string] m = null; boolean boo = m.isEmpty(); return 0;}","test_containerlib_isEmpty_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_containerlib_length(){
		doCompile("test_containerlib_length");
		check("lengthByte", 18);
		check("lengthByte2", 18);
		check("recordLength", 9);
		check("recordLength2", 9);
		check("listLength", 3);
		check("listLength2", 3);
		check("emptyListLength", 0);
		check("emptyListLength2", 0);
		check("emptyMapLength", 0);
		check("emptyMapLength2", 0);
		check("nullLength1", 0);
		check("nullLength2", 0);
		check("nullLength3", 0);
		check("nullLength4", 0);
		check("nullLength5", 0);
		check("nullLength6", 0);
	}
	
	public void test_containerlib_poll() throws UnsupportedEncodingException {
		doCompile("test_containerlib_poll");

		check("intElem", Integer.valueOf(1));
		check("intElem1", 2);
		check("intList", Arrays.asList(3, 4, 5));
		check("strElem", "Zyra");
		check("strElem2", "Tresh");
		check("strList", Arrays.asList("Janna", "Wu Kong"));
		Calendar cal = Calendar.getInstance();
		cal.set(2002, 10, 12, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);
		check("dateElem", cal.getTime());
		cal.clear();
		cal.set(2003,5,12,0,0,0);
		cal.set(Calendar.MILLISECOND, 0);
		check("dateElem2", cal.getTime());
		cal.clear();
		cal.set(2006,9,15,0,0,0);
		cal.set(Calendar.MILLISECOND, 0);
		check("dateList", Arrays.asList(cal.getTime()));
		checkArray("byteElem", "Maoki".getBytes("UTF-8"));
		checkArray("byteElem2", "Nasus".getBytes("UTF-8"));
		check("longElem", 12L);
		check("longElem2", 15L);
		check("longList", Arrays.asList(16L,23L));
		check("numElem", 23.6d);
		check("numElem2", 15.9d);
		check("numList", Arrays.asList(78.8d, 57.2d));
		check("decElem", new BigDecimal("12.3"));
		check("decElem2", new BigDecimal("23.4"));
		check("decList", Arrays.asList(new BigDecimal("34.5"), new BigDecimal("45.6")));
		check("emptyElem", null);
		check("emptyElem2", null);
		check("emptyList", Arrays.asList());
	}

	public void test_containerlib_poll_expect_error(){
		try {
			doCompile("function integer transform(){integer[] arr = null; integer i = poll(arr); return 0;}","test_containerlib_poll_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer[] arr = null; integer i = arr.poll(); return 0;}","test_containerlib_poll_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_containerlib_pop() {
		doCompile("test_containerlib_pop");

		check("intElem", 5);
		check("intElem2", 4);
		check("intList", Arrays.asList(1, 2, 3));
		check("longElem", 14L);
		check("longElem2", 13L);
		check("longList", Arrays.asList(11L,12L));
		check("numElem", 11.5d);
		check("numElem2", 11.4d);
		check("numList", Arrays.asList(11.2d,11.3d));
		check("decElem", new BigDecimal("22.5"));
		check("decElem2", new BigDecimal("22.4"));
		check("decList", Arrays.asList(new BigDecimal("22.2"), new BigDecimal("22.3")));
		Calendar cal = Calendar.getInstance();
		cal.set(2005, 8, 24, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);
		check("dateElem",cal.getTime());
		cal.clear();
		cal.set(2001, 6, 13, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);
		check("dateElem2", cal.getTime());
		cal.clear();
		cal.set(2010, 5, 11, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Calendar cal2 = Calendar.getInstance();
		cal2.set(2011,3,3,0,0,0);
		cal2.set(Calendar.MILLISECOND, 0);
		check("dateList", Arrays.asList(cal.getTime(),cal2.getTime()));
		check("strElem", "Ezrael");
		check("strElem2", null);
		check("strList", Arrays.asList("Kha-Zix", "Xerath"));
		check("emptyElem", null);
		check("emptyElem2", null);
	}
	
	public void test_containerlib_pop_expect_error(){
		try {
			doCompile("function integer transform(){string[] arr = null; string str = pop(arr); return 0;}","test_containerlib_pop_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string[] arr = null; string str = arr.pop(); return 0;}","test_containerlib_pop_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}

	@SuppressWarnings("unchecked")
	public void test_containerlib_push() {
		doCompile("test_containerlib_push");

		check("intCopy", Arrays.asList(1, 2, 3));
		check("intRet", Arrays.asList(1, 2, 3));
		check("longCopy", Arrays.asList(12l,13l,14l));
		check("longRet", Arrays.asList(12l,13l,14l));
		check("numCopy", Arrays.asList(11.1d,11.2d,11.3d));
		check("numRet", Arrays.asList(11.1d,11.2d,11.3d));
		check("decCopy", Arrays.asList(new BigDecimal("12.2"), new BigDecimal("12.3"), new BigDecimal("12.4")));
		check("decRet", Arrays.asList(new BigDecimal("12.2"), new BigDecimal("12.3"), new BigDecimal("12.4")));
		check("strCopy", Arrays.asList("Fiora", "Nunu", "Amumu"));
		check("strRet", Arrays.asList("Fiora", "Nunu", "Amumu"));
		Calendar cal = Calendar.getInstance();
		cal.set(2001, 5, 9, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Calendar cal1 = Calendar.getInstance();
		cal1.set(2005, 5, 9, 0, 0, 0);
		cal1.set(Calendar.MILLISECOND, 0);
		Calendar cal2 = Calendar.getInstance();
		cal2.set(2011, 5, 9, 0, 0, 0);
		cal2.set(Calendar.MILLISECOND, 0);
		check("dateCopy", Arrays.asList(cal.getTime(),cal1.getTime(),cal2.getTime()));
		check("dateRet", Arrays.asList(cal.getTime(),cal1.getTime(),cal2.getTime()));
		String str = null;
		check("emptyCopy", Arrays.asList(str));
		check("emptyRet", Arrays.asList(str));
		// there is hardly any way to get an instance of DataRecord
		// hence we just check if the list has correct size
		// and if its elements have correct metadata
		List<DataRecord> recordList = (List<DataRecord>) getVariable("recordList");
		List<DataRecordMetadata> mdList = Arrays.asList(
				graph.getDataRecordMetadata(OUTPUT_1),
				graph.getDataRecordMetadata(INPUT_2),
				graph.getDataRecordMetadata(INPUT_1)
		);
		assertEquals(mdList.size(), recordList.size());
		for (int i = 0; i < mdList.size(); i++) {
			assertEquals(mdList.get(i), recordList.get(i).getMetadata());
		}
	}

	public void test_containerlib_push_expect_error(){
		try {
			doCompile("function integer transform(){string[] str = null; str.push('a'); return 0;}","test_containerlib_push_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string[] str = null; push(str, 'a'); return 0;}","test_containerlib_push_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_containerlib_remove() {
		doCompile("test_containerlib_remove");

		check("intElem", 2);
		check("intList", Arrays.asList(1, 3, 4, 5));
		check("longElem", 13L);
		check("longList", Arrays.asList(11l,12l,14l));
		check("numElem", 11.3d);
		check("numList", Arrays.asList(11.1d,11.2d,11.4d));
		check("decElem", new BigDecimal("11.3"));
		check("decList", Arrays.asList(new BigDecimal("11.1"),new BigDecimal("11.2"),new BigDecimal("11.4")));
		Calendar cal = Calendar.getInstance();
		cal.set(2002,10,13,0,0,0);
		cal.set(Calendar.MILLISECOND, 0);
		check("dateElem", cal.getTime());
		cal.clear();
		cal.set(2001,10,13,0,0,0);
		cal.set(Calendar.MILLISECOND, 0);
		Calendar cal2 = Calendar.getInstance();
		cal2.set(2003,10,13,0,0,0);
		cal2.set(Calendar.MILLISECOND, 0);
		check("dateList", Arrays.asList(cal.getTime(), cal2.getTime()));
		check("strElem", "Shivana");
		check("strList", Arrays.asList("Annie","Lux"));
	}

	public void test_containerlib_remove_expect_error(){
		try {
			doCompile("function integer transform(){string[] strList; string str = remove(strList,0); return 0;}","test_containerlib_remove_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string[] strList; string str = strList.remove(0); return 0;}","test_containerlib_remove_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string[] strList = ['Teemo']; string str = remove(strList,5); return 0;}","test_containerlib_remove_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string[] strList = ['Teemo']; string str = remove(strList,-1); return 0;}","test_containerlib_remove_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string[] strList = null; string str = remove(strList,0); return 0;}","test_containerlib_remove_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_containerlib_reverse_expect_error(){
		try {
			doCompile("function integer transform(){long[] longList = null; reverse(longList); return 0;}","test_containerlib_reverse_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long[] longList = null; long[] reversed = longList.reverse(); return 0;}","test_containerlib_reverse_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_containerlib_reverse() {
		doCompile("test_containerlib_reverse");

		check("intList", Arrays.asList(5, 4, 3, 2, 1));
		check("intList2", Arrays.asList(5, 4, 3, 2, 1));
		check("longList", Arrays.asList(14l,13l,12l,11l));
		check("longList2", Arrays.asList(14l,13l,12l,11l));
		check("numList", Arrays.asList(1.3d,1.2d,1.1d));
		check("numList2", Arrays.asList(1.3d,1.2d,1.1d));
		check("decList", Arrays.asList(new BigDecimal("1.3"),new BigDecimal("1.2"),new BigDecimal("1.1")));
		check("decList2", Arrays.asList(new BigDecimal("1.3"),new BigDecimal("1.2"),new BigDecimal("1.1")));
		check("strList", Arrays.asList(null,"Lulu","Kog Maw"));
		check("strList2", Arrays.asList(null,"Lulu","Kog Maw"));
		Calendar cal = Calendar.getInstance();
		cal.set(2001,2,1,0,0,0);
		cal.set(Calendar.MILLISECOND, 0);
		Calendar cal2 = Calendar.getInstance();
		cal2.set(2002,2,1,0,0,0);
		cal2.set(Calendar.MILLISECOND, 0);
		check("dateList", Arrays.asList(cal2.getTime(),cal.getTime()));
		check("dateList2", Arrays.asList(cal2.getTime(),cal.getTime()));
	}

	public void test_containerlib_sort() {
		doCompile("test_containerlib_sort");

		check("intList", Arrays.asList(1, 1, 2, 3, 5));
		check("intList2", Arrays.asList(1, 1, 2, 3, 5));
		check("longList", Arrays.asList(21l,22l,23l,24l));
		check("longList2", Arrays.asList(21l,22l,23l,24l));
		check("decList", Arrays.asList(new BigDecimal("1.1"),new BigDecimal("1.2"),new BigDecimal("1.3"),new BigDecimal("1.4")));
		check("decList2", Arrays.asList(new BigDecimal("1.1"),new BigDecimal("1.2"),new BigDecimal("1.3"),new BigDecimal("1.4")));
		check("numList", Arrays.asList(1.1d,1.2d,1.3d,1.4d));
		check("numList2", Arrays.asList(1.1d,1.2d,1.3d,1.4d));
		Calendar cal = Calendar.getInstance();
		cal.set(2002,5,12,0,0,0);
		cal.set(Calendar.MILLISECOND, 0);
		Calendar cal2 = Calendar.getInstance();
		cal2.set(2003,5,12,0,0,0);
		cal2.set(Calendar.MILLISECOND, 0);
		Calendar cal3 = Calendar.getInstance();
		cal3.set(2004,5,12,0,0,0);
		cal3.set(Calendar.MILLISECOND, 0);
		check("dateList", Arrays.asList(cal.getTime(),cal2.getTime(),cal3.getTime()));
		check("dateList2", Arrays.asList(cal.getTime(),cal2.getTime(),cal3.getTime()));
		check("strList", Arrays.asList("","Alistar", "Nocturne", "Soraka"));
		check("strList2", Arrays.asList("","Alistar", "Nocturne", "Soraka"));
		check("emptyList", Arrays.asList());
		check("emptyList2", Arrays.asList());
	}

	public void test_containerlib_sort_expect_error(){
		try {
			doCompile("function integer transform(){string[] strList = ['Renektor', null, 'Jayce']; sort(strList); return 0;}","test_containerlib_sort_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string[] strList = null; sort(strList); return 0;}","test_containerlib_sort_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_containerlib_containsAll() {
		doCompile("test_containerlib_containsAll");

		check("results", Arrays.asList(true, false, true, false, true, true, true, false, true, true, false));
		check("test1", true);
		check("test2", true);
		check("test3", true);
		check("test4", false);
		check("test5", true);
		check("test6", false);
		check("test7", true);
		check("test8", false);
		check("test9", true);
		check("test10", false);
		check("test11", true);
		check("test12", false);
		check("test13", false);
		check("test14", true);
		check("test15", false);
		check("test16", false);
	}
	
	public void test_containerlib_containsAll_expect_error(){
		try {
			doCompile("function integer transform(){integer[] intList = null; boolean b =intList.containsAll([1]); return 0;}","test_containerlib_containsAll_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}

	public void test_containerlib_containsKey() {
		doCompile("test_containerlib_containsKey");

		check("results", Arrays.asList(false, true, false, true, false, true));
		check("test1", true);
		check("test2", false);
		check("test3", true);
		check("test4", false);
		check("test5", true);
		check("test6", false);
		check("test7", true);
		check("test8", true);
		check("test9", true);
		check("test10", false);
		check("test11", true);
		check("test12", true);
		check("test13", false);
		check("test14", true);
		check("test15", true);
		check("test16", false);
		check("test17", true);
		check("test18", true);
		check("test19", false);
		check("test20", false);
	}
	
	public void test_containerlib_containsKey_expect_error(){
		try {
			doCompile("function integer transform(){map[string, integer] emptyMap = null; boolean b = emptyMap.containsKey('a'); return 0;}","test_containerlib_containsKey_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}

	public void test_containerlib_containsValue() {
		doCompile("test_containerlib_containsValue");

		check("results", Arrays.asList(true, false, false, true, false, false, true, false));
		check("test1", true);
		check("test2", true);
		check("test3", false);
		check("test4", true);
		check("test5", true);
		check("test6", false);
		check("test7", false);
		check("test8", true);
		check("test9", true);
		check("test10", false);
		check("test11", true);
		check("test12", true);
		check("test13", false);
		check("test14", true);
		check("test15", true);
		check("test16", false);
		check("test17", true);
		check("test18", true);
		check("test19", false);
		check("test20", true);
		check("test21", true);
		check("test22", false);
		check("test23", true);
		check("test24", true);
		check("test25", false);
		check("test26", false);
	}
	
	public void test_convertlib_containsValue_expect_error(){
		try {
			doCompile("function integer transform(){map[integer, long] nullMap = null; boolean b = nullMap.containsValue(18L); return 0;}","test_convertlib_containsValue_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}

	public void test_containerlib_getKeys() {
		doCompile("test_containerlib_getKeys");
		check("stringList", Arrays.asList("a","b"));
		check("stringList2", Arrays.asList("a","b"));
		check("integerList", Arrays.asList(5,7,2));
		check("integerList2", Arrays.asList(5,7,2));
		List<Date> list = new ArrayList<Date>();
		Calendar cal = Calendar.getInstance();
		cal.set(2008, 10, 12, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Calendar cal2 = Calendar.getInstance();
		cal2.set(2001, 5, 28, 0, 0, 0);
		cal2.set(Calendar.MILLISECOND, 0);
		list.add(cal.getTime());
		list.add(cal2.getTime());
		check("dateList", list);
		check("dateList2", list);
		check("longList", Arrays.asList(14L, 45L));
		check("longList2", Arrays.asList(14L, 45L));
		check("numList", Arrays.asList(12.3d, 13.4d));
		check("numList2", Arrays.asList(12.3d, 13.4d));
		check("decList", Arrays.asList(new BigDecimal("34.5"), new BigDecimal("45.6")));
		check("decList2", Arrays.asList(new BigDecimal("34.5"), new BigDecimal("45.6")));
		check("emptyList", Arrays.asList());
		check("emptyList2", Arrays.asList());
	}
	
	public void test_containerlib_getKeys_expect_error(){
		try {
			doCompile("function integer transform(){map[string,string] strMap = null; string[] str = strMap.getKeys(); return 0;}","test_containerlib_getKeys_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){map[string,string] strMap = null; string[] str = getKeys(strMap); return 0;}","test_containerlib_getKeys_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
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
	
	public void test_stringlib_charAt_error(){
		//test: attempt to access char at position, which is out of bounds -> upper bound 
		try {
			doCompile("string test;function integer transform(){test = charAt('milk', 7);return 0;}", "test_stringlib_charAt_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: attempt to access char at position, which is out of bounds -> lower bound 
		try {
			doCompile("string test;function integer transform(){test = charAt('milk', -1);return 0;}", "test_stringlib_charAt_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: argument for position is null
		try {
			doCompile("string test; integer i = null; function integer transform(){test = charAt('milk', i);return 0;}", "test_stringlib_charAt_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: input is null
		try {
			doCompile("string test;function integer transform(){test = charAt(null, 1);return 0;}", "test_stringlib_charAt_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: input is empty string
		try {
			doCompile("string test;function integer transform(){test = charAt('', 1);return 0;}", "test_stringlib_charAt_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_stringlib_concat() {
		doCompile("test_stringlib_concat");
		
		final SimpleDateFormat format = new SimpleDateFormat();
		format.applyPattern("yyyy MMM dd");
				
		check("concat", "");
		check("concat1", "ello hi   ELLO 2,today is " + format.format(new Date()));
		check("concat2", "");
		check("concat3", "clover");
		check("test_null1", "null");
		check("test_null2", "null");
		check("test_null3","skynullisnullblue");
	}
	
	public void test_stringlib_countChar() {
		doCompile("test_stringlib_countChar");
		check("charCount", 3);
		check("count2", 0);
	}
	
	public void test_stringlib_countChar_emptychar() {
		// test: attempt to count empty chars in string.
		try {
			doCompile("integer charCount;function integer transform() {charCount = countChar('aaa','');return 0;}", "test_stringlib_countChar_emptychar");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		// test: attempt to count empty chars in empty string.
		try {
			doCompile("integer charCount;function integer transform() {charCount = countChar('','');return 0;}", "test_stringlib_countChar_emptychar");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: null input - test 1
		try {
			doCompile("integer charCount;function integer transform() {charCount = countChar(null,'a');return 0;}", "test_stringlib_countChar_emptychar");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: null input - test 2
		try {
			doCompile("integer charCount;function integer transform() {charCount = countChar(null,'');return 0;}", "test_stringlib_countChar_emptychar");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: null input - test 3
		try {
			doCompile("integer charCount;function integer transform() {charCount = countChar(null, null);return 0;}", "test_stringlib_countChar_emptychar");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		
	}
	
	public void test_stringlib_cut() {
		doCompile("test_stringlib_cut");
		check("cutInput", Arrays.asList("a", "1edf", "h3ijk"));
	}
	
	public void test_string_cut_expect_error() {
		// test: Attempt to cut substring from position after the end of original string. E.g. string is 6 char long and
		// user attempt to cut out after position 8.
		try {
			doCompile("string input;string[] cutInput;function integer transform() {input = 'abc1edf2geh3ijk10lmn999opq';cutInput = cut(input,[28,3]);return 0;}", "test_stringlib_cut_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		// test: Attempt to cut substring longer then possible. E.g. string is 6 characters long and user cuts from
		// position
		// 4 substring 4 characters long
		try {
			doCompile("string input;string[] cutInput;function integer transform() {input = 'abc1edf2geh3ijk10lmn999opq';cutInput = cut(input,[20,8]);return 0;}", "test_stringlib_cut_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		// test: Attempt to cut a substring with negative length
		try {
			doCompile("string input;string[] cutInput;function integer transform() {input = 'abc1edf2geh3ijk10lmn999opq';cutInput = cut(input,[20,-3]);return 0;}", "test_stringlib_cut_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		// test: Attempt to cut substring from negative position. E.g cut([-3,3]).
		try {
			doCompile("string input;string[] cutInput;function integer transform() {input = 'abc1edf2geh3ijk10lmn999opq';cutInput = cut(input,[-3,3]);return 0;}", "test_stringlib_cut_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: input is empty string
		try {
			doCompile("string input;string[] cutInput;function integer transform() {input = '';cutInput = cut(input,[0,3]);return 0;}", "test_stringlib_cut_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: second arg is null
		try {
			doCompile("string input;string[] cutInput;function integer transform() {input = 'aaaa';cutInput = cut(input,null);return 0;}", "test_stringlib_cut_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: input is null
		try {
			doCompile("string input;string[] cutInput;function integer transform() {input = null;cutInput = cut(input,[5,11]);return 0;}", "test_stringlib_cut_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
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
	
	public void test_stringlib_editDistance_expect_error(){
		//test: input - empty string - first arg
		try {
			doCompile("integer test;function integer transform() {test = editDistance('','mark');return 0;}","test_stringlib_editDistance_expect_error");
		} catch ( Exception e) {
			// do nothing
		}
		//test: input - null - first arg
		try {
			doCompile("integer test;function integer transform() {test = editDistance(null,'mark');return 0;}","test_stringlib_editDistance_expect_error");
		} catch ( Exception e) {
			// do nothing
		}
		//test: input- empty string - second arg
		try {
			doCompile("integer test;function integer transform() {test = editDistance('mark','');return 0;}","test_stringlib_editDistance_expect_error");
		} catch ( Exception e) {
			// do nothing
		}
		//test: input - null - second argument
		try {
			doCompile("integer test;function integer transform() {test = editDistance('mark',null);return 0;}","test_stringlib_editDistance_expect_error");
		} catch ( Exception e) {
			// do nothing
		}
		//test: input - both empty
		try {
			doCompile("integer test;function integer transform() {test = editDistance('','');return 0;}","test_stringlib_editDistance_expect_error");
		} catch ( Exception e) {
			// do nothing
		}
		//test: input - both null
		try {
			doCompile("integer test;function integer transform() {test = editDistance(null,null);return 0;}","test_stringlib_editDistance_expect_error");
		} catch ( Exception e) {
			// do nothing
		}
	}
	
	public void test_stringlib_find() {
		doCompile("test_stringlib_find");
		check("findList", Arrays.asList("The quick br", "wn f", "x jumps ", "ver the lazy d", "g"));
		check("findList2", Arrays.asList("mark.twain"));
		check("findList3", Arrays.asList());
		check("findList4", Arrays.asList("", "", "", "", ""));
		check("findList5", Arrays.asList("twain"));
		check("findList6", Arrays.asList(""));
	}
	
	public void test_stringlib_find_expect_error() {
		//test: regexp group number higher then count of regexp groups
		try {
			doCompile("string[] findList;function integer transform() {findList = find('mark.twain@javlin.eu','(^[a-z]*).([a-z]*)',5);	return 0;}", "test_stringlib_find_expect_error");
		} catch (Exception e) {
			// do nothing
		}
		//test: negative regexp group number
		try {
			doCompile("string[] findList;function integer transform() {findList = find('mark.twain@javlin.eu','(^[a-z]*).([a-z]*)',-1);	return 0;}", "test_stringlib_find_expect_error");
		} catch (Exception e) {
			// do nothing
		}
		//test: arg1 null input 
		try {
			doCompile("string[] findList;function integer transform() {findList = find(null,'(^[a-z]*).([a-z]*)');	return 0;}", "test_stringlib_find_expect_error");
		} catch (Exception e) {
			// do nothing
		}
		//test: arg2 null input - test1
		try {
			doCompile("string[] findList;function integer transform() {findList = find('mark.twain@javlin.eu',null);	return 0;}", "test_stringlib_find_expect_error");
		} catch (Exception e) {
			// do nothing
		}
		//test: arg2 null input - test2
		try {
			doCompile("string[] findList;function integer transform() {findList = find('',null);	return 0;}", "test_stringlib_find_expect_error");
		} catch (Exception e) {
			// do nothing
		}
		//test: arg1 and arg2 null input
		try {
			doCompile("string[] findList;function integer transform() {findList = find(null,null);	return 0;}", "test_stringlib_find_expect_error");
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_stringlib_join() {
		doCompile("test_stringlib_join");
		//check("joinedString", "Bagr,3,3.5641,-87L,CTL2");
		check("joinedString1", "80=5455.987\"-5=5455.987\"3=0.1");
		check("joinedString2", "5.0♫54.65♫67.0♫231.0");
		//check("joinedString3", "5☺54☺65☺67☺231☺80=5455.987☺-5=5455.987☺3=0.1☺CTL2☺42");
		check("test_empty1", "abc");
		check("test_empty2", "");
		check("test_empty3","  ");
		check("test_empty4","anullb");
		check("test_empty5","80=5455.987-5=5455.9873=0.1");
		check("test_empty6","80=5455.987 -5=5455.987 3=0.1");
		
		check("test_null1","abc");
		check("test_null2","");
		check("test_null3","anullb");
		check("test_null4","80=5455.987-5=5455.9873=0.1");

		//CLO-1210 
//		check("test_empty7","a=xb=nullc=z");
//		check("test_empty8","a=x b=null c=z");
//		check("test_empty9","null=xeco=storm");
//		check("test_empty10","null=x eco=storm");
//		check("test_null5","a=xb=nullc=z");
//		check("test_null6","null=xeco=storm");
		
	}
	
	public void test_stringlib_join_expect_error(){
		// CLO-1567 - join("", null) is ambiguous
//		try {
//			doCompile("function integer transform(){string s = join(';',null);return 0;}","test_stringlib_join_expect_error");
//			fail();
//		} catch (Exception e) {
//			// do nothing
//		}
		try {
			doCompile("function integer transform(){string[] tmp = null; string s = join(';',tmp);return 0;}","test_stringlib_join_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){map[string,string] a = null; string s = join(';',a);return 0;}","test_stringlib_join_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_stringlib_left() {
		//CLO - 1193
//		doCompile("test_stringlib_left");
//		check("test1", "aa");
//		check("test2", "aaa");
//		check("test3", "");
//		check("test4", null);
//		check("test5", "abc");
//		check("test6", "ab  ");
//		check("test7", "   ");
//		check("test8", "  ");
//		check("test9", "abc");
//		check("test10", "abc");
//		check("test11", "");
//		check("test12", null);
	}
	
	public void test_stringlib_length() {
		doCompile("test_stringlib_length");
		check("lenght1", new BigDecimal(50));
		
		check("stringLength", 8);
		check("length_empty", 0);
		check("length_null1", 0);
	}
	
	public void test_stringlib_lowerCase() {
		doCompile("test_stringlib_lowerCase");
		check("lower", "the quick !!$  brown fox jumps over the lazy dog bagr  ");
		check("lower_empty", "");
		check("lower_null", null);
	}
	
	public void test_stringlib_matches() {
		doCompile("test_stringlib_matches");
		check("matches1", true);
		check("matches2", true);
		check("matches3", false);
		check("matches4", true);
		check("matches5", false);
		check("matches6", false);
		check("matches7", false);
		check("matches8", false);
		check("matches9", true);
		check("matches10", true);
	}
	
	public void test_stringlib_matches_expect_error(){
		//test: regexp param null - test 1
		try {
			doCompile("boolean test; function integer transform(){test = matches('aaa', null); return 0;}","test_stringlib_matches_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: regexp param null - test 2
		try {
			doCompile("boolean test; function integer transform(){test = matches('', null); return 0;}","test_stringlib_matches_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: regexp param null - test 3
		try {
			doCompile("boolean test; function integer transform(){test = matches(null, null); return 0;}","test_stringlib_matches_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}

	}
	
	public void test_stringlib_matchGroups() {
		doCompile("test_stringlib_matchGroups");
		check("result1", null);
		check("result2", Arrays.asList(
				//"(([^:]*)([:])([\\(]))(.*)(\\))(((#)(.*))|($))"
				"zip:(zip:(/path/name?.zip)#innerfolder/file.zip)#innermostfolder?/filename*.txt",
				"zip:(",
				"zip",
				":",
				"(",
				"zip:(/path/name?.zip)#innerfolder/file.zip",
				")",
				"#innermostfolder?/filename*.txt",
				"#innermostfolder?/filename*.txt",
				"#",
				"innermostfolder?/filename*.txt",
				null
			)
		);
		check("result3", null);
		check("test_empty1", null);
		check("test_empty2", Arrays.asList(""));
		check("test_null1", null);
		check("test_null2", null);
	}
	public void test_stringlib_matchGroups_expect_error(){
		//test: regexp is null - test 1
		try {
			doCompile("string[] test; function integer transform(){test = matchGroups('eat all the cookies',null); return 0;}","test_stringlib_matchGroups_expect_error");
		} catch (Exception e) {
			// do nothing
		}
		//test: regexp is null - test 2
		try {
			doCompile("string[] test; function integer transform(){test = matchGroups('',null); return 0;}","test_stringlib_matchGroups_expect_error");
		} catch (Exception e) {
			// do nothing
		}
		//test: regexp is null - test 3
		try {
			doCompile("string[] test; function integer transform(){test = matchGroups(null,null); return 0;}","test_stringlib_matchGroups_expect_error");
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_stringlib_matchGroups_unmodifiable() {
		try {
			doCompile("test_stringlib_matchGroups_unmodifiable");
			fail();
		} catch (RuntimeException re) {
		};
	}
	
	public void test_stringlib_metaphone() {
		doCompile("test_stringlib_metaphone");
		check("metaphone1", "XRS");
		check("metaphone2", "KWNTLN");
		check("metaphone3", "KWNT");
		check("metaphone4", "");
		check("metaphone5", "");
		check("test_empty1", "");
		check("test_empty2", "");
		check("test_null1", null);
		check("test_null2", null);
	}
	
	public void test_stringlib_nysiis() {
		doCompile("test_stringlib_nysiis");
		check("nysiis1", "CAP");
		check("nysiis2", "CAP");
		check("nysiis3", "1234");
		check("nysiis4", "C2 PRADACTAN");
		check("nysiis_empty", "");
		check("nysiis_null", null);
	}
	
	public void test_stringlib_replace() {
		doCompile("test_stringlib_replace");
		
		final SimpleDateFormat format = new SimpleDateFormat();
		format.applyPattern("yyyy MMM dd");
		
		check("rep", format.format(new Date()).replaceAll("[lL]", "t"));
		check("rep1", "The cat says meow. All cats say meow.");
		check("rep2", "intruders must die");
		check("test_empty1", "a");
		check("test_empty2", "");
		check("test_null", null);
		check("test_null2","");
		check("test_null3","bbb");
		check("test_null4",null);
	}
	public void test_stringlib_replace_expect_error(){
		//test: regexp null - test1
		try {
			doCompile("string test; function integer transform(){test = replace('a b',null,'b'); return 0;}","test_stringlib_replace_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: regexp null - test2
		try {
			doCompile("string test; function integer transform(){test = replace('',null,'b'); return 0;}","test_stringlib_replace_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: regexp null - test3
		try {
			doCompile("string test; function integer transform(){test = replace(null,null,'b'); return 0;}","test_stringlib_replace_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: arg3 null - test1 
		try {
			doCompile("string test; function integer transform(){test = replace('a b','a+',null); return 0;}","test_stringlib_replace_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
//		//test: arg3 null - test2
//		try {
//			doCompile("string test; function integer transform(){test = replace('','a+',null); return 0;}","test_stringlib_replace_expect_error");
//			fail();
//		} catch (Exception e) {
//			// do nothing
//		}
//		//test: arg3 null - test3
//		try {
//			doCompile("string test; function integer transform(){test = replace(null,'a+',null); return 0;}","test_stringlib_replace_expect_error");
//			fail();
//		} catch (Exception e) {
//			// do nothing
//		}
		//test: regexp and arg3 null - test1
		try {
			doCompile("string test; function integer transform(){test = replace('a b',null,null); return 0;}","test_stringlib_replace_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: regexp and arg3 null - test1
			try {
				doCompile("string test; function integer transform(){test = replace(null,null,null); return 0;}","test_stringlib_replace_expect_error");
				fail();
			} catch (Exception e) {
				// do nothing
			}
	}
	
	
	public void test_stringlib_right() {
		doCompile("test_stringlib_right");
		check("righ", "y dog");
		check("rightNotPadded", "y dog");
		check("rightPadded", "y dog");
		check("padded", "   y dog");
		check("notPadded", "y dog");
		check("short", "Dog");
		check("shortNotPadded", "Dog");
		check("shortPadded", "     Dog");
		check("simple", "milk");
		check("test_null1", null);
		check("test_null2", null);
		check("test_null3", "  ");
		check("test_empty1", "");
		check("test_empty2", "");
		check("test_empty3","   ");
	}
	
	public void test_stringlib_soundex() {
		doCompile("test_stringlib_soundex");
		check("soundex1", "W630");
		check("soundex2", "W643");
		check("test_null", null);
		check("test_empty", "");
	}
	
	public void test_stringlib_split() {
		doCompile("test_stringlib_split");
		check("split1", Arrays.asList("The quick br", "wn f", "", " jumps " , "ver the lazy d", "g"));
		check("test_empty", Arrays.asList(""));
		check("test_empty2", Arrays.asList("","a","a"));
		List<String> tmp = new ArrayList<String>();
		tmp.add(null);
		check("test_null", tmp);
	}
	
	public void test_stringlib_split_expect_error(){
		//test: regexp null - test1
		try {
			doCompile("function integer transform(){string[] s = split('aaa',null); return 0;}","test_stringlib_split_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: regexp null - test2
		try {
			doCompile("function integer transform(){string[] s = split('',null); return 0;}","test_stringlib_split_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: regexp null - test3
		try {
			doCompile("function integer transform(){string[] s = split(null,null); return 0;}","test_stringlib_split_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_stringlib_substring() {
		doCompile("test_stringlib_substring");
		check("subs", "UICk ");
		check("test1", "");
		check("test_empty", "");
	}
	
	public void test_stringlib_substring_expect_error(){
		try {
			doCompile("function integer transform(){string test = substring('arabela',4,19);return 0;}","test_stringlib_substring_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = substring('arabela',15,3);return 0;}","test_stringlib_substring_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = substring('arabela',2,-3);return 0;}","test_stringlib_substring_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = substring('arabela',-5,7);return 0;}","test_stringlib_substring_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = substring('',0,7);return 0;}","test_stringlib_substring_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = substring('',7,7);return 0;}","test_stringlib_substring_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = substring(null,0,0);return 0;}","test_stringlib_substring_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = substring(null,0,4);return 0;}","test_stringlib_substring_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = substring(null,1,4);return 0;}","test_stringlib_substring_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_stringlib_trim() {
		doCompile("test_stringlib_trim");
		check("trim1", "im  The QUICk !!$  broWn fox juMPS over the lazy DOG");
		check("trim_empty", "");
		check("trim_null", null);
	}
	
	public void test_stringlib_upperCase() {
		doCompile("test_stringlib_upperCase");
		check("upper", "THE QUICK !!$  BROWN FOX JUMPS OVER THE LAZY DOG BAGR	");
		check("test_empty", "");
		check("test_null", null);
	}

	public void test_stringlib_isFormat() {
		doCompile("test_stringlib_isFormat");
		check("test", "test");
		check("isBlank", Boolean.FALSE);
		check("blank", "");
		checkNull("nullValue");
		check("isBlank1", true);
		check("isBlank2", true);
		check("isAscii1", true);
		check("isAscii2", false);
		check("isAscii3", true);
		check("isAscii4", true);
		check("isNumber", false);
		check("isNumber1", false);
		check("isNumber2", true);
		check("isNumber3", true);
		check("isNumber4", false);
		check("isNumber5", true);
		check("isNumber6", true);
		check("isNumber7", false);
		check("isNumber8", false);
		check("isInteger", false);
		check("isInteger1", false);
		check("isInteger2", false);
		check("isInteger3", true);
		check("isInteger4", false);
		check("isInteger5", false);
		check("isLong", true);
		check("isLong1", false);
		check("isLong2", false);
		check("isLong3", false);
		check("isLong4", false);
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
		
		check("isDate16", false);
		check("isDate17", true);
		check("isDate18", true);
		check("isDate19", false);
		check("isDate20", false);
		check("isDate21", false);
		/* CLO-1190
		check("isDate22", false);
		check("isDate23", false);
		check("isDate24", true);
		check("isDate25", false);
		 */
	}	
	public void test_stringlib_empty_strings() {
		String[] expressions = new String[] {
			"isInteger(?)",
			"isNumber(?)",
			"isLong(?)",
			"isAscii(?)",
			"isBlank(?)",
			"isDate(?, \"yyyy\")",
			"isUrl(?)",
			"string x = ?; length(x)",
			"lowerCase(?)",
			"matches(?, \"\")",
			"NYSIIS(?)",
			"removeBlankSpace(?)",
			"removeDiacritic(?)",
			"removeNonAscii(?)",
			"removeNonPrintable(?)",
			"replace(?, \"a\", \"a\")",
			"translate(?, \"ab\", \"cd\")",
			"trim(?)",
			"upperCase(?)",
			"chop(?)",
			"concat(?)",
			"getAlphanumericChars(?)",
		};
		
		StringBuilder sb = new StringBuilder();
		for (String expr : expressions) {
			String emptyString = expr.replace("?", "\"\"");
			boolean crashesEmpty = test_expression_crashes(emptyString);
			
			assertFalse("Function " + emptyString + " crashed", crashesEmpty);
			
			String nullString = expr.replace("?", "null");
			boolean crashesNull = test_expression_crashes(nullString);
			sb.append(String.format("|%20s|%5s|%5s|%n", expr, crashesEmpty ? "CRASH" : "ok", crashesNull ? "CRASH" : "ok"));
		}
		
		System.out.println(sb.toString());
	}
	
	private boolean test_expression_crashes(String expr) {
		String expStr = "function integer transform() { " + expr + "; return 0; }";
		try {
			doCompile(expStr, "test_stringlib_empty_null_strings");
			return false;
		} catch (RuntimeException e) {
			return true;
		}
	}
	
	public void test_stringlib_removeBlankSpace() {
		String expStr = 
			"string r1;\n" +
			"string str_empty;\n" +
			"string str_null;\n" +
			"function integer transform() {\n" +
				"r1=removeBlankSpace(\"" + StringUtils.specCharToString(" a	b\nc\rd   e \u000Cf\r\n") +	"\");\n" +
				"printErr(r1);\n" +
				"str_empty = removeBlankSpace('');\n" +
				"str_null = removeBlankSpace(null);\n" +
				"return 0;\n" +
			"}\n";
		doCompile(expStr, "test_removeBlankSpace");
		check("r1", "abcdef");
		check("str_empty", "");
		check("str_null", null);
	}
	
	public void test_stringlib_removeNonPrintable() {
		doCompile("test_stringlib_removeNonPrintable");
		check("nonPrintableRemoved", "AHOJ");
		check("test_empty", "");
		check("test_null", null);
	}
	
	public void test_stringlib_getAlphanumericChars() {
		String expStr = 
			"string an1;\n" +
			"string an2;\n" +
			"string an3;\n" +
			"string an4;\n" +
			"string an5;\n" +
			"string an6;\n" +
			"string an7;\n" +
			"string an8;\n" +
			"string an9;\n" +
			"string an10;\n" +
			"string an11;\n" +
			"string an12;\n" +
			"string an13;\n" +
			"string an14;\n" +
			"string an15;\n" +
			"function integer transform() {\n" +
				"an1=getAlphanumericChars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\");\n" +
				"an2=getAlphanumericChars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\",true,true);\n" +
				"an3=getAlphanumericChars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\",true,false);\n" +
				"an4=getAlphanumericChars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\",false,true);\n" +
				"an5=getAlphanumericChars(\"\");\n" +
				"an6=getAlphanumericChars(\"\",true,true);\n"+
				"an7=getAlphanumericChars(\"\",true,false);\n"+
				"an8=getAlphanumericChars(\"\",false,true);\n"+
				"an9=getAlphanumericChars(null);\n" +
				"an10=getAlphanumericChars(null,false,false);\n" +
				"an11=getAlphanumericChars(null,true,false);\n" +
				"an12=getAlphanumericChars(null,false,true);\n" +
				"an13=getAlphanumericChars('  0  ľeškó11');\n" +
				"an14=getAlphanumericChars('  0  ľeškó11', false, false);\n" +
				//CLO-1174
				"string tmp = \""+StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\";\n"+
				"printErr('BEFORE DO COMPILE: '+tmp); \n"+
				"an15=getAlphanumericChars(\"" + StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + "\",false,false);\n" +
				"printErr('AFTER GET_ALPHA_NUMERIC_CHARS: '+ an15);\n" +
				"return 0;\n" +
			"}\n";
		doCompile(expStr, "test_getAlphanumericChars");

		check("an1", "a1bcde2f");
		check("an2", "a1bcde2f");
		check("an3", "abcdef");
		check("an4", "12");
		check("an5", "");
		check("an6", "");
		check("an7", "");
		check("an8", "");
		check("an9", null);
		check("an10", null);
		check("an11", null);
		check("an12", null);
		check("an13", "0ľeškó11");
		check("an14","  0  ľeškó11");
		
		//CLO-1174
		String tmp = StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n");
		System.out.println("FROM JAVA - AFTER DO COMPILE: "+ tmp);
		//check("an15", tmp);
	}
	
	public void test_stringlib_indexOf(){
        doCompile("test_stringlib_indexOf");
        check("index",2);
		check("index1",9);
		check("index2",0);
		check("index3",-1);
		check("index4",6);
		check("index5",-1);
		check("index6",0);
		check("index7",4);
		check("index8",4);
		check("index9", -1);
		check("index10", 2);
		check("index_empty1", -1);
		check("index_empty2", 0);
		check("index_empty3", 0);
		check("index_empty4", -1);
	}
	
	public void test_stringlib_indexOf_expect_error(){
		//test: second arg is null - test1
		try {
			doCompile("integer index;function integer transform() {index = indexOf('hello world',null); return 0;}","test_stringlib_indexOf_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: second arg is null - test2
		try {
			doCompile("integer index;function integer transform() {index = indexOf('',null); return 0;}","test_stringlib_indexOf_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: first arg is null - test1
		try {
			doCompile("integer index;function integer transform() {index = indexOf(null,'a'); return 0;}","test_stringlib_indexOf_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: first arg is null - test2
		try {
			doCompile("integer index;function integer transform() {index = indexOf(null,''); return 0;}","test_stringlib_indexOf_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: both args are null
		try {
			doCompile("integer index;function integer transform() {index = indexOf(null,null); return 0;}","test_stringlib_indexOf_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_stringlib_removeDiacritic(){
        doCompile("test_stringlib_removeDiacritic");
        check("test","tescik");
    	check("test1","zabicka");
    	check("test_empty", "");
    	check("test_null", null);
    }
	
	public void test_stringlib_translate(){
        doCompile("test_stringlib_translate");
        check("trans","hippi");
		check("trans1","hipp");
		check("trans2","hippi");
		check("trans3","");
		check("trans4","y lanuaX nXXd thX lXttXr X");
		check("trans5", "hello");
		check("test_empty1", "");
		check("test_empty2", "");
		check("test_null", null);
	}
  
	public void test_stringlib_translate_expect_error(){
		try {
			doCompile("function integer transform(){string test = translate('bla bla',null,'o');return 0;}","test_stringlib_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = translate('bla bla','a',null);return 0;}","test_stringlib_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = translate('bla bla',null,null);return 0;}","test_stringlib_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = translate(null,'a',null);return 0;}","test_stringlib_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = translate(null,null,'a');return 0;}","test_stringlib_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string test = translate(null,null,null);return 0;}","test_stringlib_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	public void test_stringlib_removeNonAscii(){
		doCompile("test_stringlib_removeNonAscii");
		check("test1", "Sun is shining");
		check("test2", "");
		check("test_empty", "");
		check("test_null", null);
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
		check("s8", "hello");
		check("s9", "world");
		check("s10", "hello");
		check("s11", "world");
		check("s12", "mark.twain");
		check("s13", "two words");
		check("s14", "");
		check("s15", "");
		check("s16", "");
		check("s17", "");
		check("s18", "");
		check("s19", "word");
		check("s20", "");
		check("s21", "");
		check("s22", "mark.twain");
	}
	
	public void test_stringlib_chop_expect_error() {
		//test: arg is null
		try {
			doCompile("string test;function integer transform() {test = chop(null);return 0;}","test_strlib_chop_erxpect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}

		//test: regexp pattern is null
				try {
					doCompile("string test;function integer transform() {test = chop('aaa', null);return 0;}","test_strlib_chop_erxpect_error");
					fail();
				} catch (Exception e) {
					// do nothing
				}
		
		//test: regexp pattern is null - test 2
		try {
			doCompile("string test;function integer transform() {test = chop('', null);return 0;}","test_strlib_chop_erxpect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: arg is null
		try {
			doCompile("string test;function integer transform() {test = chop(null, 'aaa');return 0;}","test_strlib_chop_erxpect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: arg is null - test2
		try {
			doCompile("string test;function integer transform() {test = chop(null, '');return 0;}","test_strlib_chop_erxpect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: arg is null - test3
		try {
			doCompile("string test;function integer transform() {test = chop(null, null);return 0;}","test_strlib_chop_erxpect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}

	}
	
//-------------------------- MathLib Tests ------------------------
	public void test_bitwise_bitSet(){
		doCompile("test_bitwise_bitSet");
		check("test1", 3);
		check("test2", 15);
		check("test3", 34);
		check("test4", 3l);
		check("test5", 15l);
		check("test6", 34l);
	}
	
	public void test_bitwise_bitSet_expect_error(){
		try {
			doCompile("function integer transform(){"
					+ "integer var1 = null;"
					+ "integer var2 = 3;"
					+ "boolean var3 = false;"
					+ "integer i = bitSet(var1,var2,var3); return 0;}","test_bitwise_bitSet_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "integer var1 = 512;"
					+ "integer var2 = null;"
					+ "boolean var3 = false;"
					+ "integer i = bitSet(var1,var2,var3); return 0;}","test_bitwise_bitSet_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "integer var1 = 512;"
					+ "integer var2 = 3;"
					+ "boolean var3 = null;"
					+ "integer i = bitSet(var1,var2,var3); return 0;}","test_bitwise_bitSet_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "long var1 = 512l;"
					+ "integer var2 = 3;"
					+ "boolean var3 = null;"
					+ "long i = bitSet(var1,var2,var3); return 0;}","test_bitwise_bitSet_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "long var1 = 512l;"
					+ "integer var2 = null;"
					+ "boolean var3 = true;"
					+ "long i = bitSet(var1,var2,var3); return 0;}","test_bitwise_bitSet_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "long var1 = null;"
					+ "integer var2 = 3;"
					+ "boolean var3 = true;"
					+ "long i = bitSet(var1,var2,var3); return 0;}","test_bitwise_bitSet_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_bitwise_bitIsSet(){
		doCompile("test_bitwise_bitIsSet");
		check("test1", true);
		check("test2", false);
		check("test3", false);
		check("test4", false);
		check("test5", true);
		check("test6", false);
		check("test7", false);
		check("test8", false);
	}
	
	public void test_bitwise_bitIsSet_expect_error(){
		try {
			doCompile("function integer transform(){integer i = null; boolean b = bitIsSet(i,3); return 0;}","test_bitwise_bitIsSet_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer i = null; boolean b = bitIsSet(11,i); return 0;}","test_bitwise_bitIsSet_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long i = null; boolean b = bitIsSet(i,3); return 0;}","test_bitwise_bitIsSet_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long i = 12l; boolean b = bitIsSet(i,null); return 0;}","test_bitwise_bitIsSet_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
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
		check("resultMix1", 15L);
		check("resultMix2", 15L);
	}
	
	public void test_bitwise_or_expect_error(){
		try {
			doCompile("function integer transform(){"
					+ "integer input1 = 12; "
					+ "integer input2 = null; "
					+ "integer i = bitOr(input1, input2); return 0;}","test_bitwise_or_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "integer input1 = null; "
					+ "integer input2 = 13; "
					+ "integer i = bitOr(input1, input2); return 0;}","test_bitwise_or_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "long input1 = null; "
					+ "long input2 = 13l; "
					+ "long i = bitOr(input1, input2); return 0;}","test_bitwise_or_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "long input1 = 23l; "
					+ "long input2 = null; "
					+ "long i = bitOr(input1, input2); return 0;}","test_bitwise_or_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
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
		check("test_mixed1", 4l);
		check("test_mixed2", 4l);
	}

	public void test_bitwise_and_expect_error(){
		try {
			doCompile("function integer transform(){\n"
					+ "integer a = null; integer b = 16;\n"
					+ "integer i = bitAnd(a,b);\n"
					+ "return 0;}",
					"test_bitwise_end_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){\n"
					+ "integer a = 16; integer b = null;\n"
					+ "integer i = bitAnd(a,b);\n"
					+ "return 0;}",
					"test_bitwise_end_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){\n"
					+ "long a = 16l; long b = null;\n"
					+ "long i = bitAnd(a,b);\n"
					+ "return 0;}",
					"test_bitwise_end_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){\n"
					+ "long a = null; long b = 10l;\n"
					+ "long i = bitAnd(a,b);\n"
					+ "return 0;}",
					"test_bitwise_end_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
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
		check("test_mixed1", 15L);
		check("test_mixed2", 60L);
	}
	
	public void test_bitwise_xor_expect_error(){
		try {
			doCompile("function integer transform(){"
					+ "integer var1 = null;"
					+ "integer var2 = 123;"
					+ "integer i = bitXor(var1,var2); return 0;}","test_bitwise_xor_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "integer var1 = 23;"
					+ "integer var2 = null;"
					+ "integer i = bitXor(var1,var2); return 0;}","test_bitwise_xor_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "long var1 = null;"
					+ "long var2 = 123l;"
					+ "long i = bitXor(var1,var2); return 0;}","test_bitwise_xor_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "long var1 = 2135l;"
					+ "long var2 = null;"
					+ "long i = bitXor(var1,var2); return 0;}","test_bitwise_xor_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}

	public void test_bitwise_lshift() {
		doCompile("test_bitwise_lshift");
		check("resultInt1", 2);
		check("resultInt2", 4);
		check("resultInt3", 10);
		check("resultInt4", 20);
		check("resultInt5", -2147483648);
		check("resultLong1", 2l);
		check("resultLong2", 4l);
		check("resultLong3", 10l);
		check("resultLong4", 20l);
		check("resultLong5",-9223372036854775808l);
	}

	public void test_bitwise_lshift_expect_error(){
		try {
			doCompile("function integer transform(){integer input = null; integer i = bitLShift(input,2); return 0;}","test_bitwise_lshift_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer input = null; integer i = bitLShift(44,input); return 0;}","test_bitwise_lshift_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long input = null; long i = bitLShift(input,4l); return 0;}","test_bitwise_lshift_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long input = null; long i = bitLShift(444l,input); return 0;}","test_bitwise_lshift_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		
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
		check("test_neg1", 0);
		check("test_neg2", 0);
		check("test_neg3", 0l);
		check("test_neg4", 0l);
//		CLO-1399
//		check("test_mix1", 2);
//		check("test_mix2", 2);
		
	}
	
	public void test_bitwise_rshift_expect_error(){
		try {
			doCompile("function integer transform(){"
					+ "integer var1 = 23;"
					+ "integer var2 = null;"
					+ "integer i = bitRShift(var1,var2);"
					+ "return 0;}","test_bitwise_rshift_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
		try {
			doCompile("function integer transform(){"
					+ "integer var1 = null;"
					+ "integer var2 = 78;"
					+ "integer u = bitRShift(var1,var2);"
					+ "return 0;}","test_bitwise_rshift_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
		try {
			doCompile("function integer transform(){"
					+ "long var1 = 23l;"
					+ "long  var2 = null;"
					+ "long l =bitRShift(var1,var2);"
					+ "return 0;}","test_bitwise_rshift_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
		try {
			doCompile("function integer transform(){"
					+ "long var1 = null;"
					+ "long  var2 = 84l;"
					+ "long l = bitRShift(var1,var2);"
					+ "return 0;}","test_bitwise_rshift_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
	}

	public void test_bitwise_negate() {
		doCompile("test_bitwise_negate");
		check("resultInt", -59081717);
		check("resultLong", -3321654987654105969L);
		check("test_zero_int", -1);
		check("test_zero_long", -1l);
	}
	
	public void test_bitwise_negate_expect_error(){
		try {
			doCompile("function integer transform(){integer input = null; integer i = bitNegate(input); return 0;}","test_bitwise_negate_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long input = null; long i = bitNegate(input); return 0;}","test_bitwise_negate_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
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
	
	public void test_mathlib_abs_expect_error(){
		try {
			doCompile("function integer transform(){ \n "
					+ "integer tmp;\n "
					+ "tmp = null; \n"
					+ " integer i = abs(tmp); \n "
					+ "return 0;}",
					"test_mathlib_abs_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){ \n "
					+ "long tmp;\n "
					+ "tmp = null; \n"
					+ "long i = abs(tmp); \n "
					+ "return 0;}",
					"test_mathlib_abs_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){ \n "
					+ "double tmp;\n "
					+ "tmp = null; \n"
					+ "double i = abs(tmp); \n "
					+ "return 0;}",
					"test_mathlib_abs_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){ \n "
					+ "decimal tmp;\n "
					+ "tmp = null; \n"
					+ "decimal i = abs(tmp); \n "
					+ "return 0;}",
					"test_mathlib_abs_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_mathlib_ceil() {
		doCompile("test_mathlib_ceil");
		check("ceil1", -3.0);
		
		check("intResult", Arrays.asList(2.0, 3.0));
		check("longResult", Arrays.asList(2.0, 3.0));
		check("doubleResult", Arrays.asList(3.0, -3.0));
		check("decimalResult", Arrays.asList(3.0, -3.0));
	}
	
	public void test_mathlib_ceil_expect_error(){
		try {
			doCompile("function integer transform(){integer var = null; double d = ceil(var); return 0;}","test_mathlib_ceil_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long var = null; double d = ceil(var); return 0;}","test_mathlib_ceil_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){double var = null; double d = ceil(var); return 0;}","test_mathlib_ceil_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal var = null; double d = ceil(var); return 0;}","test_mathlib_ceil_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_mathlib_e() {
		doCompile("test_mathlib_e");
		check("varE", Math.E);
	}
	
	public void test_mathlib_exp() {
		doCompile("test_mathlib_exp");
		check("ex", Math.exp(1.123));
		check("test1", Math.exp(2));
		check("test2", Math.exp(22));
		check("test3", Math.exp(23));
		check("test4", Math.exp(94));
	}
	
	public void test_mathlib_exp_expect_error(){
		try {
			doCompile("function integer transform(){integer input = null; number n = exp(input); return 0;}","test_mathlib_exp_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long input = null; number n = exp(input); return 0;}","test_mathlib_exp_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){double input = null; number n = exp(input); return 0;}","test_mathlib_exp_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){number input = null; number n = exp(input); return 0;}","test_mathlib_exp_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_mathlib_floor() {
		doCompile("test_mathlib_floor");
		check("floor1", -4.0);
		
		check("intResult", Arrays.asList(2.0, 3.0));
		check("longResult", Arrays.asList(2.0, 3.0));
		check("doubleResult", Arrays.asList(2.0, -4.0));
		check("decimalResult", Arrays.asList(2.0, -4.0));
	}
	
	public void test_math_lib_floor_expect_error(){
		try {
			doCompile("function integer transform(){integer input = null; double d = floor(input); return 0;}","test_math_lib_floor_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long input= null; double d = floor(input); return 0;}","test_math_lib_floor_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){double input = null; double d = floor(input); return 0;}","test_math_lib_floor_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal input = null; double d = floor(input); return 0;}","test_math_lib_floor_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){number input = null; double d = floor(input); return 0;}","test_math_lib_floor_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_mathlib_log() {
		doCompile("test_mathlib_log");
		check("ln", Math.log(3));
		check("test_int", Math.log(32));
		check("test_long", Math.log(14l));
		check("test_double", Math.log(12.9));
		check("test_decimal", Math.log(23.7));
	}
	
	public void test_math_log_expect_error(){
		try {
			doCompile("function integer transform(){integer input = null; number n = log(input); return 0;}","test_math_log_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
		try {
			doCompile("function integer transform(){long input = null; number n = log(input); return 0;}","test_math_log_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
		try {
			doCompile("function integer transform(){decimal input = null; number n = log(input); return 0;}","test_math_log_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
		try {
			doCompile("function integer transform(){double input = null; number n = log(input); return 0;}","test_math_log_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
	}
	
	public void test_mathlib_log10() {
		doCompile("test_mathlib_log10");
		check("varLog10", Math.log10(3));
		check("test_int", Math.log10(5));
		check("test_long", Math.log10(90L));
		check("test_decimal", Math.log10(32.1));
		check("test_number", Math.log10(84.12));
	}
	
	public void test_mathlib_log10_expect_error(){
		try {
			doCompile("function integer transform(){integer input = null; number n = log10(input); return 0;}","test_mathlib_log10_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long input = null; number n = log10(input); return 0;}","test_mathlib_log10_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){number input = null; number n = log10(input); return 0;}","test_mathlib_log10_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal input = null; number n = log10(input); return 0;}","test_mathlib_log10_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
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
	
	public void test_mathlib_pow_expect_error(){
		try {
			doCompile("function integer transform(){integer var1 = 12; integer var2 = null; number n = pow(var1, var2); return 0;}","test_mathlib_pow_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer var1 = null; integer var2 = 2; number n = pow(var1, var2); return 0;}","test_mathlib_pow_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long var1 = 12l; long var2 = null; number n = pow(var1, var2); return 0;}","test_mathlib_pow_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long var1 = null; long var2 = 12L; number n = pow(var1, var2); return 0;}","test_mathlib_pow_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){number var1 = 12.2; number var2 = null; number n = pow(var1, var2); return 0;}","test_mathlib_pow_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){number var1 = null; number var2 = 2.1; number n = pow(var1, var2); return 0;}","test_mathlib_pow_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal var1 = 12.2d; decimal var2 = null; number n = pow(var1, var2); return 0;}","test_mathlib_pow_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal var1 = null; decimal var2 = 45.3d; number n = pow(var1, var2); return 0;}","test_mathlib_pow_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_mathlib_round() {
		doCompile("test_mathlib_round");
		check("round1", -4l);
		
		check("intResult", Arrays.asList(2l, 3l));
		check("longResult", Arrays.asList(2l, 3l));
		check("doubleResult", Arrays.asList(2l, 4l));
		check("decimalResult", Arrays.asList(2l, 4l));
	}
	
	public void test_mathlib_round_expect_error(){
		try {
			doCompile("function integer transform(){number input = null; long l = round(input);return 0;}","test_mathlib_round_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal input = null; long l = round(input);return 0;}","test_mathlib_round_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_mathlib_sqrt() {
		doCompile("test_mathlib_sqrt");
		check("sqrtPi", Math.sqrt(Math.PI));
		check("sqrt9", Math.sqrt(9));
		check("test_int", 2.0);
		check("test_long", Math.sqrt(64L));
		check("test_num", Math.sqrt(86.9));
		check("test_dec", Math.sqrt(34.5));
	}
	
	public void test_mathlib_sqrt_expect_error(){
		try {
			doCompile("function integer transform(){integer input = null; number num = sqrt(input);return 0;}","test_mathlib_sqrt_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long input = null; number num = sqrt(input);return 0;}","test_mathlib_sqrt_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){number input = null; number num = sqrt(input);return 0;}","test_mathlib_sqrt_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal input = null; number num = sqrt(input);return 0;}","test_mathlib_sqrt_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_mathlib_randomInteger(){
		doCompile("test_mathlib_randomInteger");
		assertNotNull(getVariable("test1"));
		check("test2", 2);
	}
	
	public void test_mathlib_randomInteger_expect_error(){
		try {
			doCompile("function integer transform(){integer i = randomInteger(1,null); return 0;}","test_mathlib_randomInteger_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer i = randomInteger(null,null); return 0;}","test_mathlib_randomInteger_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer i = randomInteger(null, -3); return 0;}","test_mathlib_randomInteger_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer i = randomInteger(1,-7); return 0;}","test_mathlib_randomInteger_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}

	public void test_mathlib_randomLong(){
		doCompile("test_mathlib_randomLong");
		assertNotNull(getVariable("test1"));
		check("test2", 15L);
	}
	
	public void test_mathlib_randomLong_expect_error(){
		try {
			doCompile("function integer transform(){long lo = randomLong(15L, null); return 0;}","test_mathlib_randomLong_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long lo = randomLong(null, null); return 0;}","test_mathlib_randomLong_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long lo = randomLong(null, 15L); return 0;}","test_mathlib_randomLong_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long lo = randomLong(15L, 10L); return 0;}","test_mathlib_randomLong_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
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
		Date expectedDate = new Date();
		//the returned date does not need to be exactly the same date which is in expectedData variable
		//let say 1000ms is tolerance for equality
		assertTrue("todayDate", Math.abs(expectedDate.getTime() - ((Date) getVariable("todayDate")).getTime()) < 1000);
	}
	
	public void test_datelib_zeroDate() {
		doCompile("test_datelib_zeroDate");
		check("zeroDate", new Date(0));
	}
	
	public void test_datelib_dateDiff() {
		doCompile("test_datelib_dateDiff");
		
		long diffYears = Years.yearsBetween(new DateTime(), new DateTime(BORN_VALUE)).getYears();
		check("ddiff", diffYears);
		
		long[] results = {1, 12, 52, 365, 8760, 525600, 31536000, 31536000000L};
		String[] vars = {"ddiffYears", "ddiffMonths", "ddiffWeeks", "ddiffDays", "ddiffHours", "ddiffMinutes", "ddiffSeconds", "ddiffMilliseconds"};
		
		for (int i = 0; i < results.length; i++) {
			check(vars[i], results[i]);
		}
	}
	
	public void test_datelib_dateDiff_epect_error(){
		try {
			doCompile("function integer transform(){long i = dateDiff(null,today(),millisec);return 0;}","test_datelib_dateDiff_epect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long i = dateDiff(today(),null,millisec);return 0;}","test_datelib_dateDiff_epect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long i = dateDiff(today(),today(),null);return 0;}","test_datelib_dateDiff_epect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_datelib_dateAdd() {
		doCompile("test_datelib_dateAdd");
		check("datum", new Date(BORN_MILLISEC_VALUE + 100));
	}
	
	public void test_datelib_dateAdd_expect_error(){
		try {
			doCompile("function integer transform(){date d = dateAdd(null,120,second); return 0;}","test_datelib_dateAdd_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){date d = dateAdd(today(),null,second); return 0;}","test_datelib_dateAdd_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){date d = dateAdd(today(),120,null); return 0;}","test_datelib_dateAdd_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
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
    	check("originalDate", BORN_VALUE);
    	check("nullDate", null);
    	check("nullDate2", null);
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
    	check("originalDate", BORN_VALUE);
    	check("nullDate", null);
    	check("nullDate2", null);
	}
	
	public void test_datelib_createDate() {
		doCompile("test_datelib_createDate");
		
		Calendar cal = Calendar.getInstance();
		
		// no time zone
		cal.clear();
		cal.set(2013, 5, 11);
		check("date1", cal.getTime());
		
		cal.clear();
		cal.set(2013, 5, 11, 14, 27, 53);
		check("dateTime1", cal.getTime());
		
		cal.clear();
		cal.set(2013, 5, 11, 14, 27, 53);
		cal.set(Calendar.MILLISECOND, 123);
		check("dateTimeMillis1", cal.getTime());

		// literal
		cal.setTimeZone(TimeZone.getTimeZone("GMT+5"));
		
		cal.clear();
		cal.set(2013, 5, 11);
		check("date2", cal.getTime());
		
		cal.clear();
		cal.set(2013, 5, 11, 14, 27, 53);
		check("dateTime2", cal.getTime());

		cal.clear();
		cal.set(2013, 5, 11, 14, 27, 53);
		cal.set(Calendar.MILLISECOND, 123);
		check("dateTimeMillis2", cal.getTime());

		// variable
		cal.clear();
		cal.set(2013, 5, 11);
		check("date3", cal.getTime());
		
		cal.clear();
		cal.set(2013, 5, 11, 14, 27, 53);
		check("dateTime3", cal.getTime());

		cal.clear();
		cal.set(2013, 5, 11, 14, 27, 53);
		cal.set(Calendar.MILLISECOND, 123);
		check("dateTimeMillis3", cal.getTime());
	}
	
	public void test_datelib_getPart() {
		doCompile("test_datelib_getPart");
		
		Calendar cal = Calendar.getInstance();
		
		cal.clear();
		cal.setTimeZone(TimeZone.getTimeZone("GMT+1"));
		cal.set(2013, 5, 11, 14, 46, 34);
		cal.set(Calendar.MILLISECOND, 123);
		
		Date date = cal.getTime();
		
		cal = Calendar.getInstance();
		cal.setTime(date);
		
		// no time zone
		check("year1", cal.get(Calendar.YEAR));
		check("month1", cal.get(Calendar.MONTH) + 1);
		check("day1", cal.get(Calendar.DAY_OF_MONTH));
		check("hour1", cal.get(Calendar.HOUR_OF_DAY));
		check("minute1", cal.get(Calendar.MINUTE));
		check("second1", cal.get(Calendar.SECOND));
		check("millisecond1", cal.get(Calendar.MILLISECOND));
		
		cal.setTimeZone(TimeZone.getTimeZone("GMT+5"));
		
		// literal
		check("year2", cal.get(Calendar.YEAR));
		check("month2", cal.get(Calendar.MONTH) + 1);
		check("day2", cal.get(Calendar.DAY_OF_MONTH));
		check("hour2", cal.get(Calendar.HOUR_OF_DAY));
		check("minute2", cal.get(Calendar.MINUTE));
		check("second2", cal.get(Calendar.SECOND));
		check("millisecond2", cal.get(Calendar.MILLISECOND));

		// variable
		check("year3", cal.get(Calendar.YEAR));
		check("month3", cal.get(Calendar.MONTH) + 1);
		check("day3", cal.get(Calendar.DAY_OF_MONTH));
		check("hour3", cal.get(Calendar.HOUR_OF_DAY));
		check("minute3", cal.get(Calendar.MINUTE));
		check("second3", cal.get(Calendar.SECOND));
		check("millisecond3", cal.get(Calendar.MILLISECOND));
		
		check("year_null", 2013);
		check("month_null", 6);
		check("day_null", 11);
		check("hour_null", 15);
		check("minute_null", cal.get(Calendar.MINUTE));
		check("second_null", cal.get(Calendar.SECOND));
		check("milli_null", cal.get(Calendar.MILLISECOND));
		
		check("year_null2", null);
		check("month_null2", null);
		check("day_null2", null);
		check("hour_null2", null);
		check("minute_null2", null);
		check("second_null2", null);
		check("milli_null2", null);
		
		check("year_null3", null);
		check("month_null3", null);
		check("day_null3", null);
		check("hour_null3", null);
		check("minute_null3", null);
		check("second_null3", null);
		check("milli_null3", null);
		
	}
	
	public void test_datelib_randomDate() {
		doCompile("test_datelib_randomDate");
		
		final long HOUR = 60L * 60L * 1000L;
		Date BORN_VALUE_NO_MILLIS = new Date(BORN_VALUE.getTime() / 1000L * 1000L);
		
		check("noTimeZone1", BORN_VALUE);
		check("noTimeZone2", BORN_VALUE_NO_MILLIS);
		
		check("withTimeZone1", new Date(BORN_VALUE_NO_MILLIS.getTime() + 2*HOUR)); // timezone changes from GMT+5 to GMT+3
		check("withTimeZone2", new Date(BORN_VALUE_NO_MILLIS.getTime() - 2*HOUR)); // timezone changes from GMT+3 to GMT+5
		assertNotNull(getVariable("patt_null"));
	}
	
	
	public void test_datelib_randomDate_expect_error(){
		try {
			doCompile("function integer transform(){date a = null; date b = today(); "
					+ "date d = randomDate(a,b); return 0;}","test_datelib_randomDate_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){date a = today(); date b = null; "
					+ "date d = randomDate(a,b); return 0;}","test_datelib_randomDate_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){date a = null; date b = null; "
					+ "date d = randomDate(a,b); return 0;}","test_datelib_randomDate_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long a = 843484317231l; long b = null; "
					+ "date d = randomDate(a,b); return 0;}","test_datelib_randomDate_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long a = null; long b = 12115641158l; "
					+ "date d = randomDate(a,b); return 0;}","test_datelib_randomDate_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long a = null; long b = null; "
					+ "date d = randomDate(a,b); return 0;}","test_datelib_randomDate_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "string a = null; string b = '2006-11-12'; string pattern='yyyy-MM-dd';"
					+ "date d = randomDate(a,b,pattern); return 0;}","test_datelib_randomDate_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){"
					+ "string a = '2006-11-12'; string b = null; string pattern='yyyy-MM-dd';"
					+ "date d = randomDate(a,b,pattern); return 0;}","test_datelib_randomDate_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//wrong format
		try {
			doCompile("function integer transform(){"
					+ "string a = '2006-10-12'; string b = '2006-11-12'; string pattern='yyyy:MM:dd';"
					+ "date d = randomDate(a,b,pattern); return 0;}","test_datelib_randomDate_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//start date bigger then end date
		try {
			doCompile("function integer transform(){"
					+ "string a = '2008-10-12'; string b = '2006-11-12'; string pattern='yyyy-MM-dd';"
					+ "date d = randomDate(a,b,pattern); return 0;}","test_datelib_randomDate_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
//-----------------Convert Lib tests-----------------------
	public void test_convertlib_json2xml(){
		doCompile("test_convertlib_json2xml");
		String xmlChunk =""
				+ "<lastName>Smith</lastName>"
				+ "<phoneNumber>"
					+ "<number>212 555-1234</number>"
					+ "<type>home</type>"
				+ "</phoneNumber>"
				+ "<phoneNumber>"
					+ "<number>646 555-4567</number>"
					+ "<type>fax</type>"
				+ "</phoneNumber>"
				+ "<address>"
					+ "<streetAddress>21 2nd Street</streetAddress>"
					+ "<postalCode>10021</postalCode>"
					+ "<state>NY</state>"
					+ "<city>New York</city>"
				+ "</address>"
				+ "<age>25</age>"
				+ "<firstName>John</firstName>";
		check("ret", xmlChunk);
		check("ret2", "<name/>");
		check("ret3", "<address></address>");
		check("ret4", "</>");
		check("ret5", "<#/>");
		check("ret6", "</>/<//>");
		check("ret7","");
		check("ret8", "<>Urgot</>");
	}
	
	public void test_convertlib_json2xml_expect_error(){
		try {
			doCompile("function integer transform(){string str = json2xml(''); return 0;}","test_convertlib_json2xml_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
		try {
			doCompile("function integer transform(){string str = json2xml(null); return 0;}","test_convertlib_json2xml_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
		try {
			doCompile("function integer transform(){string str = json2xml('{\"name\"}'); return 0;}","test_convertlib_json2xml_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
		try {
			doCompile("function integer transform(){string str = json2xml('{\"name\":}'); return 0;}","test_convertlib_json2xml_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
		try {
			doCompile("function integer transform(){string str = json2xml('{:\"name\"}'); return 0;}","test_convertlib_json2xml_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
	}
	

	public void test_convertlib_xml2json(){
		doCompile("test_convertlib_xml2json");
		String json = "{\"lastName\":\"Smith\",\"phoneNumber\":[{\"number\":\"212 555-1234\",\"type\":\"home\"},{\"number\":\"646 555-4567\",\"type\":\"fax\"}],\"address\":{\"streetAddress\":\"21 2nd Street\",\"postalCode\":10021,\"state\":\"NY\",\"city\":\"New York\"},\"age\":25,\"firstName\":\"John\"}";
		check("ret1", json);
		check("ret2", "{\"name\":\"Renektor\"}");
		check("ret3", "{}");
		check("ret4", "{\"address\":\"\"}");
		check("ret5", "{\"age\":32}");
		check("ret6", "{\"b\":\"\"}");
		check("ret7", "{\"char\":{\"name\":\"Anivia\",\"lane\":\"mid\"}}");
		check("ret8", "{\"#\":\"/\"}");
	}
	public void test_convertlib_xml2json_expect_error(){
		try {
			doCompile("function integer transform(){string json = xml2json(null); return 0;}","test_convertlib_xml2json_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string json = xml2json('<></>'); return 0;}","test_convertlib_xml2json_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string json = xml2json('<#>/</>'); return 0;}","test_convertlib_xml2json_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
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
	
	public void test_convertlib_base64byte_expect_error(){
		//this test should be expected to success in future
		try {
			doCompile("function integer transform(){byte b = base64byte(null); return 0;}","test_convertlib_base64byte_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_convertlib_bits2str() {
		doCompile("test_convertlib_bits2str");
		check("bitsAsString1", "00000000");
		check("bitsAsString2", "11111111");
		check("bitsAsString3", "010100000100110110100000");
	}
	
	public void test_convertlib_bits2str_expect_error(){
		//this test should be expected to success in future
		try {
			doCompile("function integer transform(){string s = bits2str(null); return 0;}","test_convertlib_bits2str_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_convertlib_bool2num() {
		doCompile("test_convertlib_bool2num");
		check("resultTrue", 1);
		check("resultFalse", 0);
		check("nullRet1", null);
		check("nullRet2", null);
	}
	
	public void test_convertlib_byte2base64() {
		doCompile("test_convertlib_byte2base64");
		check("inputBase64", Base64.encodeBytes("Abeceda zedla deda".getBytes()));
	}
	
	public void test_convertlib_byte2base64_expect_error(){
		//this test should be expected to success in future
		try {
			doCompile("function integer transform(){string s = byte2base64(null);return 0;}","test_convertlib_byte2base64_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_convertlib_byte2hex() {
		doCompile("test_convertlib_byte2hex");
		check("hexResult", "41626563656461207a65646c612064656461");
		check("test_null", null);
	}
	
	public void test_convertlib_date2long() {
		doCompile("test_convertlib_date2long");
		check("bornDate", BORN_MILLISEC_VALUE);
		check("zeroDate", 0l);
		check("nullRet1", null);
		check("nullRet2", null);
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
		check("weekMinCs", 1);
		check("dayMin", 1);
		check("hourMin", 1); //TODO: check!
		check("minuteMin", 0);
		check("secondMin", 0);
		check("millisecMin", 0);
		check("nullRet1", null);
		check("nullRet2", null);
	}
	
	public void test_convertlib_date2num_expect_error(){
		try {
			doCompile("function integer transform(){number num = date2num(1982-09-02,null); return 0;}","test_convertlib_date2num_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}	
		try {
			doCompile("function integer transform(){number num = date2num(1982-09-02,null,null); return 0;}","test_convertlib_date2num_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}	
		try {
			doCompile("function integer transform(){string s = null; number num = date2num(1982-09-02,null,s); return 0;}","test_convertlib_date2num_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
		try {
			doCompile("function integer transform(){string s = null; number num = date2num(1982-09-02,year,s); return 0;}","test_convertlib_date2num_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}	

	}
	
	public void test_convertlib_date2str() {
		doCompile("test_convertlib_date2str");
		check("inputDate", "1987:05:12");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy:MM:dd");
		check("bornDate", sdf.format(BORN_VALUE));

		SimpleDateFormat sdfCZ = new SimpleDateFormat("yyyy:MMMM:dd", MiscUtils.createLocale("cs.CZ"));
		check("czechBornDate", sdfCZ.format(BORN_VALUE));

		SimpleDateFormat sdfEN = new SimpleDateFormat("yyyy:MMMM:dd", MiscUtils.createLocale("en"));
		check("englishBornDate", sdfEN.format(BORN_VALUE));
		
		
		{
			String[] locales = {"en", "pl", null, "cs.CZ", null};
			List<String> expectedDates = new ArrayList<String>();
			
			for (String locale: locales) {
				expectedDates.add(new SimpleDateFormat("yyyy:MMMM:dd", MiscUtils.createLocale(locale)).format(BORN_VALUE));
			}
			
			check("loopTest", expectedDates);
		}
		
		SimpleDateFormat sdfGMT8 = new SimpleDateFormat("yyyy:MMMM:dd z", MiscUtils.createLocale("en"));
		sdfGMT8.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		check("timeZone", sdfGMT8.format(BORN_VALUE));
		check("nullRet", null);
		check("nullRet2", null);
	}
	
	public void test_convertlib_date2str_expect_error(){
		try {
			doCompile("function integer transform(){string s = date2str(1985-11-12,null, 'cs.CZ', 'GMT+8');return 0;}","test_convertlib_date2str_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_convertlib_decimal2double() {
		doCompile("test_convertlib_decimal2double");
		check("toDouble", 0.007d);
		check("nullRet1", null);
		check("nullRet2", null);
	}
	
	public void test_convertlib_decimal2integer() {
		doCompile("test_convertlib_decimal2integer");
		check("toInteger", 0);
		check("toInteger2", -500);
		check("toInteger3", 1000000);
		check("nullRet1", null);
		check("nullRet2", null);
	}
	
	public void test_convertlib_decimal2long() {
		doCompile("test_convertlib_decimal2long");
		check("toLong", 0l);
		check("toLong2", -500l);
		check("toLong3", 10000000000l);
		check("nullRet1", null);
		check("nullRet2", null);
	}
	
	public void test_convertlib_double2integer() {
		doCompile("test_convertlib_double2integer");
		check("toInteger", 0);
		check("toInteger2", -500);
		check("toInteger3", 1000000);
		check("nullRet1", null);
		check("nullRet2", null);
	}
	
	public void test_convertlib_double2long() {
		doCompile("test_convertlib_double2long");
		check("toLong", 0l);
		check("toLong2", -500l);
		check("toLong3", 10000000000l);
		check("nullRet1", null);
		check("nullRet2", null);
	}

	public void test_convertlib_getFieldName() {
		doCompile("test_convertlib_getFieldName");
		check("fieldNames",Arrays.asList("Name", "Age", "City", "Born", "BornMillisec", "Value", "Flag", "ByteArray", "Currency"));
	}

	public void test_convertlib_getFieldType() {
		doCompile("test_convertlib_getFieldType");
		check("fieldTypes",Arrays.asList(DataFieldType.STRING.getName(), DataFieldType.NUMBER.getName(), DataFieldType.STRING.getName(),
				DataFieldType.DATE.getName(), DataFieldType.LONG.getName(), DataFieldType.INTEGER.getName(), DataFieldType.BOOLEAN.getName(),
				DataFieldType.BYTE.getName(), DataFieldType.DECIMAL.getName()));
	}
	
	public void test_convertlib_hex2byte() {
		doCompile("test_convertlib_hex2byte");
		assertTrue(Arrays.equals((byte[])getVariable("fromHex"), BYTEARRAY_VALUE));
		check("test_null", null);
	}
	
	public void test_convertlib_long2date() {
		doCompile("test_convertlib_long2date");
		check("fromLong1", new Date(0));
		check("fromLong2", new Date(50000000000L));
		check("fromLong3", new Date(-5000L));
		check("nullRet1", null);
		check("nullRet2", null);
	}
	
	public void test_convertlib_long2integer() {
		doCompile("test_convertlib_long2integer");
		check("fromLong1", 10);
		check("fromLong2", -10);
		check("nullRet1", null);
		check("nullRet2", null);
	}
	
	public void test_convertlib_long2integer_expect_error(){
		//this test should be expected to success in future
		try {
			doCompile("function integer transform(){integer i = long2integer(200032132463123L); return 0;}","test_convertlib_long2integer_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	
	public void test_convertlib_long2packDecimal() {
		doCompile("test_convertlib_long2packDecimal");
		assertTrue(Arrays.equals((byte[])getVariable("packedLong"), new byte[] {5, 0, 12}));
	}
	
	public void test_convertlib_long2packDecimal_expect_error(){
		//this test should be expected to success in future
		try {
			doCompile("function integer transform(){byte b = long2packDecimal(null); return 0;}","test_convertlib_long2packDecimal_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_convertlib_md5() {
		doCompile("test_convertlib_md5");
		assertTrue(Arrays.equals((byte[])getVariable("md5Hash1"), Digest.digest(DigestType.MD5, "The quick brown fox jumps over the lazy dog")));
		assertTrue(Arrays.equals((byte[])getVariable("md5Hash2"), Digest.digest(DigestType.MD5, BYTEARRAY_VALUE)));
		assertTrue(Arrays.equals((byte[])getVariable("test_empty"), Digest.digest(DigestType.MD5, "")));
	}
	public void test_convertlib_md5_expect_error(){
//CLO-1254
//		//this test should be expected to success in future
//		try {
//			doCompile("function integer transform(){byte b = md5(null); return 0;}","test_convertlib_md5_expect_error");
//			fail();
//		} catch (Exception e) {
//			// do nothing
//		}
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
	
	public void test_convertlib_num2bool_expect_error(){
		//this test should be expected to success in future
		//test: integer
		try {
			doCompile("integer input; function integer transform(){input=null; boolean b = num2bool(input); return 0;}","test_convertlib_num2bool_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//this test should be expected to success in future
		//test: long
		try {
			doCompile("long input; function integer transform(){input=null; boolean b = num2bool(input); return 0;}","test_convertlib_num2bool_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}		
		//this test should be expected to success in future
		//test: double
		try {
			doCompile("double input; function integer transform(){input=null; boolean b = num2bool(input); return 0;}","test_convertlib_num2bool_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//this test should be expected to success in future
		//test: decimal
		try {
			doCompile("decimal input; function integer transform(){input=null; boolean b = num2bool(input); return 0;}","test_convertlib_num2bool_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_convertlib_num2str() {
		System.out.println("num2str() test:");
		doCompile("test_convertlib_num2str");

		check("intOutput", Arrays.asList("16", "10000", "20", "10", "1.235E3", "12 350 001 Kcs"));
		check("longOutput", Arrays.asList("16", "10000", "20", "10", "1.235E13", "12 350 001 Kcs"));
		check("doubleOutput", Arrays.asList("16.16", "0x1.028f5c28f5c29p4", "1.23548E3", "12 350 001,1 Kcs"));
		check("decimalOutput", Arrays.asList("16.16", "1235.44", "12 350 001,1 Kcs"));
		check("nullIntRet", Arrays.asList(null,null,null,null,"12","12",null,null));
		check("nullLongRet", Arrays.asList(null,null,null,null,"12","12",null,null));
		check("nullDoubleRet", Arrays.asList(null,null,null,null,"12.2","12.2",null,null));
		check("nullDecRet", Arrays.asList(null,null,null,"12.2","12.2",null,null));
	}

	public void test_convertlib_num2str_expect_error(){
		try {
			doCompile("function integer transform(){integer var = null; string ret = num2str(12, var); return 0;}","test_convertlib_num2str_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer var = null; string ret = num2str(12L, var); return 0;}","test_convertlib_num2str_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer var = null; string ret = num2str(12.3, var); return 0;}","test_convertlib_num2str_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_convertlib_packdecimal2long() {
		doCompile("test_convertlib_packDecimal2long");
		check("unpackedLong", PackedDecimal.parse(BYTEARRAY_VALUE));
	}

	public void test_convertlib_packdecimal2long_expect_error(){
		try {
			doCompile("function integer transform(){long l =packDecimal2long(null); return 0;}","test_convertlib_packdecimal2long_expect_error");
			fail();		
		} catch (Exception e) {
			// do nothing;
		}
	}
	
	public void test_convertlib_sha() {
		doCompile("test_convertlib_sha");
		assertTrue(Arrays.equals((byte[])getVariable("shaHash1"), Digest.digest(DigestType.SHA, "The quick brown fox jumps over the lazy dog")));
		assertTrue(Arrays.equals((byte[])getVariable("shaHash2"), Digest.digest(DigestType.SHA, BYTEARRAY_VALUE)));
		assertTrue(Arrays.equals((byte[])getVariable("test_empty"), Digest.digest(DigestType.SHA, "")));
	}
	
	public void test_convertlib_sha_expect_error(){
//		CLO-1258
//		try {
//			doCompile("function integer transform(){byte b = sha(null); return 0;}","test_convertlib_sha_expect_error");
//			fail();
//		} catch (Exception e) {
//			// do nothing
//		}
	}

	public void test_convertlib_sha256() {
		doCompile("test_convertlib_sha256");
		assertTrue(Arrays.equals((byte[])getVariable("shaHash1"), Digest.digest(DigestType.SHA256, "The quick brown fox jumps over the lazy dog")));
		assertTrue(Arrays.equals((byte[])getVariable("shaHash2"), Digest.digest(DigestType.SHA256, BYTEARRAY_VALUE)));
		assertTrue(Arrays.equals((byte[])getVariable("test_empty"), Digest.digest(DigestType.SHA256, "")));
	}
	
	public void test_convertlib_sha256_expect_error(){
//		CLO-1258
//		try {
//			doCompile("function integer transform(){byte b = sha256(null); return 0;}","test_convertlib_sha256_expect_error");
//			fail();
//		} catch (Exception e) {
//			// do nothing
//		}
	}

	public void test_convertlib_str2bits() {
		doCompile("test_convertlib_str2bits");
		//TODO: uncomment -> test will pass, but is that correct?
		assertTrue(Arrays.equals((byte[]) getVariable("textAsBits1"), new byte[] {0/*, 0, 0, 0, 0, 0, 0, 0*/}));
		assertTrue(Arrays.equals((byte[]) getVariable("textAsBits2"), new byte[] {-1/*, 0, 0, 0, 0, 0, 0, 0*/}));
		assertTrue(Arrays.equals((byte[]) getVariable("textAsBits3"), new byte[] {10, -78, 5/*, 0, 0, 0, 0, 0*/}));
		assertTrue(Arrays.equals((byte[]) getVariable("test_empty"), new byte[] {}));
	}
	
	public void test_convertlib_str2bits_expect_error(){
		try {
			doCompile("function integer transform(){byte b = str2bits(null); return 0;}","test_convertlib_str2bits_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}

	public void test_convertlib_str2bool() {
		doCompile("test_convertlib_str2bool");
		check("fromTrueString", true);
		check("fromFalseString", false);
		check("nullRet1", null);
		check("nullRet2", null);
	}
	
	public void test_convertlib_str2bool_expect_error(){
		try {
			doCompile("function integer transform(){boolean b = str2bool('asd'); return 0;}","test_convertlib_str2bool_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
		try {
			doCompile("function integer transform(){boolean b = str2bool(''); return 0;}","test_convertlib_str2bool_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing;
		}
	}

	public void test_convertlib_str2date() {
		doCompile("test_convertlib_str2date");
		
		Calendar cal = Calendar.getInstance();
		cal.set(2050, 4, 19, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);
		
		Date checkDate = cal.getTime();
		
		check("date1", checkDate);
		check("date2", checkDate);
		
		cal.clear();
		cal.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		cal.set(2013, 04, 30, 17, 15, 12);
		check("withTimeZone1", cal.getTime());
		
		cal.clear();
		cal.setTimeZone(TimeZone.getTimeZone("GMT-8"));
		cal.set(2013, 04, 30, 17, 15, 12);
		check("withTimeZone2", cal.getTime());
		
		assertFalse(getVariable("withTimeZone1").equals(getVariable("withTimeZone2")));
	}

	public void test_convertlib_str2date_expect_error(){
		try {
			doCompile("function integer transform(){date d = str2date('1987-11-17', null); return 0;}","test_convertlib_str2date_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){date d = str2date(null, 'dd.MM.yyyy'); return 0;}","test_convertlib_str2date_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){date d = str2date(null, null); return 0;}","test_convertlib_str2date_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){date d = str2date('1987-11-17', 'dd.MM.yyyy'); return 0;}","test_convertlib_str2date_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}            
		try {
			doCompile("function integer transform(){date d = str2date('1987-33-17', 'yyyy-MM-dd'); return 0;}","test_convertlib_str2date_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){date d = str2date('17.11.1987', null, 'cs.CZ'); return 0;}","test_convertlib_str2date_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_convertlib_str2decimal() {
		doCompile("test_convertlib_str2decimal");
		check("parsedDecimal1", new BigDecimal("100.13"));
		check("parsedDecimal2", new BigDecimal("123123123.123"));
		check("parsedDecimal3", new BigDecimal("-350000.01"));
		check("parsedDecimal4", new BigDecimal("1000000"));
		check("parsedDecimal5", new BigDecimal("1000000.99"));
		check("parsedDecimal6", new BigDecimal("123123123.123"));
		check("parsedDecimal7", new BigDecimal("5.01"));
	}
	
	public void test_convertlib_str2decimal_expect_result(){
		try {
			doCompile("function integer transform(){decimal d = str2decimal(''); return 0;}","test_convertlib_str2decimal_expect_result");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal d = str2decimal(null); return 0;}","test_convertlib_str2decimal_expect_result");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal d = str2decimal('5.05 CZK','#.#CZ'); return 0;}","test_convertlib_str2decimal_expect_result");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal d = str2decimal('5.05 CZK',null); return 0;}","test_convertlib_str2decimal_expect_result");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){decimal d = str2decimal(null,'#.# US'); return 0;}","test_convertlib_str2decimal_expect_result");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}

	public void test_convertlib_str2double() {
		doCompile("test_convertlib_str2double");
		check("parsedDouble1", 100.13);
		check("parsedDouble2", 123123123.123);
		check("parsedDouble3", -350000.01);
	}
	
	public void test_convertlib_str2double_expect_error(){
		try {
			doCompile("function integer transform(){double d = str2double(''); return 0;}","test_convertlib_str2double_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){double d = str2double(null); return 0;}","test_convertlib_str2double_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){double d = str2double('text'); return 0;}","test_convertlib_str2double_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){double d = str2double('0.90c',null); return 0;}","test_convertlib_str2double_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){double d = str2double('0.90c','#.# c'); return 0;}","test_convertlib_str2double_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}

	public void test_convertlib_str2integer() {
		doCompile("test_convertlib_str2integer");
		check("parsedInteger1", 123456789);
		check("parsedInteger2", 123123);
		check("parsedInteger3", -350000);
		check("parsedInteger4", 419);
	}
	
	public void test_convertlib_str2integer_expect_error(){
		try {
			doCompile("function integer transform(){integer i = str2integer('abc'); return 0;}","test_convertlib_str2integer_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer i = str2integer(''); return 0;}","test_convertlib_str2integer_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){integer i = str2integer(null); return 0;}","test_convertlib_str2integer_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}

	public void test_convertlib_str2long() {
		doCompile("test_convertlib_str2long");
		check("parsedLong1", 1234567890123L);
		check("parsedLong2", 123123123456789L);
		check("parsedLong3", -350000L);
		check("parsedLong4", 133L);
	}
	
	public void test_convertlib_str2long_expect_error(){
		try {
			doCompile("function integer transform(){long i = str2long('abc'); return 0;}","test_convertlib_str2long_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long i = str2long(''); return 0;}","test_convertlib_str2long_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){long i = str2long(null); return 0;}","test_convertlib_str2long_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}	

	public void test_convertlib_toString() {
		doCompile("test_convertlib_toString");
		check("integerString", "10");
		check("longString", "110654321874");
		check("doubleString", "1.547874E-14");
		check("decimalString", "-6847521431.1545874");
		check("listString", "[not ALI A, not ALI B, not ALI D..., but, ALI H!]");
		check("mapString", "{1=Testing, 2=makes, 3=me, 4=crazy :-)}");
		String byteMapString = getVariable("byteMapString").toString();
		assertTrue(byteMapString.contains("1=value1"));
		assertTrue(byteMapString.contains("2=value2"));
		String fieldByteMapString = getVariable("fieldByteMapString").toString();
		assertTrue(fieldByteMapString.contains("key1=value1"));
		assertTrue(fieldByteMapString.contains("key2=value2"));
		check("byteListString", "[firstElement, secondElement]");
		check("fieldByteListString", "[firstElement, secondElement]");
//		CLO-1262
		check("test_null_l", "null");
		check("test_null_dec", "null");
		check("test_null_d", "null");
		check("test_null_i", "null");
	}
	
	public void test_convertlib_str2byte() {
		doCompile("test_convertlib_str2byte");

		checkArray("utf8Hello", new byte[] { 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 33 });
		checkArray("utf8Horse", new byte[] { 80, -59, -103, -61, -83, 108, 105, -59, -95, 32, -59, -66, 108, 117, -59, -91, 111, 117, -60, -115, 107, -61, -67, 32, 107, -59, -81, -59, -120, 32, 112, -60, -101, 108, 32, -60, -113, -61, -95, 98, 108, 115, 107, -61, -87, 32, -61, -77, 100, 121 });
		checkArray("utf8Math", new byte[] { -62, -67, 32, -30, -123, -109, 32, -62, -68, 32, -30, -123, -107, 32, -30, -123, -103, 32, -30, -123, -101, 32, -30, -123, -108, 32, -30, -123, -106, 32, -62, -66, 32, -30, -123, -105, 32, -30, -123, -100, 32, -30, -123, -104, 32, -30, -126, -84, 32, -62, -78, 32, -62, -77, 32, -30, -128, -96, 32, -61, -105, 32, -30, -122, -112, 32, -30, -122, -110, 32, -30, -122, -108, 32, -30, -121, -110, 32, -30, -128, -90, 32, -30, -128, -80, 32, -50, -111, 32, -50, -110, 32, -30, -128, -109, 32, -50, -109, 32, -50, -108, 32, -30, -126, -84, 32, -50, -107, 32, -50, -106, 32, -49, -128, 32, -49, -127, 32, -49, -126, 32, -49, -125, 32, -49, -124, 32, -49, -123, 32, -49, -122, 32, -49, -121, 32, -49, -120, 32, -49, -119 });

		checkArray("utf16Hello", new byte[] { -2, -1, 0, 72, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 87, 0, 111, 0, 114, 0, 108, 0, 100, 0, 33 });
		checkArray("utf16Horse", new byte[] { -2, -1, 0, 80, 1, 89, 0, -19, 0, 108, 0, 105, 1, 97, 0, 32, 1, 126, 0, 108, 0, 117, 1, 101, 0, 111, 0, 117, 1, 13, 0, 107, 0, -3, 0, 32, 0, 107, 1, 111, 1, 72, 0, 32, 0, 112, 1, 27, 0, 108, 0, 32, 1, 15, 0, -31, 0, 98, 0, 108, 0, 115, 0, 107, 0, -23, 0, 32, 0, -13, 0, 100, 0, 121 });
		checkArray("utf16Math", new byte[] { -2, -1, 0, -67, 0, 32, 33, 83, 0, 32, 0, -68, 0, 32, 33, 85, 0, 32, 33, 89, 0, 32, 33, 91, 0, 32, 33, 84, 0, 32, 33, 86, 0, 32, 0, -66, 0, 32, 33, 87, 0, 32, 33, 92, 0, 32, 33, 88, 0, 32, 32, -84, 0, 32, 0, -78, 0, 32, 0, -77, 0, 32, 32, 32, 0, 32, 0, -41, 0, 32, 33, -112, 0, 32, 33, -110, 0, 32, 33, -108, 0, 32, 33, -46, 0, 32, 32, 38, 0, 32, 32, 48, 0, 32, 3, -111, 0, 32, 3, -110, 0, 32, 32, 19, 0, 32, 3, -109, 0, 32, 3, -108, 0, 32, 32, -84, 0, 32, 3, -107, 0, 32, 3, -106, 0, 32, 3, -64, 0, 32, 3, -63, 0, 32, 3, -62, 0, 32, 3, -61, 0, 32, 3, -60, 0, 32, 3, -59, 0, 32, 3, -58, 0, 32, 3, -57, 0, 32, 3, -56, 0, 32, 3, -55 });

		checkArray("macHello", new byte[] { 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 33 });
		checkArray("macHorse", new byte[] { 80, -34, -110, 108, 105, -28, 32, -20, 108, 117, -23, 111, 117, -117, 107, -7, 32, 107, -13, -53, 32, 112, -98, 108, 32, -109, -121, 98, 108, 115, 107, -114, 32, -105, 100, 121 });

		checkArray("asciiHello", new byte[] { 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 33 });

		checkArray("isoHello", new byte[] { 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 33 });
		checkArray("isoHorse", new byte[] { 80, -8, -19, 108, 105, -71, 32, -66, 108, 117, -69, 111, 117, -24, 107, -3, 32, 107, -7, -14, 32, 112, -20, 108, 32, -17, -31, 98, 108, 115, 107, -23, 32, -13, 100, 121 });

		checkArray("cpHello", new byte[] { 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 33 });
		checkArray("cpHorse", new byte[] { 80, -8, -19, 108, 105, -102, 32, -98, 108, 117, -99, 111, 117, -24, 107, -3, 32, 107, -7, -14, 32, 112, -20, 108, 32, -17, -31, 98, 108, 115, 107, -23, 32, -13, 100, 121 });
	}

	public void test_convertlib_str2byte_expect_error(){
		try {
			doCompile("function integer transform(){byte b = str2byte(null,'utf-8'); return 0;}","test_convertlib_str2byte_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){byte b = str2byte(null,'utf-16'); return 0;}","test_convertlib_str2byte_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){byte b = str2byte(null,'MacCentralEurope'); return 0;}","test_convertlib_str2byte_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){byte b = str2byte(null,'ascii'); return 0;}","test_convertlib_str2byte_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){byte b = str2byte(null,'iso-8859-2'); return 0;}","test_convertlib_str2byte_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){byte b = str2byte(null,'windows-1250'); return 0;}","test_convertlib_str2byte_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){byte b = str2byte('wallace',null); return 0;}","test_convertlib_str2byte_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){byte b = str2byte('wallace','knock'); return 0;}","test_convertlib_str2byte_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){byte b = str2byte('wallace',null); return 0;}","test_convertlib_str2byte_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_convertlib_byte2str() {
		doCompile("test_convertlib_byte2str");

		String hello = "Hello World!";
		String horse = "Příliš žluťoučký kůň pěl ďáblské ódy";
		String math = "½ ⅓ ¼ ⅕ ⅙ ⅛ ⅔ ⅖ ¾ ⅗ ⅜ ⅘ € ² ³ † × ← → ↔ ⇒ … ‰ Α Β – Γ Δ € Ε Ζ π ρ ς σ τ υ φ χ ψ ω";

		check("utf8Hello", hello);
		check("utf8Horse", horse);
		check("utf8Math", math);

		check("utf16Hello", hello);
		check("utf16Horse", horse);
		check("utf16Math", math);

		check("macHello", hello);
		check("macHorse", horse);

		check("asciiHello", hello);

		check("isoHello", hello);
		check("isoHorse", horse);

		check("cpHello", hello);
		check("cpHorse", horse);

	}

	public void test_convertlib_byte2str_expect_error(){
		try {
			doCompile("function integer transform(){string s = byte2str(null,'utf-8'); return 0;}","test_convertlib_byte2str_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string s = byte2str(null,null); return 0;}","test_convertlib_byte2str_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		try {
			doCompile("function integer transform(){string s = byte2str(str2byte('hello', 'utf-8'),null); return 0;}","test_convertlib_byte2str_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_conditional_fail() {
		doCompile("test_conditional_fail");
		check("result", 3);
	}
	
	
	public void test_expression_statement(){
		// test case for issue 4174
        doCompileExpectErrors("test_expression_statement", Arrays.asList("Syntax error, statement expected","Syntax error, statement expected"));
	}

	public void test_dictionary_read() {
		doCompile("test_dictionary_read");
		check("s", "Verdon");
		check("i", Integer.valueOf(211));
		check("l", Long.valueOf(226));
		check("d", BigDecimal.valueOf(239483061));
		check("n", Double.valueOf(934.2));
		check("a", new GregorianCalendar(1992, GregorianCalendar.AUGUST, 1).getTime());
		check("b", true);
		byte[] y = (byte[]) getVariable("y");
		assertEquals(10, y.length);
		assertEquals(89, y[9]);
		
		check("sNull", null);
		check("iNull", null);
		check("lNull", null);
		check("dNull", null);
		check("nNull", null);
		check("aNull", null);
		check("bNull", null);
		check("yNull", null);
		
		check("stringList", Arrays.asList("aa", "bb", null, "cc"));
		check("dateList", Arrays.asList(new Date(12000), new Date(34000), null, new Date(56000)));
		@SuppressWarnings("unchecked")
		List<byte[]> byteList = (List<byte[]>) getVariable("byteList"); 
		assertDeepEquals(byteList, Arrays.asList(new byte[] {0x12}, new byte[] {0x34, 0x56}, null, new byte[] {0x78}));
	}

	public void test_dictionary_write() {
		doCompile("test_dictionary_write");
		assertEquals(832, graph.getDictionary().getValue("i") );
		assertEquals("Guil", graph.getDictionary().getValue("s"));
		assertEquals(Long.valueOf(540), graph.getDictionary().getValue("l"));
		assertEquals(BigDecimal.valueOf(621), graph.getDictionary().getValue("d"));
		assertEquals(934.2, graph.getDictionary().getValue("n"));
		assertEquals(new GregorianCalendar(1992, GregorianCalendar.DECEMBER, 2).getTime(), graph.getDictionary().getValue("a"));
		assertEquals(true, graph.getDictionary().getValue("b"));
		byte[] y = (byte[]) graph.getDictionary().getValue("y");
		assertEquals(2, y.length);
		assertEquals(18, y[0]);
		assertEquals(-94, y[1]);
		
		assertEquals(Arrays.asList("xx", null), graph.getDictionary().getValue("stringList"));
		assertEquals(Arrays.asList(new Date(98000), null, new Date(76000)), graph.getDictionary().getValue("dateList"));
		@SuppressWarnings("unchecked")
		List<byte[]> byteList = (List<byte[]>) graph.getDictionary().getValue("byteList"); 
		assertDeepEquals(byteList, Arrays.asList(null, new byte[] {(byte) 0xAB, (byte) 0xCD}, new byte[] {(byte) 0xEF}));

		check("assignmentReturnValue", "Guil");
		
	}

	public void test_dictionary_write_null() {
		doCompile("test_dictionary_write_null");
		assertEquals(null, graph.getDictionary().getValue("s"));
		assertEquals(null, graph.getDictionary().getValue("sVerdon"));
		assertEquals(null, graph.getDictionary().getValue("i") );
		assertEquals(null, graph.getDictionary().getValue("i211") );
		assertEquals(null, graph.getDictionary().getValue("l"));
		assertEquals(null, graph.getDictionary().getValue("l452"));
		assertEquals(null, graph.getDictionary().getValue("d"));
		assertEquals(null, graph.getDictionary().getValue("d621"));
		assertEquals(null, graph.getDictionary().getValue("n"));
		assertEquals(null, graph.getDictionary().getValue("n9342"));
		assertEquals(null, graph.getDictionary().getValue("a"));
		assertEquals(null, graph.getDictionary().getValue("a1992"));
		assertEquals(null, graph.getDictionary().getValue("b"));
		assertEquals(null, graph.getDictionary().getValue("bTrue"));
		assertEquals(null, graph.getDictionary().getValue("y"));
		assertEquals(null, graph.getDictionary().getValue("yFib"));
	}
	
	public void test_dictionary_invalid_key(){
        doCompileExpectErrors("test_dictionary_invalid_key", Arrays.asList("Dictionary entry 'invalid' does not exist"));
	}
	
	public void test_dictionary_string_to_int(){
        doCompileExpectErrors("test_dictionary_string_to_int", Arrays.asList("Type mismatch: cannot convert from 'string' to 'integer'","Type mismatch: cannot convert from 'string' to 'integer'"));
	}
	
	public void test_utillib_sleep() {
		long time = System.currentTimeMillis();
		doCompile("test_utillib_sleep");
		long tmp = System.currentTimeMillis() - time;
		assertTrue("sleep() function didn't pause execution "+ tmp, tmp >= 1000);
	}
	
	public void test_utillib_random_uuid() {
		doCompile("test_utillib_random_uuid");
		assertNotNull(getVariable("uuid"));
	}
	public void test_stringlib_randomString(){
		doCompile("string test; function integer transform(){test = randomString(1,3); return 0;}","test_stringlib_randomString");
		assertNotNull(getVariable("test"));
	}
	
	public void test_stringlib_validUrl() {
		doCompile("test_stringlib_url");
		check("urlValid", Arrays.asList(true, true, false, true, false, true));
		check("protocol", Arrays.asList("http", "https", null, "sandbox", null, "zip"));
		check("userInfo", Arrays.asList("", "chuck:norris", null, "", null, ""));
		check("host", Arrays.asList("example.com", "server.javlin.eu", null, "cloveretl.test.scenarios", null, ""));
		check("port", Arrays.asList(-1, 12345, -2, -1, -2, -1));
		check("path", Arrays.asList("", "/backdoor/trojan.cgi", null, "/graph/UDR_FileURL_SFTP_OneGzipFileSpecified.grf", null, "(sftp://test:test@koule/home/test/data-in/file2.zip)"));
		check("query", Arrays.asList("", "hash=SHA560;god=yes", null, "", null, ""));
		check("ref", Arrays.asList("", "autodestruct", null, "", null, "innerfolder2/URLIn21.txt"));
	}
	
	public void test_stringlib_escapeUrl() {
		doCompile("test_stringlib_escapeUrl");
		check("escaped", "http://example.com/foo%20bar%5E");
		check("unescaped", "http://example.com/foo bar^");
	}
	
	public void test_stringlib_escapeUrl_unescapeUrl_expect_error(){
		//test: escape - empty string
		try {
			doCompile("string test; function integer transform() {test = escapeUrl(''); return 0;}","test_stringlib_escapeUrl_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: escape - null string 
		try {
			doCompile("string test; function integer transform() {test = escapeUrl(null); return 0;}","test_stringlib_escapeUrl_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: unescape - empty string
		try {
			doCompile("string test; function integer transform() {test = unescapeUrl(''); return 0;}","test_stringlib_escapeUrl_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: unescape - null
		try {
			doCompile("string test; function integer transform() {test = unescapeUrl(null); return 0;}","test_stringlib_escapeUrl_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: escape - invalid URL
		try {
			doCompile("string test; function integer transform() {test = escapeUrl('somewhere over the rainbow'); return 0;}","test_stringlib_escapeUrl_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
		//test: unescpae - invalid URL
		try {
			doCompile("string test; function integer transform() {test = unescapeUrl('mister%20postman'); return 0;}","test_stringlib_escapeUrl_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_stringlib_resolveParams() {
		doCompile("test_stringlib_resolveParams");
		check("resultNoParams", "Special character representing new line is: \\n calling CTL function MESSAGE; $DATAIN_DIR=./data-in");
		check("resultFalseFalseParams", "Special character representing new line is: \\n calling CTL function `uppercase(\"message\")`; $DATAIN_DIR=./data-in");
		check("resultTrueFalseParams", "Special character representing new line is: \n calling CTL function `uppercase(\"message\")`; $DATAIN_DIR=./data-in");
		check("resultFalseTrueParams", "Special character representing new line is: \\n calling CTL function MESSAGE; $DATAIN_DIR=./data-in");
		check("resultTrueTrueParams", "Special character representing new line is: \n calling CTL function MESSAGE; $DATAIN_DIR=./data-in");
	}
	
	public void test_utillib_getEnvironmentVariables() {
		doCompile("test_utillib_getEnvironmentVariables");
		check("empty", false);
	}

	public void test_utillib_getJavaProperties() {
		String key1 = "my.testing.property";
		String key2 = "my.testing.property2";
		String value = "my value";
		String value2;
		assertNull(System.getProperty(key1));
		assertNull(System.getProperty(key2));
		System.setProperty(key1, value);
		try {
			doCompile("test_utillib_getJavaProperties");
			value2 = System.getProperty(key2);
		} finally {
			System.clearProperty(key1);
			assertNull(System.getProperty(key1));
			System.clearProperty(key2);
			assertNull(System.getProperty(key2));
		}
		check("java_specification_name", "Java Platform API Specification");
		check("my_testing_property", value);
		assertEquals("my value 2", value2);
	}

	public void test_utillib_getParamValues() {
		doCompile("test_utillib_getParamValues");
		Map<String, String> params = new HashMap<String, String>();
		params.put("PROJECT", ".");
		params.put("DATAIN_DIR", "./data-in");
		params.put("COUNT", "3");
		params.put("NEWLINE", "\\n"); // special characters should NOT be resolved
		check("params", params);
	}

	public void test_utillib_getParamValue() {
		doCompile("test_utillib_getParamValue");
		Map<String, String> params = new HashMap<String, String>();
		params.put("PROJECT", ".");
		params.put("DATAIN_DIR", "./data-in");
		params.put("COUNT", "3");
		params.put("NEWLINE", "\\n"); // special characters should NOT be resolved
		params.put("NONEXISTING", null);
		check("params", params);
	}

	public void test_stringlib_getUrlParts() {
		doCompile("test_stringlib_getUrlParts");
		
		List<Boolean> isUrl = Arrays.asList(true, true, true, true, false);
		List<String> path = Arrays.asList(
				"/users/a6/15e83578ad5cba95c442273ea20bfa/msf-183/out5.txt",
				"/data-in/fileOperation/input.txt",
				"/data/file.txt",
				"/data/file.txt",
				null);
		List<String> protocol = Arrays.asList("sftp", "sandbox", "ftp", "https", null);
		List<String> host = Arrays.asList(
				"ava-fileManipulator1-devel.getgooddata.com",
				"cloveretl.test.scenarios",
				"ftp.test.com",
				"www.test.com",
				null);
		List<Integer> port = Arrays.asList(-1, -1, 21, 80, -2);
		List<String> userInfo = Arrays.asList(
				"user%40gooddata.com:password",
				"",
				"test:test",
				"test:test",
				null);
		List<String> ref = Arrays.asList("", "", "", "", null);
		List<String> query = Arrays.asList("", "", "", "", null);

		check("isUrl", isUrl);
		check("path", path);
		check("protocol", protocol);
		check("host", host);
		check("port", port);
		check("userInfo", userInfo);
		check("ref", ref);
		check("query", query);
		
		check("isURL_empty", false);
		check("path_empty", null);
		check("protocol_empty", null);
		check("host_empty", null);
		check("port_empty", -2);
		check("userInfo_empty", null);
		check("ref_empty", null);
		check("query_empty", null);
		
		check("isURL_null", false);
		check("path_null", null);
		check("protocol_null", null);
		check("host_null", null);
		check("port_null", -2);
		check("userInfo_null", null);
		check("ref_null", null);
		check("query_empty", null);
	}

	public void test_utillib_iif() throws UnsupportedEncodingException{
		doCompile("test_utillib_iif");
		check("ret1", "Renektor");
		Calendar cal = Calendar.getInstance();
		cal.set(2005,10,12,0,0,0);
		cal.set(Calendar.MILLISECOND,0);
		check("ret2", cal.getTime());
		checkArray("ret3", "Akali".getBytes("UTF-8"));
		check("ret4", 236);
		check("ret5", 78L);
		check("ret6", 78.2d);
		check("ret7", new BigDecimal("87.69"));
		check("ret8", true);
	}
	
	public void test_utillib_iif_expect_error(){
		try {
			doCompile("function integer transform(){boolean b = null; string str = iif(b, 'Rammus', 'Sion'); return 0;}","test_utillib_iif_expect_error");
			fail();
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void test_utillib_isnull(){
		doCompile("test_utillib_isnull");
		check("ret1", false);
		check("ret2", true);
		check("ret3", false);
		check("ret4", true);
		check("ret5", false);
		check("ret6", true);
		check("ret7", false);
		check("ret8", true);
		check("ret9", false);
		check("ret10", true);
		check("ret11", false);
		check("ret12", true);
		check("ret13", false);
		check("ret14", true);
		check("ret15", false);
		check("ret16", true);
		check("ret17", false);
		check("ret18", true);
		check("ret19", true);
	}
	
	public void test_utillib_nvl() throws UnsupportedEncodingException{
		doCompile("test_utillib_nvl");
		check("ret1", "Fiora");
		check("ret2", "Olaf");
		checkArray("ret3", "Elise".getBytes("UTF-8"));
		checkArray("ret4", "Diana".getBytes("UTF-8"));
		Calendar cal = Calendar.getInstance();
		cal.set(2005,4,13,0,0,0);
		cal.set(Calendar.MILLISECOND, 0);
		check("ret5", cal.getTime());
		cal.clear();
		cal.set(2004,2,14,0,0,0);
		cal.set(Calendar.MILLISECOND, 0);
		check("ret6", cal.getTime());
		check("ret7", 7);
		check("ret8", 8);
		check("ret9", 111l);
		check("ret10", 112l);
		check("ret11", 10.1d);
		check("ret12", 10.2d);
		check("ret13", new BigDecimal("12.2"));
		check("ret14", new BigDecimal("12.3"));
//		check("ret15", null);
	}
	
	public void test_utillib_nvl2() throws UnsupportedEncodingException{
		doCompile("test_utillib_nvl2");
		check("ret1", "Ahri");
		check("ret2", "Galio");
		checkArray("ret3", "Mordekaiser".getBytes("UTF-8"));
		checkArray("ret4", "Zed".getBytes("UTF-8"));
		Calendar cal = Calendar.getInstance();
		cal.set(2010,4,18,0,0,0);
		cal.set(Calendar.MILLISECOND, 0);
		check("ret5", cal.getTime());
		cal.clear();
		cal.set(2008,7,9,0,0,0);
		cal.set(Calendar.MILLISECOND, 0);
		check("ret6", cal.getTime());
		check("ret7", 11);
		check("ret8", 18);
		check("ret9", 20L);
		check("ret10", 23L);
		check("ret11", 15.2d);
		check("ret12", 89.3d);
		check("ret13", new BigDecimal("22.2"));
		check("ret14", new BigDecimal("55.5"));
		check("ret15", null);
		check("ret16", null);
	}
}
