package org.jetel.ctl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import junit.framework.AssertionFailedError;

import org.jetel.component.CTLRecordTransform;
import org.jetel.data.DataRecord;
import org.jetel.data.SetVal;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.lookup.LookupTableFactory;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.primitive.TypedProperties;

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
	
	/** Flag to trigger Java compilation */
	private boolean compileToJava;

	
	public CompilerTestCase(boolean compileToJava) {
		this.compileToJava = compileToJava;
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
		g.addLookupTable(createDefaultLookup(g,"TestLookup"));
		
		return g;
	}
	
	protected Sequence createDefaultSequence(TransformationGraph graph, String name) {
		Sequence seq = SequenceFactory.createSequence(graph, "PRIMITIVE_SEQUENCE", new Object[] { "Sequence0", graph, name}, new Class[] { String.class, TransformationGraph.class, String.class });
		
		try {
			seq.checkConfig(new ConfigurationStatus());
			seq.init();
		} catch (ComponentNotReadyException e) {
			throw new RuntimeException(e);
		}

		return seq;
	}

	/**
	 * Creates default lookup table of type SimpleLookupTable with 4 records
	 * using default metadata and a composite lookup key Name+Value.
	 * Use field City for testing response.
	 * 
	 * @param graph
	 * @param name
	 * @return
	 */
	protected LookupTable createDefaultLookup(TransformationGraph graph, String name) {
		final TypedProperties props = new TypedProperties();
		props.setProperty("id","LookupTable0"); 
		props.setProperty("type", "simpleLookup");  
		props.setProperty("metadata",LOOKUP);
		props.setProperty("key", "Name;Value"); 
		props.setProperty("name", name);
		props.setProperty("keyDuplicates", "true");
		
		/*
		 * The test lookup table is populated from file TestLookup.dat.
		 * Alternatively uncomment the populating code below, however this will most probably
		 * break down test_lookup() because free() will wipe away all data and noone will restore them
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

		
		/* ********* POPULATING CODE *************
		DataRecord lkpRecord = createEmptyRecord(createDefaultMetadata("lookupResponse"));
		
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
		lkp.put(lkpRecord);
		
		* ************ END OF POPULATING CODE *************/
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
		List<ErrorMessage> messages = compiler.compile(expStr,CTLRecordTransform.class, testIdentifier);
		printMessages(messages);
		if (compiler.errorCount() > 0) {
			throw new AssertionFailedError("Error in execution. Check standard output for details.");
		}

//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
		
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
		List<ErrorMessage> messages = compiler.compile(expStr,CTLRecordTransform.class, testIdentifier);
		printMessages(messages);
		
		if (compiler.errorCount() == 0) {
			throw new AssertionFailedError("No errors in parsing. Expected " + errCodes.size() + " errors.");
		}

		if (compiler.errorCount() != errCodes.size()) {
			throw new AssertionFailedError(compiler.errorCount() + " errors in code, but expected " + errCodes.size() + " errors.");
		}
		

		Iterator<String> it = errCodes.iterator();
		
		for (ErrorMessage errorMessage: compiler.getDiagnosticMessages()) {
			String expectedError = it.next();
			if (!expectedError.equals(errorMessage.getErrorMessage())) {
				throw new AssertionFailedError("Error : \'" + compiler.getDiagnosticMessages().get(0).getErrorMessage() + "\', but expected: \'" + expectedError + "\'");
			}
		}

//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
		
//		executeCode(compiler);
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
			throw new RuntimeException("I/O error occured when reading source file",e);
		}
		
		doCompileExpectError(sourceCode.toString(),testIdentifier, errCodes);
	}

	
	
	/**
	 * Method loads tested CTL code from a file with the name <code>testIdentifier.ctl</code>
	 * The CTL code files should be stored in the same directory as this class.
	 * 
	 * @param Test identifier defining  CTL file to load code from
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
			throw new RuntimeException("I/O error occured when reading source file",e);
		}
		
		doCompile(sourceCode.toString(),testIdentifier);
	}
	
	

	/**
	 * Method to execute tested CTL code in a way specific to testing scenario.
	 * 
	 * @param compiler
	 */
	public abstract void executeCode(ITLCompiler compiler);

	protected void printMessages(List<ErrorMessage> diagnosticMessages) {
		for (ErrorMessage e : diagnosticMessages) {
			System.out.println(e);
		}
	}

	/**
	 * Compares two records if they have the same number of fields
	 * and identical values in their fields.
	 * Does not consider (or examine) metadata.
	 * 
	 * @param lhs
	 * @param rhs
	 * @return	true if records have the same number of fields and the same values in them
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
		for (int i=0; i<lhs.getNumFields(); i++) {
			if (lhs.getField(i).isNull()) {
				if (! rhs.getField(i).isNull()) {
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
		for (int i=0; i<values.length; i++) {
			ret.add(values[i]);
		}
		return ret;
	}

}
