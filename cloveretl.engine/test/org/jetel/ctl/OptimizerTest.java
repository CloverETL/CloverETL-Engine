package org.jetel.ctl;

import java.net.URL;
import java.util.Arrays;
import java.util.List;

import junit.framework.AssertionFailedError;

import org.jetel.component.CTLRecordTransform;
import org.jetel.component.CTLRecordTransformAdapter;
import org.jetel.component.RecordTransform;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.ASTnode.CLVFStart;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

public class OptimizerTest extends CompilerTestCase {

	public OptimizerTest() {
		super(false);
	}
	
	private TransformLangExecutor executor;
	
	public void executeCode(ITLCompiler compiler) { 
		TransformationGraph graph = createDefaultGraph();
		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
	
		executor = (TransformLangExecutor)compiler.getCompiledCode();
		
		executor.setInputRecords(inputRecords);
		executor.setOutputRecords(outputRecords);
		executor.setRuntimeLogger(graph.getLogger());
		
		executor.keepGlobalScope();
		executor.init();
		executor.execute();
		CLVFFunctionDeclaration transform = executor.getFunction("transform");
		if (transform == null) {
			System.err.println("Function 'transform' not found and will not be executed");
			return;
		}
		
		executor.executeFunction(transform, new Object[0]);
	}
	
	protected void check(String varName, Object value) {
		assertEquals(varName,value,executor.getVariableValue(varName));
	}
	
	
	public void test_parser() {
		System.out.println("\nParser test:");

		String expStr = "/*#TL:COMPILED\n*/\n" + 
						"// this is other comment\n" + 
						"for (int i=0; i<5; i++) \n" + 
						"  if (i%2 == 0) {\n" +
						"		continue;\n" + 
						"  }\n";
		TransformationGraph graph = createDefaultGraph();
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };

		print_code(expStr);
		ITLCompiler compiler = TLCompilerFactory.createCompiler(graph, inMetadata, outMetadata, "UTF-8");
		List<ErrorMessage> messages = compiler.compile(expStr, CTLRecordTransform.class,"parser_test");
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
//		String expStr =
//			"function string computeSomething(int n) {\n" +
//			"	string s = '';\n" +
//			"	do  {\n" +
//			"		int i = n--;\n" +
//			"		s = s + '-' + i;\n" +
//			"	} while (n > 0)\n" +
//			"	return s;" + 
//			"}\n\n" +
//			"function int transform() {\n" +
//			"	print_err(computeSomething(10));\n" + 
//			"   return 0;\n" + 
//			"}";
		URL importLoc = getClass().getResource("samplecode.ctl");
		String expStr = "import '" + importLoc + "';\n";
		
//			"function int getIndexOfOffsetStart(string encodedDate) {\n" + 
//			"int offsetStart;\n"  +
//			"int actualLastMinus;\n" + 
//			"int lastMinus = -1;\n" +
//			"if ( index_of(encodedDate, '+') != -1 )\n" + 
//			"	return index_of(encodedDate, '+');\n" +
//			"do {\n" +
//			"	actualLastMinus = index_of(encodedDate, '-', lastMinus+1);\n" +
//			"	if ( actualLastMinus != -1 )\n" +
//			"		lastMinus = actualLastMinus;\n" +
//			"} while ( actualLastMinus != -1 )\n" +
//			"return lastMinus;\n" +  
//			"}\n" +
//			"function int transform() {\n" + 
//			"	getIndexOfOffsetStart('2009-04-24T08:00:00-05:00');\n" + 
//			" 	return 0;\n" + 
//			"}\n";
		
		TransformationGraph graph = createDefaultGraph();
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };

		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };

		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };

		print_code(expStr);
		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
		List<ErrorMessage> messages = compiler.compile(expStr,CTLRecordTransform.class,"test_scope");
		printMessages(messages);

		if (messages.size() > 0) {
			throw new AssertionFailedError("Error in execution. Check standard output for details.");
		}

		CTLRecordTransformAdapter executor = new CTLRecordTransformAdapter((TransformLangExecutor)compiler.getCompiledCode(),graph.getLogger());
		executor.init(null,inMetadata,outMetadata);
		executor.transform(inputRecords, outputRecords);
		
	}
	
//	public void test_int() {
//		System.out.println("int test:");
//		String expStr = "int i; i=0; print_err(i); \n" +
//						"int j; j=-1; print_err(j);\n" + 
//						"int minInt; minInt=" + Integer.MIN_VALUE + "; print_err(minInt, true);\n" + 
//						"int maxInt; maxInt=" + Integer.MAX_VALUE + "; print_err(maxInt, true);\n" + 
//						"int field; field=$firstInput.Value; print_err(field);\n" + 
//						"int nullValue; nullValue = null; print_err(nullValue);\n" + 
//						"int varWithInitializer = 123; print_err(varWithInitializer);\n" +
//						"int varWithNullInitializer = null; print_err(varWithNullInitializer);\n";
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		
//		ITLCompiler compiler = TLCompilerFactory.createCompiler(graph, inMetadata, outMetadata, "UTF-8");
//		List<ErrorMessage> messages = compiler.compile(expStr,DataRecordTransform.class, "test_int");
//		printMessages(messages);
//		if (compiler.errorCount() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		assertEquals(0, executor.getVariableValue("i"));
//		assertEquals(-1, executor.getVariableValue("j"));
//		assertEquals(Integer.MIN_VALUE, executor.getVariableValue("minInt"));
//		assertEquals(Integer.MAX_VALUE, executor.getVariableValue("maxInt"));
//		assertEquals(VALUE_VALUE, executor.getVariableValue("field"));
//		assertNull(executor.getVariableValue("nullValue"));
//		assertEquals(123,executor.getVariableValue("varWithInitializer"));
//		assertNull(executor.getVariableValue("varWithNullInitializer"));
//
//	}
//
//	public void test_long() {
//		System.out.println("\nlong test:");
//		String expStr = "long i; i=0; print_err(i); \n" + 
//						"long j; j=-1; print_err(j);\n" + 
//						"long minLong; minLong=" + (Long.MIN_VALUE) + "L; print_err(minLong);\n" +
//						"long maxLong; maxLong=" + (Long.MAX_VALUE) + "L; print_err(maxLong);\n" + 
//						"long field; field=$firstInput.BornMillisec; print_err(field);\n" + 
//						"long def; print_err(def);\n" + 
//						"long nullValue; nullValue = null; print_err(nullValue);\n" +
//						"long varWithInitializer = 123L; print_err(varWithInitializer);\n" +
//						"long varWithNullInitializer = null; print_err(varWithNullInitializer);\n";
//
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		assertEquals(Long.valueOf(0), executor.getVariableValue("i"));
//		assertEquals(Long.valueOf(-1), executor.getVariableValue("j"));
//		assertEquals(Long.MIN_VALUE, executor.getVariableValue("minLong"));
//		assertEquals(Long.MAX_VALUE, executor.getVariableValue("maxLong"));
//		assertEquals(BORN_MILLISEC_VALUE, executor.getVariableValue("field"));
//		assertEquals(Long.valueOf(0), executor.getVariableValue("def"));
//		assertNull(executor.getVariableValue("nullValue"));
//		assertEquals(123L,executor.getVariableValue("varWithInitializer"));
//		assertNull(executor.getVariableValue("varWithNullInitializer"));
//
//	}
//
//	public void test_decimal() {
//		System.out.println("\ndecimal test:");
//		String expStr = "decimal i; i=0; print_err(i); \n" + 
//						"decimal j; j=-1.0; print_err(j);\n" + 
//						"decimal minLong; minLong=" + String.valueOf(Long.MIN_VALUE) + "d; print_err(minLong);\n" + 
//						"decimal maxLong; maxLong=" + String.valueOf(Long.MAX_VALUE) + "d; print_err(maxLong);\n" + 
//						"decimal minLongNoDist; minLongNoDist=" + String.valueOf(Long.MIN_VALUE) + "L; print_err(minLongNoDist);\n" + 
//						"decimal maxLongNoDist; maxLongNoDist=" + String.valueOf(Long.MAX_VALUE) + "L; print_err(maxLongNoDist);\n" +
//						// distincter will cause the double-string be parsed into exact representation within BigDecimal
//						"decimal minDouble; minDouble=" + String.valueOf(Double.MIN_VALUE) + "D; print_err(minDouble);\n" + 
//						"decimal maxDouble; maxDouble=" + String.valueOf(Double.MAX_VALUE) + "D; print_err(maxDouble);\n" +
//						// no distincter will cause the double-string to be parsed into inexact representation within double
//						// then to be assigned into BigDecimal (which will extract only MAX_PRECISION digits)
//						"decimal minDoubleNoDist; minDoubleNoDist=" + String.valueOf(Double.MIN_VALUE) + "; print_err(minDoubleNoDist);\n" + 
//						"decimal maxDoubleNoDist; maxDoubleNoDist=" + String.valueOf(Double.MAX_VALUE) + "; print_err(maxDoubleNoDist);\n" +
//						"decimal field; field=$firstInput.Currency; print_err(field);\n" + 
//						"decimal def; print_err(def);\n" + 
//						"decimal nullValue; nullValue = null; print_err(nullValue);\n" +
//						"decimal varWithInitializer = 123.35D; print_err(varWithInitializer);\n" +
//						"decimal varWithNullInitializer = null; print_err(varWithNullInitializer);\n" +
//						"decimal varWithInitializerNoDist = 123.35; print_err(varWithInitializer);\n";
//		
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		assertEquals(new BigDecimal(0,MAX_PRECISION), executor.getVariableValue("i"));
//		assertEquals(new BigDecimal(-1,MAX_PRECISION), executor.getVariableValue("j"));
//		assertEquals(new BigDecimal(String.valueOf(Long.MIN_VALUE),MAX_PRECISION), executor.getVariableValue("minLong"));
//		assertEquals(new BigDecimal(String.valueOf(Long.MAX_VALUE),MAX_PRECISION), executor.getVariableValue("maxLong"));
//		assertEquals(new BigDecimal(String.valueOf(Long.MIN_VALUE),MAX_PRECISION), executor.getVariableValue("minLongNoDist"));
//		assertEquals(new BigDecimal(String.valueOf(Long.MAX_VALUE),MAX_PRECISION), executor.getVariableValue("maxLongNoDist"));
//		// distincter will cause the MIN_VALUE to be parsed into exact representation (i.e. 4.9E-324)
//		assertEquals(new BigDecimal(String.valueOf(Double.MIN_VALUE), MAX_PRECISION), executor.getVariableValue("minDouble"));
//		assertEquals(new BigDecimal(String.valueOf(Double.MAX_VALUE), MAX_PRECISION), executor.getVariableValue("maxDouble"));
//		// no distincter will cause MIN_VALUE to be parsed into double inexact representation and extraction of
//		// MAX_PRECISION digits (i.e. 4.94065.....E-324)
//		assertEquals(new BigDecimal(Double.MIN_VALUE, MAX_PRECISION), executor.getVariableValue("minDoubleNoDist"));
//		assertEquals(new BigDecimal(Double.MAX_VALUE, MAX_PRECISION), executor.getVariableValue("maxDoubleNoDist"));
//		assertEquals(CURRENCY_VALUE, executor.getVariableValue("field"));
//		assertEquals(new BigDecimal(0,MAX_PRECISION), executor.getVariableValue("def"));
//		assertNull(executor.getVariableValue("nullValue"));
//		assertEquals(new BigDecimal("123.35",MAX_PRECISION),executor.getVariableValue("varWithInitializer"));
//		assertNull(executor.getVariableValue("varWithNullInitializer"));
//		assertEquals(new BigDecimal(123.35,MAX_PRECISION),executor.getVariableValue("varWithInitializerNoDist"));
//
//	}
//
//	public void test_number() {
//		System.out.println("\nnumber test:");
//		String expStr = "number i; i=0; print_err(i); \n" + 
//						"number j; j=-1.0; print_err(j);\n" + 
//						"number minDouble; minDouble=" + Double.MIN_VALUE + "; print_err(minDouble);\n" + 
//						"number maxDouble; maxDouble=" + Double.MAX_VALUE + "; print_err(maxDouble);\n" + 
//						"number field; field = $firstInput.Age; print_err(field);\n" + 
//						"number def;print_err(def);\n" + 
//						"number nullValue; nullValue = null; print_err(nullValue);\n" +
//						"number varWithNullInitializer = null; print_err(varWithNullInitializer);\n";
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		assertEquals(Double.valueOf(0), executor.getVariableValue("i"));
//		assertEquals(Double.valueOf(-1), executor.getVariableValue("j"));
//		assertEquals(Double.valueOf(Double.MIN_VALUE), executor.getVariableValue("minDouble"));
//		assertEquals(Double.valueOf(Double.MAX_VALUE), executor.getVariableValue("maxDouble"));
//		assertEquals(AGE_VALUE, executor.getVariableValue("field"));
//		assertEquals(Double.valueOf(0), executor.getVariableValue("def"));
//		assertNull(executor.getVariableValue("nullValue"));
//		assertNull(executor.getVariableValue("varWithNullInitializer"));
//	}
//
//	public void test_string() {
//		System.out.println("\nstring test:");
//		int length = 1000;
//		StringBuilder tmp = new StringBuilder(length);
//		for (int i = 0; i < length; i++) {
//			tmp.append(i % 10);
//		}
//		String expStr = "string i; i=\"0\"; print_err(i); \n" + 
//						"string helloEscaped; helloEscaped='hello\\nworld'; print_err(helloEscaped);\n" + 
//						"string helloExpanded; helloExpanded=\"hello\\nworld\"; print_err(helloExpanded);\n" + 
//						"string fieldName; fieldName=$Name; print_err(fieldName);\n" + 
//						"string fieldCity; fieldCity=$City; print_err(fieldCity);\n" + 
//						"string longString; longString=\"" + tmp + "\"; print_err(longString);\n" + 
//						"string escapeChars; escapeChars='a\u0101\u0102A'; print_err(escapeChars);\n" + 
//						"string doubleEscapeChars; doubleEscapeChars='a\\u0101\\u0102A'; print_err(doubleEscapeChars);\n" + 
//						"string specialChars; specialChars='špeciálne značky s mäkčeňom môžu byť'; print_err(specialChars);\n" + 
//						"string dQescapeChars; dQescapeChars=\"a\u0101\u0102A\"; print_err(dQescapeChars);\n" + 
//						"string dQdoubleEscapeChars; dQdoubleEscapeChars=\"a\\u0101\\u0102A\"; print_err(dQdoubleEscapeChars);\n" + 
//						"string dQspecialChars; dQspecialChars=\"špeciálne značky s mäkčeňom môžu byť\"; print_err(dQspecialChars);\n" + 
//						"string empty=\"\";print_err(empty+specialChars);\n" + 
//						"string def; print_err(def);\n" + 
//						"string nullValue; nullValue = null; print_err(nullValue);\n" +
//						"string varWithNullInitializer = null; print_err(varWithNullInitializer);\n";
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		assertEquals("0", executor.getVariableValue("i"));
//		assertEquals("hello\\nworld", executor.getVariableValue("helloEscaped"));
//		assertEquals("hello\nworld", executor.getVariableValue("helloExpanded"));
//		assertEquals(NAME_VALUE, executor.getVariableValue("fieldName"));
//		assertEquals(CITY_VALUE, executor.getVariableValue("fieldCity"));
//		assertEquals(String.valueOf(tmp), executor.getVariableValue("longString"));
//		assertEquals("a\u0101\u0102A", executor.getVariableValue("escapeChars"));
//		assertEquals("a\u0101\u0102A", executor.getVariableValue("doubleEscapeChars"));
//		assertEquals("špeciálne značky s mäkčeňom môžu byť", executor.getVariableValue("specialChars"));
//		assertEquals("a\u0101\u0102A", executor.getVariableValue("dQescapeChars"));
//		assertEquals("a\u0101\u0102A", executor.getVariableValue("dQdoubleEscapeChars"));
//		assertEquals("špeciálne značky s mäkčeňom môžu byť", executor.getVariableValue("dQspecialChars"));
//		assertEquals("", executor.getVariableValue("empty"));
//		assertEquals("",executor.getVariableValue("def"));
//		assertNull(executor.getVariableValue("varWithNullInitializer"));
//	}
//
//	public void test_date() {
//		System.out.println("\ndate test:");
//		String expStr = "date d3; d3=2006-08-01; print_err(d3);\n" + 
//						"date d2; d2=2006-08-02 15:15:03 ; print_err(d2);\n" + 
//						"date d1; d1=2006-1-1 1:2:3; print_err(d1);\n" + 
//						"date field; field=$firstInput.Born; print_err(field);\n" + 
//						"date nullValue; nullValue = null; print_err(nullValue);\n" + 
//						"date minValue; minValue = 1970-01-01 01:00:00; print_err(minValue);\n" +
//						"date varWithNullInitializer = null; print_err(varWithNullInitializer);\n";
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		assertEquals(new GregorianCalendar(2006, GregorianCalendar.AUGUST, 1).getTime(), executor.getVariableValue("d3"));
//		assertEquals(new GregorianCalendar(2006, GregorianCalendar.AUGUST, 2, 15, 15, 3).getTime(), executor.getVariableValue("d2"));
//		assertEquals(new GregorianCalendar(2006, GregorianCalendar.JANUARY, 1, 1, 2, 3).getTime(), executor.getVariableValue("d1"));
//		assertEquals(BORN_VALUE, executor.getVariableValue("field"));
//		assertNull(executor.getVariableValue("nullValue"));
//		assertEquals(new GregorianCalendar(1970, GregorianCalendar.JANUARY, 1, 1, 0, 0).getTime(), executor.getVariableValue("minValue"));
//		assertNull(executor.getVariableValue("varWithNullInitializer"));
//	}
//
//	public void test_boolean() {
//		System.out.println("\nboolean test:");
//		String expStr = "boolean b1; b1=true; print_err(b1);\n" + 
//						"boolean b2; b2=false ; print_err(b2);\n" + 
//						"boolean b4; print_err(b4);\n" +
//						"boolean nullValue; nullValue = null; print_err(nullValue);\n" +
//						"boolean varWithNullInitializer = null; print_err(varWithNullInitializer);\n";
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		
//		assertEquals(true, executor.getVariableValue("b1"));
//		assertEquals(false, executor.getVariableValue("b2"));
//		assertEquals(false, executor.getVariableValue("b4"));
//		assertNull(executor.getVariableValue("nullValue"));
//		assertNull(executor.getVariableValue("varWithNullInitializer"));
//	}
//	
//	
//	@SuppressWarnings("unchecked")
//	public void test_list() {
//		System.out.println("\nList test:");
//		String expStr = 
//						"int[] intList;\n" +
//						"intList[0] = 1;\n" + 
//						"intList[1] = 2;\n" +
//						"intList[2] = intList[0]+intList[1];\n" +
//						"intList[3] = 3;\n" + 
//						"intList[3] = 4;\n" +
//						"string[] stringList = ['first', 'second', 'third'];\n" +
//						"stringList[1] = 'replaced';\n" +
//						"string[] stringListCopy = stringList + [ 'fourth' ];\n" +
//						"stringListCopy = stringListCopy + [ 'fifth' ];\n";
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		
//		assertEquals(Integer.valueOf(1), ((List<Integer>)executor.getVariableValue("intList")).get(0));
//		assertEquals(Integer.valueOf(2), ((List<Integer>)executor.getVariableValue("intList")).get(1));
//		assertEquals(Integer.valueOf(3), ((List<Integer>)executor.getVariableValue("intList")).get(2));
//		assertEquals(Integer.valueOf(4), ((List<Integer>)executor.getVariableValue("intList")).get(3));
//		assertEquals(4, ((List<Integer>)executor.getVariableValue("intList")).size());
//		assertEquals("first", ((List<String>)executor.getVariableValue("stringList")).get(0));
//		assertEquals("replaced", ((List<String>)executor.getVariableValue("stringList")).get(1));
//		assertEquals("third", ((List<String>)executor.getVariableValue("stringList")).get(2));
//		assertEquals("fourth", ((List<String>)executor.getVariableValue("stringList")).get(3));
//		assertEquals("fifth", ((List<String>)executor.getVariableValue("stringList")).get(4));
//		assertEquals((List<String>)executor.getVariableValue("stringList"),(List<String>)executor.getVariableValue("stringListCopy"));
//	}
//	
//	@SuppressWarnings("unchecked")
//	public void test_map() {
//		System.out.println("\nMap test:");
//		String expStr = "map[string,int] testMap;\n" +
//						"testMap['zero'] = 1;\n" + 
//						"testMap['one'] = 2;\n" +
//						"testMap['two'] = testMap['zero']+testMap['one'];\n" +
//						"testMap['three'] = 3;\n" + 
//						"testMap['three'] = 4;\n" +
//						// map concatenation
//						"map[date,string] dayInWeek;\n" +
//						"dayInWeek[2009-03-02] = 'Monday';\n" +
//						"dayInWeek[2009-03-03] = 'unknown';\n" +
//						"map[date,string] tuesday;\n" +
//						"tuesday[2009-03-03] = 'Tuesday';\n" +
//						"map[date,string] dayInWeekCopy = dayInWeek + tuesday;\n" +
//						"map[date,string] wednesday;\n" +
//						// concat affects the original list as well
//						"wednesday[2009-03-04] = 'Wednesday';\n" +
//						"dayInWeekCopy = dayInWeekCopy + wednesday;\n";
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		
//		assertEquals(Integer.valueOf(1), ((Map<String,Integer>)executor.getVariableValue("testMap")).get("zero"));
//		assertEquals(Integer.valueOf(2), ((Map<String,Integer>)executor.getVariableValue("testMap")).get("one"));
//		assertEquals(Integer.valueOf(3), ((Map<String,Integer>)executor.getVariableValue("testMap")).get("two"));
//		assertEquals(Integer.valueOf(4), ((Map<String,Integer>)executor.getVariableValue("testMap")).get("three"));
//		assertEquals(4, ((Map<String,Integer>)executor.getVariableValue("testMap")).size());
//	
//		Calendar c = Calendar.getInstance();
//		c.set(2009,Calendar.MARCH,2,0,0,0);
//		c.set(Calendar.MILLISECOND, 0);
//		assertEquals("Monday", ((Map<Date,String>)executor.getVariableValue("dayInWeek")).get(c.getTime()));
//
//		c.set(2009,Calendar.MARCH,3,0,0,0);
//		c.set(Calendar.MILLISECOND, 0);
//		assertEquals("Tuesday", ((Map<Date,String>)executor.getVariableValue("tuesday")).get(c.getTime()));
//		assertEquals("Tuesday", ((Map<Date,String>)executor.getVariableValue("dayInWeek")).get(c.getTime()));
//		
//		c.set(2009,Calendar.MARCH,4,0,0,0);
//		c.set(Calendar.MILLISECOND, 0);
//		assertEquals("Wednesday", ((Map<Date,String>)executor.getVariableValue("wednesday")).get(c.getTime()));
//		assertEquals("Wednesday", ((Map<Date,String>)executor.getVariableValue("dayInWeek")).get(c.getTime()));
//		assertEquals(executor.getVariableValue("dayInWeek"),executor.getVariableValue("dayInWeekCopy"));
//	}
//	
//	public void test_record() {
//		System.out.println("\nRecord test:");
//		String expStr = // copy field by value
//						"firstInput copy; copy.* = $firstInput.*;\n" +
//						// copy fields by value and modify - original record is untouched (input -> record variable)
//						"firstInput modified; modified.* = $firstInput.*;\n" + 
//						"modified.Name = 'empty';\n" +
//						"modified.Value = 321;\n" +
//						"modified.Born = 1987-11-13;\n" +
//						// copy fields by value and modify (record variable -> record variable) 
//						"firstInput modified2; modified2.* = modified.*;\n" + 
//						"modified2.Name = 'not empty';\n" +
//						// copy reference and modify the original target as well
//						"firstInput modified3; modified3.* = modified2.*;\n" + 
//						"firstInput reference = modified3;\n" +  
//						"reference.Value = 654321;\n" +
//						// copy fields by value to output (record variable -> output)
//						"$secondOutput.* = reference.*;\n" +
//						// set all fields in record to null value
//						"firstInput nullRecord; nullRecord.* = $firstInput.*;\n" + 
//						"nullRecord.* = null;\n" ;
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		// expected result 
//		DataRecord expected = createDefaultRecord(createDefaultMetadata("expected"));
//		
//		// simple copy
//		assertTrue(recordEquals(expected,inputRecords[0]));
//		assertTrue(recordEquals(expected,(DataRecord)executor.getVariableValue("copy")));
//		
//		// copy and modify
//		expected.getField("Name").setValue("empty");
//		expected.getField("Value").setValue(321);
//		Calendar c = Calendar.getInstance();
//		c.set(1987,Calendar.NOVEMBER,13,0,0,0);
//		c.set(Calendar.MILLISECOND, 0);
//		expected.getField("Born").setValue(c.getTime());
//		assertTrue(recordEquals(expected, (DataRecord)executor.getVariableValue("modified")));
//
//		// 2x modified copy
//		expected.getField("Name").setValue("not empty");
//		assertTrue(recordEquals(expected, (DataRecord)executor.getVariableValue("modified2")));
//		
//		// modified by reference
//		expected.getField("Value").setValue(654321);
//		assertTrue(recordEquals(expected, (DataRecord)executor.getVariableValue("modified3")));
//		assertTrue(recordEquals(expected, (DataRecord)executor.getVariableValue("reference")));
//		assertTrue(executor.getVariableValue("modified3") == executor.getVariableValue("reference"));
//		
//		// output record
//		assertTrue(recordEquals(expected, outputRecords[1]));
//		
//		// null record
//		expected.setToNull();
//		assertTrue(recordEquals(expected, (DataRecord)executor.getVariableValue("nullRecord")));
//	}
//
//	
//	public void test_variables(){
//		System.out.println("\nvariable test:");
//		String expStr = "boolean b1; boolean b2; b1=true; print_err(b1);\n"+
//						"b2=false ; print_err(b2);\n"+
//						"string b4; b4=\"hello\"; print_err(b4);\n"+
//						"b2 = true; print_err(b2);\n" +
//						"int inside;\n" +
//						"if (b2) {" +
//						"	inside=2;\n" +
//						"	print_err('in');\n" +
//						"}\n" +
//						"print_err(b2);\n" +
//						"b4=null; print_err(b4);\n"+
//						"b4='hi'; print_err(b4);\n";
//
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		
//		assertEquals(true,executor.getVariableValue("b1"));
//		assertEquals(true,executor.getVariableValue("b2"));
//		assertEquals("hi",executor.getVariableValue("b4"));
//		assertEquals(2,executor.getVariableValue("inside"));
//		      
//	}
//
//	
//	public void test_plus(){
//		System.out.println("\nplus test:");
//		String expStr = "int i; i=10;\n"+
//						"int j; j=100;\n" +
//						"int iplusj;iplusj=i+j; print_err(\"plus int:\"+iplusj);\n" +
//						"long l;l="+Integer.MAX_VALUE/10+"l;print_err(l);\n" +
//						"long m;m="+(Integer.MAX_VALUE)+"l;print_err(m);\n" +
//						"long lplusm;lplusm=l+m;print_err(\"plus long:\"+lplusm);\n" +
//						"long mplusl;mplusl=m+l;print_err(\"plus long:\"+mplusl);\n" +
//						"long mplusi;mplusi=m+i;print_err(\"long plus int:\"+mplusi);\n" +
//						"long iplusm;iplusm=i+m;print_err(\"int plus long:\"+iplusm);\n" +
//						"number n; n=0.1;print_err(n);\n" +
//						"number m1; m1=0.001;print_err(m1);\n" +
//						"number nplusm1; nplusm1=n+m1;print_err(\"plus number:\"+nplusm1);\n" +
//						"number nplusj;nplusj=n+j;print_err(\"number plus int:\"+nplusj);\n"+
//						"number jplusn;jplusn=j+n;print_err(\"int plus number:\"+jplusn);\n"+
//						"number m1plusm;m1plusm=m1+m;print_err(\"number plus long:\"+m1plusm);\n"+
//						"number mplusm1;mplusm1=m+m1;print_err(\"long plus number:\"+mplusm1);\n"+
//						"decimal d; d=0.1D;print_err(d);\n" +
//						"decimal d1; d1=0.0001D;print_err(d1);\n" +
//						"decimal dplusd1; dplusd1=d+d1;print_err(\"plus decimal:\"+dplusd1);\n" +
//						"decimal dplusj;dplusj=d+j;print_err(\"decimal plus int:\"+dplusj);\n" +
//						"decimal jplusd;jplusd=j+d;print_err(\"int plus decimal:\"+jplusd);\n" +
//						"decimal dplusm;dplusm=d+m;print_err(\"decimal plus long:\"+dplusm);\n" +
//						"decimal mplusd;mplusd=m+d;print_err(\"long plus decimal:\"+mplusd);\n" +
//						"decimal dplusn;dplusn=d+n;print_err(\"decimal plus number:\"+dplusn);\n" +
//						"decimal nplusd;nplusd=n+d;print_err(\"number plus decimal:\"+nplusd);\n" +
//						"string s; s=\"hello\"; print_err(s);\n" +
//						"string s1;s1=\" world\";print_err(s1);\n " +
//						"string spluss1;spluss1=s+s1;print_err(\"adding strings:\"+spluss1);\n" +
//						"string splusj;splusj=s+j;print_err(\"string plus int:\"+splusj);\n" +
//						"string jpluss;jpluss=j+s;print_err(\"int plus string:\"+jpluss);\n" +
//						"string splusm;splusm=s+m;print_err(\"string plus long:\"+splusm);\n" +
//						"string mpluss;mpluss=m+s;print_err(\"long plus string:\"+mpluss);\n" +
//						"string splusm1;splusm1=s+m1;print_err(\"string plus number:\"+splusm1);\n" +
//						"string m1pluss;m1pluss=m1+s;print_err(\"number plus string:\"+m1pluss);\n" +
//						"string splusd1;splusd1=s+d1;print_err(\"string plus decimal:\"+splusd1);\n" +
//						"string d1pluss;d1pluss=d1+s;print_err(\"decimal plus string:\"+d1pluss);\n";
//						
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		
//		assertEquals("iplusj",10+100,(executor.getVariableValue("iplusj")));
//		assertEquals("lplusm",Long.valueOf(Integer.MAX_VALUE)+Long.valueOf(Integer.MAX_VALUE/10),executor.getVariableValue("lplusm"));
//		assertEquals("mplusl",executor.getVariableValue("mplusl"), executor.getVariableValue("lplusm"));
//		assertEquals("lplusi",Long.valueOf(Integer.MAX_VALUE)+Long.valueOf(Integer.MAX_VALUE/10),executor.getVariableValue("lplusm"));
//		assertEquals("mplusi",Long.valueOf(Integer.MAX_VALUE)+10,executor.getVariableValue("mplusi"));
//		assertEquals("iplusm",executor.getVariableValue("iplusm"),executor.getVariableValue("mplusi"));
//		assertEquals("nplusm1",Double.valueOf(0.1D + 0.001D),executor.getVariableValue("nplusm1"));
//		assertEquals("nplusj",Double.valueOf(100 + 0.1D),executor.getVariableValue("nplusj"));
//		assertEquals("jplusn",executor.getVariableValue("jplusn"),executor.getVariableValue("nplusj"));
//		assertEquals("m1plusm",Double.valueOf(Long.valueOf(Integer.MAX_VALUE) + 0.001d),executor.getVariableValue("m1plusm"));
//		assertEquals("mplusm1",executor.getVariableValue("mplusm1"),executor.getVariableValue("m1plusm"));
//		assertEquals("dplusd1",new BigDecimal("0.1",MAX_PRECISION).add(new BigDecimal("0.0001",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dplusd1"));
//		assertEquals("dplusj",new BigDecimal(100,MAX_PRECISION).add(new BigDecimal("0.1",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dplusj"));
//		assertEquals("jplusd",executor.getVariableValue("jplusd"),executor.getVariableValue("dplusj"));
//		assertEquals("dplusm",new BigDecimal(Long.valueOf(Integer.MAX_VALUE),MAX_PRECISION).add(new BigDecimal("0.1",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dplusm"));
//		assertEquals("mplusd", executor.getVariableValue("mplusd"), executor.getVariableValue("dplusm"));
//		assertEquals("dplusn",new BigDecimal("0.1").add(new BigDecimal(0.1D,MAX_PRECISION)),executor.getVariableValue("dplusn"));
//		assertEquals("nplusd",executor.getVariableValue("nplusd"),executor.getVariableValue("dplusn"));
//		assertEquals("spluss1","hello world",executor.getVariableValue("spluss1"));
//		assertEquals("splusj","hello100",executor.getVariableValue("splusj"));
//		assertEquals("jpluss","100hello",executor.getVariableValue("jpluss"));
//		assertEquals("splusm","hello" + Long.valueOf(Integer.MAX_VALUE),executor.getVariableValue("splusm"));
//		assertEquals("mpluss",Long.valueOf(Integer.MAX_VALUE) + "hello",executor.getVariableValue("mpluss"));
//		assertEquals("splusm1","hello" + 0.001D,executor.getVariableValue("splusm1"));
//		assertEquals("m1pluss",0.001D + "hello",executor.getVariableValue("m1pluss"));
//		assertEquals("splusd1","hello" + new BigDecimal("0.0001"),executor.getVariableValue("splusd1"));
//		assertEquals("d1pluss",new BigDecimal("0.0001",MAX_PRECISION) + "hello",executor.getVariableValue("d1pluss"));
//	}
//
//	
//	public void test_minus(){
//		System.out.println("\nminus test:");
//		String expStr = "int i; i=10;\n"+
//						"int j; j=100;\n" +
//						"int iminusj;iminusj=i-j; print_err(\"minus int:\"+iminusj);\n" +
//						"long l;l="+Integer.MAX_VALUE/10+"l;print_err(l);\n" +
//						"long m;m="+(Integer.MAX_VALUE)+"l;print_err(m);\n" +
//						"long lminusm;lminusm=l-m;print_err(\"minus long:\"+lminusm);\n" +
//						"long mminusi;mminusi=m-i;print_err(\"long minus int:\"+mminusi);\n" +
//						"long iminusm;iminusm=i-m;print_err(\"int minus long:\"+iminusm);\n" +
//						"number n; n=0.1;print_err(n);\n" +
//						"number m1; m1=0.001;print_err(m1);\n" +
//						"number nminusm1; nminusm1=n-m1;print_err(\"minus number:\"+nminusm1);\n" +
//						"number nminusj;nminusj=n-j;print_err(\"number minus int:\"+nminusj);\n"+
//						"number jminusn;jminusn=j-n;print_err(\"int minus number:\"+jminusn);\n"+
//						"number m1minusm;m1minusm=m1-m;print_err(\"number minus long:\"+m1minusm);\n"+
//						"number mminusm1;mminusm1=m-m1;print_err(\"long minus number:\"+mminusm1);\n"+
//						"decimal d; d=0.1D;print_err(d);\n" +
//						"decimal d1; d1=0.0001D;print_err(d1);\n" +
//						"decimal dminusd1; dminusd1=d-d1;print_err(\"minus decimal:\"+dminusd1);\n" +
//						"decimal dminusj;dminusj=d-j;print_err(\"decimal minus int:\"+dminusj);\n" +
//						"decimal jminusd;jminusd=j-d;print_err(\"int minus decimal:\"+jminusd);\n" +
//						"decimal dminusm;dminusm=d-m;print_err(\"decimal minus long:\"+dminusm);\n" +
//						"decimal mminusd;mminusd=m-d;print_err(\"long minus decimal:\"+mminusd);\n" +
//						"decimal dminusn;dminusn=d-n;print_err(\"decimal minus number:\"+dminusn);\n" +
//						"decimal nminusd;nminusd=n-d;print_err(\"number minus decimal:\"+nminusd);\n";
//						
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		
//		assertEquals("iminusj",10-100,(executor.getVariableValue("iminusj")));
//		assertEquals("lminusm",Long.valueOf(Integer.MAX_VALUE/10)-Long.valueOf(Integer.MAX_VALUE),executor.getVariableValue("lminusm"));
//		assertEquals("mminusi",Long.valueOf(Integer.MAX_VALUE-10),executor.getVariableValue("mminusi"));
//		assertEquals("iminusm",10-Long.valueOf(Integer.MAX_VALUE),executor.getVariableValue("iminusm"));
//		assertEquals("nminusm1",Double.valueOf(0.1D - 0.001D),executor.getVariableValue("nminusm1"));
//		assertEquals("nminusj",Double.valueOf(0.1D - 100),executor.getVariableValue("nminusj"));
//		assertEquals("jminusn",Double.valueOf(100 - 0.1D),executor.getVariableValue("jminusn"));
//		assertEquals("m1minusm",Double.valueOf(0.001D - Long.valueOf(Integer.MAX_VALUE)),executor.getVariableValue("m1minusm"));
//		assertEquals("mminusm1",Double.valueOf(Long.valueOf(Integer.MAX_VALUE) - 0.001D),executor.getVariableValue("mminusm1"));
//		assertEquals("dminusd1",new BigDecimal("0.1",MAX_PRECISION).subtract(new BigDecimal("0.0001",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dminusd1"));
//		assertEquals("dminusj",new BigDecimal("0.1",MAX_PRECISION).subtract(new BigDecimal(100,MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dminusj"));
//		assertEquals("jminusd",new BigDecimal(100,MAX_PRECISION).subtract(new BigDecimal("0.1",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("jminusd"));
//		assertEquals("dminusm",new BigDecimal("0.1",MAX_PRECISION).subtract(new BigDecimal(Long.valueOf(Integer.MAX_VALUE),MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dminusm"));
//		assertEquals("mminusd", new BigDecimal(Long.valueOf(Integer.MAX_VALUE),MAX_PRECISION).subtract(new BigDecimal("0.1",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("mminusd"));
//		assertEquals("dminusn",new BigDecimal("0.1",MAX_PRECISION).subtract(new BigDecimal(0.1D,MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dminusn"));
//		assertEquals("nminusd",new BigDecimal(0.1D,MAX_PRECISION).subtract(new BigDecimal("0.1",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("nminusd"));
//	}
//	
//	
//	public void test_multiply(){
//		System.out.println("\nmultiply test:");
//		String expStr = "int i; i=10;\n"+
//						"int j; j=100;\n" +
//						"int itimesj;itimesj=i*j; print_err(\"times int:\"+itimesj);\n" +
//						"long l;l="+Integer.MAX_VALUE/10+"l;print_err(l);\n" +
//						"long m;m="+(Integer.MAX_VALUE)+"l;print_err(m);\n" +
//						"long ltimesm;ltimesm=l*m;print_err(\"times long:\"+ltimesm);\n" +
//						"long mtimesl;mtimesl=m*l;print_err(\"times long:\"+mtimesl);\n" +
//						"long mtimesi;mtimesi=m*i;print_err(\"long times int:\"+mtimesi);\n" +
//						"long itimesm;itimesm=i*m;print_err(\"int times long:\"+itimesm);\n" +
//						"number n; n=0.1;print_err(n);\n" +
//						"number m1; m1=0.001;print_err(m1);\n" +
//						"number ntimesm1; ntimesm1=n*m1;print_err(\"times number:\"+ntimesm1);\n" +
//						"number ntimesj;ntimesj=n*j;print_err(\"number times int:\"+ntimesj);\n"+
//						"number jtimesn;jtimesn=j*n;print_err(\"int times number:\"+jtimesn);\n"+
//						"number m1timesm;m1timesm=m1*m;print_err(\"number times long:\"+m1timesm);\n"+
//						"number mtimesm1;mtimesm1=m*m1;print_err(\"long times number:\"+mtimesm1);\n"+
//						"decimal d; d=0.1D;print_err(d);\n" +
//						"decimal d1; d1=0.0001D;print_err(d1);\n" +
//						"decimal dtimesd1; dtimesd1=d*d1;print_err(\"times decimal:\"+dtimesd1);\n" +
//						"decimal dtimesj;dtimesj=d*j;print_err(\"decimal times int:\"+dtimesj);\n" +
//						"decimal jtimesd;jtimesd=j*d;print_err(\"int times decimal:\"+jtimesd);\n" +
//						"decimal dtimesm;dtimesm=d*m;print_err(\"decimal times long:\"+dtimesm);\n" +
//						"decimal mtimesd;mtimesd=m*d;print_err(\"long times decimal:\"+mtimesd);\n" +
//						"decimal dtimesn;dtimesn=d*n;print_err(\"decimal times number:\"+dtimesn);\n" +
//						"decimal ntimesd;ntimesd=n*d;print_err(\"number times decimal:\"+ntimesd);\n";
//						
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		
//		assertEquals("itimesj",10*100,(executor.getVariableValue("itimesj")));
//		assertEquals("ltimesm",Long.valueOf(Integer.MAX_VALUE)*(Long.valueOf(Integer.MAX_VALUE/10)),executor.getVariableValue("ltimesm"));
//		assertEquals("mtimesl",executor.getVariableValue("mtimesl"), executor.getVariableValue("ltimesm"));
//		assertEquals("ltimesi",Long.valueOf(Integer.MAX_VALUE)*Long.valueOf(Integer.MAX_VALUE/10),executor.getVariableValue("ltimesm"));
//		assertEquals("mtimesi",Long.valueOf(Integer.MAX_VALUE)*10,executor.getVariableValue("mtimesi"));
//		assertEquals("itimesm",executor.getVariableValue("itimesm"),executor.getVariableValue("mtimesi"));
//		assertEquals("ntimesm1",Double.valueOf(0.1D*0.001D),executor.getVariableValue("ntimesm1"));
//		assertEquals("ntimesj",Double.valueOf(0.1)*100,executor.getVariableValue("ntimesj"));
//		assertEquals("jtimesn",executor.getVariableValue("jtimesn"),executor.getVariableValue("ntimesj"));
//		assertEquals("m1timesm",Double.valueOf(0.001d * Long.valueOf(Integer.MAX_VALUE)),executor.getVariableValue("m1timesm"));
//		assertEquals("mtimesm1",executor.getVariableValue("mtimesm1"),executor.getVariableValue("m1timesm"));
//		assertEquals("dtimesd1",new BigDecimal("0.1",MAX_PRECISION).multiply(new BigDecimal("0.0001",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dtimesd1"));
//		assertEquals("dtimesj",new BigDecimal("0.1",MAX_PRECISION).multiply(new BigDecimal(100,MAX_PRECISION)),executor.getVariableValue("dtimesj"));
//		assertEquals("jtimesd",executor.getVariableValue("jtimesd"),executor.getVariableValue("dtimesj"));
//		assertEquals("dtimesm",new BigDecimal("0.1",MAX_PRECISION).multiply(new BigDecimal(Long.valueOf(Integer.MAX_VALUE),MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dtimesm"));
//		assertEquals("mtimesd", executor.getVariableValue("mtimesd"), executor.getVariableValue("dtimesm"));
//		assertEquals("dtimesn",new BigDecimal("0.1",MAX_PRECISION).multiply(new BigDecimal(0.1,MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dtimesn"));
//		assertEquals("ntimesd",executor.getVariableValue("ntimesd"),executor.getVariableValue("dtimesn"));
//	}
//
//	
//	public void test_divide(){
//		System.out.println("\nminus test:");
//		String expStr = "int i; i=10;\n"+
//						"int j; j=100;\n" +
//						"int idividej;idividej=i/j; print_err(\"divide int:\"+idividej);\n" +
//						"long l;l="+Integer.MAX_VALUE/10+"l;print_err(l);\n" +
//						"long m;m="+(Integer.MAX_VALUE)+"l;print_err(m);\n" +
//						"long ldividem;ldividem=l/m;print_err(\"divide long:\"+ldividem);\n" +
//						"long mdividei;mdividei=m/i;print_err(\"long divide int:\"+mdividei);\n" +
//						"long idividem;idividem=i/m;print_err(\"int divide long:\"+idividem);\n" +
//						"number n; n=0.1;print_err(n);\n" +
//						"number m1; m1=0.001;print_err(m1);\n" +
//						"number ndividem1; ndividem1=n/m1;print_err(\"divide number:\"+ndividem1);\n" +
//						"number ndividej;ndividej=n/j;print_err(\"number divide int:\"+ndividej);\n"+
//						"number jdividen;jdividen=j/n;print_err(\"int divide number:\"+jdividen);\n"+
//						"number m1dividem;m1dividem=m1/m;print_err(\"number divide long:\"+m1dividem);\n"+
//						"number mdividem1;mdividem1=m/m1;print_err(\"long divide number:\"+mdividem1);\n"+
//						"decimal d; d=0.1D;print_err(d);\n" +
//						"decimal d1; d1=0.0001D;print_err(d1);\n" +
//						"decimal ddivided1; ddivided1=d/d1;print_err(\"divide decimal:\"+ddivided1);\n" +
//						"decimal ddividej;ddividej=d/j;print_err(\"decimal divide int:\"+ddividej);\n" +
//						"decimal jdivided;jdivided=j/d;print_err(\"int divide decimal:\"+jdivided);\n" +
//						"decimal ddividem;ddividem=d/m;print_err(\"decimal divide long:\"+ddividem);\n" +
//						"decimal mdivided;mdivided=m/d;print_err(\"long divide decimal:\"+mdivided);\n" +
//						"decimal ddividen;ddividen=d/n;print_err(\"decimal divide number:\"+ddividen);\n" +
//						"decimal ndivided;ndivided=n/d;print_err(\"number divide decimal:\"+ndivided);\n";
//						
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		
//		assertEquals("idividej",10/100,(executor.getVariableValue("idividej")));
//		assertEquals("ldividem",Long.valueOf(Integer.MAX_VALUE/10)/Long.valueOf(Integer.MAX_VALUE),executor.getVariableValue("ldividem"));
//		assertEquals("mdividei",Long.valueOf(Integer.MAX_VALUE/10),executor.getVariableValue("mdividei"));
//		assertEquals("idividem",10/Long.valueOf(Integer.MAX_VALUE),executor.getVariableValue("idividem"));
//		assertEquals("ndividem1",Double.valueOf(0.1D / 0.001D),executor.getVariableValue("ndividem1"));
//		assertEquals("ndividej",Double.valueOf(0.1D / 100),executor.getVariableValue("ndividej"));
//		assertEquals("jdividen",Double.valueOf(100 / 0.1D),executor.getVariableValue("jdividen"));
//		assertEquals("m1dividem",Double.valueOf(0.001D / Long.valueOf(Integer.MAX_VALUE)),executor.getVariableValue("m1dividem"));
//		assertEquals("mdividem1",Double.valueOf(Long.valueOf(Integer.MAX_VALUE) / 0.001D),executor.getVariableValue("mdividem1"));
//		assertEquals("ddivided1",new BigDecimal("0.1",MAX_PRECISION).divide(new BigDecimal("0.0001",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("ddivided1"));
//		assertEquals("ddividej",new BigDecimal("0.1",MAX_PRECISION).divide(new BigDecimal(100,MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("ddividej"));
//		assertEquals("jdivided",new BigDecimal(100,MAX_PRECISION).divide(new BigDecimal("0.1",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("jdivided"));
//		assertEquals("ddividem",new BigDecimal("0.1",MAX_PRECISION).divide(new BigDecimal(Long.valueOf(Integer.MAX_VALUE),MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("ddividem"));
//		assertEquals("mdivided", new BigDecimal(Long.valueOf(Integer.MAX_VALUE),MAX_PRECISION).divide(new BigDecimal("0.1",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("mdivided"));
//		assertEquals("ddividen",new BigDecimal("0.1",MAX_PRECISION).divide(new BigDecimal(0.1D,MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("ddividen"));
//		assertEquals("ndivided",new BigDecimal(0.1D,MAX_PRECISION).divide(new BigDecimal("0.1",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("ndivided"));
//	}
//	
//	
//	public void test_modulus(){
//		System.out.println("\nmodulus test:");
//		String expStr = "int i; i=10;\n"+
//						"int j; j=100;\n" +
//						"int imoduloj;imoduloj=i%j; print_err(\"modulo int:\"+imoduloj);\n" +
//						"long l;l="+Integer.MAX_VALUE/10+"l;print_err(l);\n" +
//						"long m;m="+(Integer.MAX_VALUE)+"l;print_err(m);\n" +
//						"long lmodulom;lmodulom=l%m;print_err(\"modulo long:\"+lmodulom);\n" +
//						"long mmoduloi;mmoduloi=m%i;print_err(\"long modulo int:\"+mmoduloi);\n" +
//						"long imodulom;imodulom=i%m;print_err(\"int modulo long:\"+imodulom);\n" +
//						"number n; n=0.1;print_err(n);\n" +
//						"number m1; m1=0.001;print_err(m1);\n" +
//						"number nmodulom1; nmodulom1=n%m1;print_err(\"modulo number:\"+nmodulom1);\n" +
//						"number nmoduloj;nmoduloj=n%j;print_err(\"number modulo int:\"+nmoduloj);\n"+
//						"number jmodulon;jmodulon=j%n;print_err(\"int modulo number:\"+jmodulon);\n"+
//						"number m1modulom;m1modulom=m1%m;print_err(\"number modulo long:\"+m1modulom);\n"+
//						"number mmodulom1;mmodulom1=m%m1;print_err(\"long modulo number:\"+mmodulom1);\n"+
//						"decimal d; d=0.1D;print_err(d);\n" +
//						"decimal d1; d1=0.0001D;print_err(d1);\n" +
//						"decimal dmodulod1; dmodulod1=d%d1;print_err(\"modulo decimal:\"+dmodulod1);\n" +
//						"decimal dmoduloj;dmoduloj=d%j;print_err(\"decimal modulo int:\"+dmoduloj);\n" +
//						"decimal jmodulod;jmodulod=j%d;print_err(\"int modulo decimal:\"+jmodulod);\n" +
//						"decimal dmodulom;dmodulom=d%m;print_err(\"decimal modulo long:\"+dmodulom);\n" +
//						"decimal mmodulod;mmodulod=m%d;print_err(\"long modulo decimal:\"+mmodulod);\n" +
//						"decimal dmodulon;dmodulon=d%n;print_err(\"decimal modulo number:\"+dmodulon);\n" +
//						"decimal nmodulod;nmodulod=n%d;print_err(\"number modulo decimal:\"+nmodulod);\n";
//						
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		
//		assertEquals("imoduloj",10%100,(executor.getVariableValue("imoduloj")));
//		assertEquals("lmodulom",Long.valueOf(Integer.MAX_VALUE/10)%Long.valueOf(Integer.MAX_VALUE),executor.getVariableValue("lmodulom"));
//		assertEquals("mmoduloi",Long.valueOf(Integer.MAX_VALUE%10),executor.getVariableValue("mmoduloi"));
//		assertEquals("imodulom",10%Long.valueOf(Integer.MAX_VALUE),executor.getVariableValue("imodulom"));
//		assertEquals("nmodulom1",Double.valueOf(0.1D % 0.001D),executor.getVariableValue("nmodulom1"));
//		assertEquals("nmoduloj",Double.valueOf(0.1D % 100),executor.getVariableValue("nmoduloj"));
//		assertEquals("jmodulon",Double.valueOf(100 % 0.1D),executor.getVariableValue("jmodulon"));
//		assertEquals("m1modulom",Double.valueOf(0.001D % Long.valueOf(Integer.MAX_VALUE)),executor.getVariableValue("m1modulom"));
//		assertEquals("mmodulom1",Double.valueOf(Long.valueOf(Integer.MAX_VALUE) % 0.001D),executor.getVariableValue("mmodulom1"));
//		assertEquals("dmodulod1",new BigDecimal("0.1",MAX_PRECISION).remainder(new BigDecimal("0.0001",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dmodulod1"));
//		assertEquals("dmoduloj",new BigDecimal("0.1",MAX_PRECISION).remainder(new BigDecimal(100,MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dmoduloj"));
//		assertEquals("jmodulod",new BigDecimal(100,MAX_PRECISION).remainder(new BigDecimal("0.1",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("jmodulod"));
//		assertEquals("dmodulom",new BigDecimal("0.1",MAX_PRECISION).remainder(new BigDecimal(Long.valueOf(Integer.MAX_VALUE),MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dmodulom"));
//		assertEquals("mmodulod", new BigDecimal(Long.valueOf(Integer.MAX_VALUE),MAX_PRECISION).remainder(new BigDecimal("0.1",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("mmodulod"));
//		assertEquals("dmodulon",new BigDecimal("0.1",MAX_PRECISION).remainder(new BigDecimal(0.1D,MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("dmodulon"));
//		assertEquals("nmodulod",new BigDecimal(0.1D,MAX_PRECISION).remainder(new BigDecimal("0.1",MAX_PRECISION),MAX_PRECISION),executor.getVariableValue("nmodulod"));
//	}
//	
//	public void test_unary_operators(){
//		System.out.println("\nunary operators test:");
//		String expStr = "print_err('Postfix operators: ');\n"+
//						// postfix operators:
//						// int
//						"int intPlus = 10;\n"+
//						"int intPlusOrig = intPlus;\n" +
//						"int intPlusPlus = intPlus++;\n" +
//						"print_err('int++ orig: ' + intPlusOrig + ', incremented: ' +  intPlus + ', opresult: ' + intPlusPlus);\n" +
//						"int intMinus = 10;\n" +
//						"int intMinusOrig = intMinus;\n" +
//						"int intMinusMinus = intMinus--;\n" +
//						"print_err('int-- orig: ' + intMinusOrig + ', decremented: ' +  intMinus + ', opresult: ' + intMinusMinus);\n" +
//						// long
//						"long longPlus = 10;\n"+
//						"long longPlusOrig = longPlus;\n" +
//						"long longPlusPlus = longPlus++;\n" +
//						"print_err('long++ orig: ' + longPlusOrig + ', incremented: ' +  longPlus + ', opresult: ' + longPlusPlus);\n" +
//						"long longMinus = 10;\n" +
//						"long longMinusOrig = longMinus;\n" +
//						"long longMinusMinus = longMinus--;\n" +
//						"print_err('long-- orig: ' + longMinusOrig + ', decremented: ' +  longMinus + ', opresult: ' + longMinusMinus);\n" +
//						//number
//						"number numberPlus = 10.1;\n"+
//						"number numberPlusOrig = numberPlus;\n" +
//						"number numberPlusPlus = numberPlus++;\n" +
//						"print_err('number++ orig: ' + numberPlusOrig + ', incremented: ' +  numberPlus + ', opresult: ' + numberPlusPlus);\n" +
//						"number numberMinus = 10.1;\n" +
//						"number numberMinusOrig = numberMinus;\n" +
//						"number numberMinusMinus = numberMinus--;\n" +
//						"print_err('number-- orig: ' + numberMinusOrig + ', decremented: ' +  numberMinus + ', opresult: ' + numberMinusMinus);\n" +
//						// decimal
//						"decimal decimalPlus = 10.1D;\n"+
//						"decimal decimalPlusOrig = decimalPlus;\n" +
//						"decimal decimalPlusPlus = decimalPlus++;\n" +
//						"print_err('decimal++ orig: ' + decimalPlusOrig + ', incremented: ' +  decimalPlus + ', opresult: ' + decimalPlusPlus);\n" +
//						"decimal decimalMinus = 10.1D;\n" +
//						"decimal decimalMinusOrig = decimalMinus;\n" +
//						"decimal decimalMinusMinus = decimalMinus--;\n" +
//						"print_err('decimal-- orig: ' + decimalMinusOrig + ', decremented: ' +  decimalMinus + ', opresult: ' + decimalMinusMinus);\n" +
//						
//						// prefix operators
//						// int 
//						"print_err('Prefix operators: ');\n" + 
//						"int plusInt = 10;\n"+
//						"int plusIntOrig = plusInt;\n" +
//						"int plusPlusInt = ++plusInt;\n" +
//						"print_err('++int orig: ' + plusIntOrig + ', incremented: ' +  plusInt + ', opresult: ' + plusPlusInt);\n" +
//						"int minusInt = 10;\n" +
//						"int unaryInt = -(minusInt);\n" + 
//						"int minusIntOrig = minusInt;\n" +
//						"int minusMinusInt = --minusInt;\n" +
//						"print_err('--int orig: ' + minusIntOrig + ', decremented: ' +  minusInt + ', opresult: ' + minusMinusInt + ', unary: ' + unaryInt);\n" +
//						// long
//						"long plusLong = 10;\n"+
//						"long plusLongOrig = plusLong;\n" +
//						"long plusPlusLong = ++plusLong;\n" +
//						"print_err('++long orig: ' + plusLongOrig + ', incremented: ' +  plusLong + ', opresult: ' + plusPlusLong);\n" +
//						"long minusLong = 10;\n" +
//						"long unaryLong = -(minusLong);\n" + 
//						"long minusLongOrig = minusLong;\n" +
//						"long minusMinusLong = --minusLong;\n" +
//						"print_err('--long orig: ' + minusLongOrig + ', decremented: ' +  minusLong + ', opresult: ' + minusMinusLong + ', unary: ' + unaryLong);\n" +
//						// double
//						"number plusNumber = 10.1;\n"+
//						"number plusNumberOrig = plusNumber;\n" +
//						"number plusPlusNumber = ++plusNumber;\n" +
//						"print_err('++number orig: ' + plusNumberOrig + ', incremented: ' +  plusNumber + ', opresult: ' + plusPlusNumber);\n" +
//						"number minusNumber = 10.1;\n" +
//						"number unaryNumber = -(minusNumber);\n" + 
//						"number minusNumberOrig = minusNumber;\n" +
//						"number minusMinusNumber = --minusNumber;\n" +
//						"print_err('--number orig: ' + minusNumberOrig + ', decremented: ' +  minusNumber + ', opresult: ' + minusMinusNumber + ', unary: ' + unaryNumber);\n" +
//						// decimal
//						"decimal plusDecimal = 10.1D;\n"+
//						"decimal plusDecimalOrig = plusDecimal;\n" +
//						"decimal plusPlusDecimal = ++plusDecimal;\n" +
//						"print_err('++decimal orig: ' + plusDecimalOrig + ', incremented: ' +  plusDecimal + ', opresult: ' + plusPlusDecimal);\n" +
//						"decimal minusDecimal = 10.1D;\n" +
//						"decimal unaryDecimal = -(minusDecimal);\n" + 
//						"decimal minusDecimalOrig = minusDecimal;\n" +
//						"decimal minusMinusDecimal = --minusDecimal;\n" +
//						"print_err('--decimal orig: ' + minusDecimalOrig + ', decremented: ' +  minusDecimal + ', opresult: ' + minusMinusDecimal + ', unary: ' + unaryDecimal);\n" +
//						// logical negation
//						"boolean booleanValue = true;\n" +
//						"boolean negation = !booleanValue;\n" +
//						"boolean doubleNegation = !negation;\n" +
//						"print_err('!boolean orig: ' + booleanValue + ' ,not: ' + negation + ' ,double not: ' + doubleNegation);\n";
//						
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		// postfix operators
//		// int
//		assertEquals("intPlusOrig",Integer.valueOf(10),executor.getVariableValue("intPlusOrig"));
//		assertEquals("intPlusPlus",Integer.valueOf(10),executor.getVariableValue("intPlusPlus"));
//		assertEquals("intPlus",Integer.valueOf(11),executor.getVariableValue("intPlus"));
//		assertEquals("intMinusOrig",Integer.valueOf(10),executor.getVariableValue("intMinusOrig"));
//		assertEquals("intMinusMinus",Integer.valueOf(10),executor.getVariableValue("intMinusMinus"));
//		assertEquals("intMinus",Integer.valueOf(9),executor.getVariableValue("intMinus"));
//		// long
//		assertEquals("longPlusOrig",Long.valueOf(10),executor.getVariableValue("longPlusOrig"));
//		assertEquals("longPlusPlus",Long.valueOf(10),executor.getVariableValue("longPlusPlus"));
//		assertEquals("longPlus",Long.valueOf(11),executor.getVariableValue("longPlus"));
//		assertEquals("longMinusOrig",Long.valueOf(10),executor.getVariableValue("longMinusOrig"));
//		assertEquals("longMinusMinus",Long.valueOf(10),executor.getVariableValue("longMinusMinus"));
//		assertEquals("longMinus",Long.valueOf(9),executor.getVariableValue("longMinus"));
//		// double
//		assertEquals("numberPlusOrig",Double.valueOf(10.1),executor.getVariableValue("numberPlusOrig"));
//		assertEquals("numberPlusPlus",Double.valueOf(10.1),executor.getVariableValue("numberPlusPlus"));
//		assertEquals("numberPlus",Double.valueOf(11.1),executor.getVariableValue("numberPlus"));
//		assertEquals("numberMinusOrig",Double.valueOf(10.1),executor.getVariableValue("numberMinusOrig"));
//		assertEquals("numberMinusMinus",Double.valueOf(10.1),executor.getVariableValue("numberMinusMinus"));
//		assertEquals("numberMinus",Double.valueOf(9.1),executor.getVariableValue("numberMinus"));
//		// decimal
//		assertEquals("decimalPlusOrig",new BigDecimal("10.1"),executor.getVariableValue("decimalPlusOrig"));
//		assertEquals("decimalPlusPlus",new BigDecimal("10.1"),executor.getVariableValue("decimalPlusPlus"));
//		assertEquals("decimalPlus",new BigDecimal("11.1"),executor.getVariableValue("decimalPlus"));
//		assertEquals("decimalMinusOrig",new BigDecimal("10.1"),executor.getVariableValue("decimalMinusOrig"));
//		assertEquals("decimalMinusMinus",new BigDecimal("10.1"),executor.getVariableValue("decimalMinusMinus"));
//		assertEquals("decimalMinus",new BigDecimal("9.1"),executor.getVariableValue("decimalMinus"));
//		// prefix operators
//		// int
//		assertEquals("plusIntOrig",Integer.valueOf(10),executor.getVariableValue("plusIntOrig"));
//		assertEquals("plusPlusInt",Integer.valueOf(11),executor.getVariableValue("plusPlusInt"));
//		assertEquals("plusInt",Integer.valueOf(11),executor.getVariableValue("plusInt"));
//		assertEquals("minusIntOrig",Integer.valueOf(10),executor.getVariableValue("minusIntOrig"));
//		assertEquals("minusMinusInt",Integer.valueOf(9),executor.getVariableValue("minusMinusInt"));
//		assertEquals("minusInt",Integer.valueOf(9),executor.getVariableValue("minusInt"));
//		assertEquals("unaryInt",Integer.valueOf(-10),executor.getVariableValue("unaryInt"));
//		// long
//		assertEquals("plusLongOrig",Long.valueOf(10),executor.getVariableValue("plusLongOrig"));
//		assertEquals("plusPlusLong",Long.valueOf(11),executor.getVariableValue("plusPlusLong"));
//		assertEquals("plusLong",Long.valueOf(11),executor.getVariableValue("plusLong"));
//		assertEquals("minusLongOrig",Long.valueOf(10),executor.getVariableValue("minusLongOrig"));
//		assertEquals("minusMinusLong",Long.valueOf(9),executor.getVariableValue("minusMinusLong"));
//		assertEquals("minusLong",Long.valueOf(9),executor.getVariableValue("minusLong"));
//		assertEquals("unaryLong",Long.valueOf(-10),executor.getVariableValue("unaryLong"));
//		// double
//		assertEquals("plusNumberOrig",Double.valueOf(10.1),executor.getVariableValue("plusNumberOrig"));
//		assertEquals("plusPlusNumber",Double.valueOf(11.1),executor.getVariableValue("plusPlusNumber"));
//		assertEquals("plusNumber",Double.valueOf(11.1),executor.getVariableValue("plusNumber"));
//		assertEquals("minusNumberOrig",Double.valueOf(10.1),executor.getVariableValue("minusNumberOrig"));
//		assertEquals("minusMinusNumber",Double.valueOf(9.1),executor.getVariableValue("minusMinusNumber"));
//		assertEquals("minusNumber",Double.valueOf(9.1),executor.getVariableValue("minusNumber"));
//		assertEquals("unaryNumber",Double.valueOf(-10.1),executor.getVariableValue("unaryNumber"));
//		// decimal
//		assertEquals("plusDecimalOrig",new BigDecimal("10.1"),executor.getVariableValue("plusDecimalOrig"));
//		assertEquals("plusPlusDecimal",new BigDecimal("11.1"),executor.getVariableValue("plusPlusDecimal"));
//		assertEquals("plusDecimal",new BigDecimal("11.1"),executor.getVariableValue("plusDecimal"));
//		assertEquals("minusDecimalOrig",new BigDecimal("10.1"),executor.getVariableValue("minusDecimalOrig"));
//		assertEquals("minusMinusDecimal",new BigDecimal("9.1"),executor.getVariableValue("minusMinusDecimal"));
//		assertEquals("minusDecimal",new BigDecimal("9.1"),executor.getVariableValue("minusDecimal"));
//		assertEquals("unaryDecimal",new BigDecimal("-10.1"),executor.getVariableValue("unaryDecimal"));
//		// logical not
//		assertEquals("booleanValue",true,executor.getVariableValue("booleanValue"));
//		assertEquals("negation",false,executor.getVariableValue("negation"));
//		assertEquals("doubleNegation",true,executor.getVariableValue("doubleNegation"));
//		
//	}
//	
//	public void test_in_operator() {
//		String expStr = 
//			"int a = 1;\n" +
//			"int[] haystack = [a,a+1,a+1+1];\n" +
//			"int needle = 2;\n" +
//			"boolean b1 = needle.in(haystack);\n" +
//			"print_err(needle + ' in ' + haystack + ': ' + b1);\n" +
//			"haystack.remove_all();\n" +
//			"boolean b2 = in(needle,haystack);\n" +
//			"print_err(needle + ' in ' + haystack + ': ' + b2);\n" +
//			"double[] h2 = [ 2.1, 2.0, 2.2];\n" +
//			"boolean b3 = needle.in(h2);\n" +
//			"print_err(needle + ' in ' + h2 + ': ' + b3);\n" +
//			"string[] h3 = [ 'memento', 'mori', 'memento ' + 'mori'];\n" +
//			"string n3 = 'memento ' + 'mori';\n" + 
//			"boolean b4 = n3.in(h3);\n" +
//			"print_err(n3 + ' in ' + h3 + ': ' + b4);\n";
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		
//		ITLCompiler compiler = TLCompilerFactory.createCompiler(graph, inMetadata, outMetadata, "UTF-8");
//		List<ErrorMessage> messages = compiler.compile(expStr,DataRecordTransform.class, "test_int");
//		printMessages(messages);
//		if (compiler.errorCount() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		assertEquals("a",Integer.valueOf(1),executor.getVariableValue("a"));
//		assertEquals("haystack",Collections.EMPTY_LIST,executor.getVariableValue("haystack"));
//		assertEquals("needle",Integer.valueOf(2),executor.getVariableValue("needle"));
//		assertEquals(true,executor.getVariableValue("b1"));
//		assertEquals(false,executor.getVariableValue("b2"));
//		assertEquals("h2",createList(2.1D,2.0D,2.2D),executor.getVariableValue("h2"));
//		assertEquals(true,executor.getVariableValue("b3"));
//		assertEquals("h3",createList("memento","mori","memento mori"),executor.getVariableValue("h3"));
//		assertEquals("n3", "memento mori", executor.getVariableValue("n3"));
//		assertEquals(true, executor.getVariableValue("b4"));
//	}
//	
//	
//	public void test_equal(){
//		System.out.println("\nequal test:");
//		String expStr = "int i=10;print_err('i='+i);\n" +
//						"int j=9;print_err('j='+j);\n" +
//						"boolean eq0=(i.eq.(j+1));print_err('eq0: i==j+1 '+eq0);\n" +
//						"boolean eq1=(i==j+1);print_err('eq1: i==j+1 '+eq1);\n" +
//						"long l=10;print_err('l='+l);\n" +
//						"boolean eq2=(l==j);print_err('eq2: l==j '+eq2);\n" +
//						"boolean eq2i=(l.eq.i);print_err('eq2i: l==i ' + eq2i);\n" +
//						"decimal d=10;print_err('d='+d);\n" +
//						"boolean eq3=d==i;print_err('eq3: d==i '+eq3);\n" +
//						"number n;n=10;print_err('n='+n);\n" +
//						"boolean eq4=n.eq.l;print_err('eq4 n==l '+eq4);\n" +
//						"boolean eq5=n==d;print_err('eq5: n==d '+eq5);\n" +
//						"string s='hello';print_err('s='+s);\n" +
//						"string s1='hello ';print_err('s1='+s1);\n" +
//						"boolean eq6=s.eq.s1;print_err('eq6 s==s1: '+eq6);\n" +
//						"boolean eq7=s==trim(s1);print_err('eq7 s==trim(s1)'+eq7);\n" +
//						"date mydate=2006-01-01;print_err('mydate='+mydate);\n" +
//						"date anothermydate;print_err('anothermydate='+anothermydate);\n" +
//						"boolean eq8=mydate.eq.anothermydate;print_err('eq8: mydate == anothermydate '+eq8);\n" +
//						"anothermydate=2006-1-1 0:0:0;print_err('anothermydate='+anothermydate);\n" +
//						"boolean eq9=mydate==anothermydate;print_err('eq9: mydate == anothermydate ='+eq9);\n" +
//						"boolean eq10=eq9.eq.eq8;print_err('eq10: eq9 == eq8 '+eq10);\n";
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		      
//		assertEquals(true,executor.getVariableValue("eq0"));
//		assertEquals(true,executor.getVariableValue("eq1"));
//		assertEquals(false,executor.getVariableValue("eq2"));
//		assertEquals(true,executor.getVariableValue("eq2i"));
//		assertEquals(true,executor.getVariableValue("eq3"));
//		assertEquals(true,executor.getVariableValue("eq4"));
//		assertEquals(true,executor.getVariableValue("eq5"));
//		assertEquals(false,executor.getVariableValue("eq6"));
//		assertEquals(true,executor.getVariableValue("eq7"));
//		assertEquals(false,executor.getVariableValue("eq8"));
//		assertEquals(true,executor.getVariableValue("eq9"));
//		assertEquals(false,executor.getVariableValue("eq10"));
//
//	}
//	
//	public void test_non_equal(){
//		System.out.println("\nNon equal test:");
//		String expStr = "int i=10;print_err(\"i=\"+i);\n" +
//						"int j=9;print_err(\"j=\"+j);\n" +
//						"boolean inei=(i!=i);print_err(\"inei=\" + inei);\n" +
//						"boolean inej=(i!=j);print_err(\"inej=\" + inej);\n" +
//						"boolean jnei=(j!=i);print_err(\"jnei=\" + jnei);\n" +
//						"boolean jnej=(j!=j);print_err(\"jnej=\" + jnej);\n" +
//						"long l;l=10;print_err(\"l=\"+l);\n" +
//						"boolean lnei=(l<>i);print_err(\"lnei=\" + lnei);\n" +
//						"boolean inel=(i<>l);print_err(\"inel=\" + inel);\n" +
//						"boolean lnej=(l<>j);print_err(\"lnej=\" + lnej);\n" +
//						"boolean jnel=(j<>l);print_err(\"jnel=\" + jnel);\n" +
//						"boolean lnel=(l<>l);print_err(\"lnel=\" + lnel);\n" +
//						"decimal d=10;print_err(\"d=\"+d);\n" +
//						"boolean dnei=d.ne.i;print_err(\"dnei=\" + dnei);\n" +
//						"boolean ined=i.ne.d;print_err(\"ined=\" + ined);\n" +
//						"boolean dnej=d.ne.j;print_err(\"dnej=\" + dnej);\n" +
//						"boolean jned=j.ne.d;print_err(\"jned=\" + jned);\n" +
//						"boolean dnel=d.ne.l;print_err(\"dnel=\" + dnel);\n" +
//						"boolean lned=l.ne.d;print_err(\"lned=\" + lned);\n" +
//						"boolean dned=d.ne.d;print_err(\"dned=\" + dned);\n";
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		assertEquals(false,executor.getVariableValue("inei"));
//		assertEquals(true,executor.getVariableValue("inej"));
//		assertEquals(true,executor.getVariableValue("jnei"));
//		assertEquals(false,executor.getVariableValue("jnej"));
//		assertEquals(false,executor.getVariableValue("lnei"));
//		assertEquals(false,executor.getVariableValue("inel"));
//		assertEquals(true,executor.getVariableValue("lnej"));
//		assertEquals(true,executor.getVariableValue("jnel"));
//		assertEquals(false,executor.getVariableValue("lnel"));
//		assertEquals(false,executor.getVariableValue("dnei"));
//		assertEquals(false,executor.getVariableValue("ined"));
//		assertEquals(true,executor.getVariableValue("dnej"));
//		assertEquals(true,executor.getVariableValue("jned"));
//		assertEquals(false,executor.getVariableValue("dnel"));
//		assertEquals(false,executor.getVariableValue("lned"));
//		assertEquals(false,executor.getVariableValue("dned"));
//	}
//	
//	
//	public void test_greater_less(){
//		System.out.println("\nGreater and less test:");
//		String expStr = "int i=10;print_err(\"i=\"+i);\n" +
//						"int j=9;print_err(\"j=\"+j);\n" +
//						"boolean eq1=(i>j);print_err(\"eq1=\"+eq1);\n" +
//						"long l=10;print_err(\"l=\"+l);\n" +
//						"boolean eq2=(l>=j);print_err(\"eq2=\"+eq2);\n" +
//						"decimal d=10;print_err(\"d=\"+d);\n" +
//						"boolean eq3=d=>i;print_err(\"eq3=\"+eq3);\n" +
//						"number n=10;print_err(\"n=\"+n);\n" +
//						"boolean eq4=n.gt.l;print_err(\"eq4=\"+eq4);\n" +
//						"boolean eq5=n.ge.d;print_err(\"eq5=\"+eq5);\n" +
//						"string s='hello';print_err(\"s=\"+s);\n" +
//						"string s1=\"hello\";print_err(\"s1=\"+s1);\n" +
//						"boolean eq6=s<s1;print_err(\"eq6=\"+eq6);\n" +
//						"date mydate=2006-01-01;print_err(\"mydate=\"+mydate);\n" +
//						"date anothermydate=2008-03-05;print_err(\"anothermydate=\"+anothermydate);\n" +
//						"date mydateandtime=2006-01-01 15:30:00; print_err(\"mydateandtime=\"+mydateandtime);\n" +
//						"boolean eq7 = mydate < mydateandtime;\n" + 
//						"boolean eq8=mydate .lt. anothermydate;print_err(\"eq8=\"+eq8);\n" +
//						"anothermydate=2006-1-1 0:0:0;print_err(\"anothermydate=\"+anothermydate);\n" +
//						"boolean eq9=mydate<=anothermydate;print_err(\"eq9=\"+eq9);\n" ;
//
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		assertEquals("eq1",true,executor.getVariableValue("eq1"));
//		assertEquals("eq2",true,executor.getVariableValue("eq2"));
//		assertEquals("eq3",true,executor.getVariableValue("eq3"));
//		assertEquals("eq4",false,executor.getVariableValue("eq4"));
//		assertEquals("eq5",true,executor.getVariableValue("eq5"));
//		assertEquals("eq6",false,executor.getVariableValue("eq6"));
//		assertEquals("eq7",true,executor.getVariableValue("eq7"));
//		assertEquals("eq8",true,executor.getVariableValue("eq8"));
//		assertEquals("eq9",true,executor.getVariableValue("eq9"));
//
//	}
//	
//	public void test_ternary_operator(){
//		System.out.println("\nTernary operator test:");
//		String expStr = "boolean trueValue = true; boolean falseValue = false;\n" +
//						"int res1 = trueValue ? 1 : 2;\n" +
//						"int res2 = falseValue ? 1 : 2;\n" +
//						// nesting in true-branch
//						"int res3 = trueValue ?  trueValue ? 1 : 2 : 3;\n" +
//						"int res4 = trueValue ?  falseValue ? 1 : 2 : 3;\n" + 
//						"int res5 = falseValue ?  trueValue ? 1 : 2 : 3;\n" + 
//						// nesting in false-branch
//						"int res6 = falseValue ?  1 : trueValue ? 2 : 3;\n" + 
//						"int res7 = falseValue ?  1 : falseValue ? 2 : 3;\n" +
//						// nesting in both branches
//						"int res8 = trueValue ?  trueValue ? 1 : 2 : trueValue ? 3 : 4;\n" +
//						"int res9 = trueValue ?  trueValue ? 1 : 2 : falseValue ? 3 : 4;\n" +
//						"int res10 = trueValue ?  falseValue ? 1 : 2 : trueValue ? 3 : 4;\n" +
//						"int res11 = falseValue ?  trueValue ? 1 : 2 : trueValue ? 3 : 4;\n" +
//						"int res12 = trueValue ?  falseValue ? 1 : 2 : falseValue ? 3 : 4;\n" +
//						"int res13 = falseValue ?  trueValue ? 1 : 2 : falseValue ? 3 : 4;\n" +
//						"int res14 = falseValue ?  falseValue ? 1 : 2 : trueValue ? 3 : 4;\n" +
//						"int res15 = falseValue ?  falseValue ? 1 : 2 : falseValue ? 3 : 4;\n";
//						
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		// simple use
//		assertEquals(true,executor.getVariableValue("trueValue"));
//		assertEquals(false,executor.getVariableValue("falseValue"));
//		assertEquals(Integer.valueOf(1),executor.getVariableValue("res1"));
//		assertEquals(Integer.valueOf(2),executor.getVariableValue("res2"));
//		// nesting in positive branch
//		assertEquals(Integer.valueOf(1),executor.getVariableValue("res3"));
//		assertEquals(Integer.valueOf(2),executor.getVariableValue("res4"));
//		assertEquals(Integer.valueOf(3),executor.getVariableValue("res5"));
//		// nesting in negative branch
//		assertEquals(Integer.valueOf(2),executor.getVariableValue("res6"));
//		assertEquals(Integer.valueOf(3),executor.getVariableValue("res7"));
//		// nesting in both branches
//		assertEquals(Integer.valueOf(1),executor.getVariableValue("res8"));
//		assertEquals(Integer.valueOf(1),executor.getVariableValue("res9"));
//		assertEquals(Integer.valueOf(2),executor.getVariableValue("res10"));
//		assertEquals(Integer.valueOf(3),executor.getVariableValue("res11"));
//		assertEquals(Integer.valueOf(2),executor.getVariableValue("res12"));
//		assertEquals(Integer.valueOf(4),executor.getVariableValue("res13"));
//		assertEquals(Integer.valueOf(3),executor.getVariableValue("res14"));
//		assertEquals(Integer.valueOf(4),executor.getVariableValue("res15"));
//
//	}
//	
//	
//	
//	public void test_regex(){
//		System.out.println("\nRegex test:");
//		String expStr = "string s;s='Hej';print_err(s);\n" +
//						"boolean eq0;eq0=(s~=\"[a-z]{3}\");\n" +
//						"print_err('eq0=' + eq0);\n" +
//						"boolean eq1;eq1=(s~=\"[A-Za-z]{3}\");\n" +
//						"print_err('eq1=' + eq1);\n" +
//						"string haystack='Needle in a haystack, Needle in a haystack';\n" +
//						"print_err(haystack);\n" +
//						"boolean eq2 = haystack ?= 'needle';\n" +
//						"print_err('eq2=' + eq2);\n" +
//						"boolean eq3 = haystack ?= 'Needle';\n" +
//						"print_err('eq3=' + eq3);\n" +
//						"boolean eq4 = haystack ~= 'Needle';\n" +
//						"print_err('eq4=' + eq4);\n" +
//						"boolean eq5 = haystack ~= 'Needle.*';\n" +
//						"print_err('eq5=' + eq5);\n";
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		
//		assertEquals(false,executor.getVariableValue("eq0"));
//		assertEquals(true,executor.getVariableValue("eq1"));
//		assertEquals(false,executor.getVariableValue("eq2"));
//		assertEquals(true,executor.getVariableValue("eq3"));
//		assertEquals(false,executor.getVariableValue("eq4"));
//		assertEquals(true,executor.getVariableValue("eq5"));
//		
//
//	}
//
//	public void test_if(){
//		System.out.println("\nIf statement test:");
//		String expStr = // if with single statement
//						"print_err('case1: simple if');\n" + 
//						"boolean cond1 = true;\n" +
//						"boolean res1 = false;\n" +
//						"if (cond1)\n" +
//						"	res1 = true;\n" +
//						// if with mutliple statements (block)
//						"boolean cond2 = true;\n" +
//						"boolean res21 = false;\n" + 
//						"boolean res22 = false;\n" +
//						"print_err('case2: if with block ');\n" + 
//						"if (cond2) {\n" +
//						"print_err('cond2: inside block');\n" + 
//						"	res21 = true;\n" + 
//						"	res22 = true;\n" +
//						"}\n" + 
//						// else with single statement
//						"boolean cond3 = false;\n" +
//						"boolean res31 = false;\n" +
//						"boolean res32 = false;\n" +
//						"print_err('case3: simple-if and simple-else');\n" + 
//						"if (cond3) \n" + 
//						"	res31 = true;\n" +
//						"else \n" + 
//						"	res32 = true;\n" + 
//						// else with multiple statements (block)
//						"print_err('case4: simple-if, block-else');\n" + 
//						"boolean cond4 = false;\n" +
//						"boolean res41 = false;\n" +
//						"boolean res42 = false;\n" +
//						"boolean res43 = false;\n" +
//						"if (cond4) \n" + 
//						"	res41 = true;\n" +
//						"else {\n" + 
//						"print_err('cond4: within else body');\n" + 
//						"	res42 = true;\n"  +
//						"	res43 = true;\n" +
//						"}\n" +
//						// if with block, else with block
//						"print_err('case5: block-if, block-else');\n" + 
//						"boolean cond5 = false;\n" +
//						"boolean res51 = false;\n" +
//						"boolean res52 = false;\n" +
//						"boolean res53 = false;\n" +
//						"boolean res54 = false;\n" +
//						"if (cond5) {\n" + 
//						"	res51 = true;\n" +
//						"	res52 = true;\n" +
//						"} else {\n" + 
//						"	print_err('cond5: within else body');\n" + 
//						"	res53 = true;\n"  +
//						"	res54 = true;\n" +
//						"}\n" +
//						// else-if with single statement
//						"print_err('case6: simple if, simple else-if');\n" + 
//						"boolean cond61 = false;\n" +
//						"boolean cond62 = true;\n" +
//						"boolean res61 = false;\n" +
//						"boolean res62 = false;\n" +
//						"if (cond61) \n" + 
//						"	res61 = true;\n" +
//						" else if (cond62)\n" + 
//						"	res62 = true;\n"  +
//						// else-if with multiple statements
//						"print_err('case7: simple if, block else-if');\n" + 
//						"boolean cond71 = false;\n" +
//						"boolean cond72 = true;\n" +
//						"boolean res71 = false;\n" +
//						"boolean res72 = false;\n" +
//						"boolean res73 = false;\n" +
//						"if (cond71) \n" + 
//						"	res71 = true;\n" +
//						" else if (cond72) {\n" + 
//						"	print_err('cond72: within else-if body');\n" + 
//						"	res72 = true;\n"  +
//						"	res73 = true;\n" +
//						"}\n" +
//						// if-elseif-else test
//						"print_err('case8: if-else/if-else ');\n" + 
//						"boolean cond81 = false;\n" +
//						"boolean cond82 = false;\n" +
//						"boolean res81 = false;\n" +
//						"boolean res82 = false;\n" +
//						"boolean res83 = false;\n" +
//						"if (cond81) {\n" + 
//						"	res81 = true;\n" +
//						"} else if (cond82) {\n" + 
//						"	res82 = true;\n"  +
//						"} else {\n" +
//						"	res83 = true;\n" + 
//						"}\n" +
//						"print_err('case9: if with inactive else');\n" + 
//						// if with single statement + inactive else
//						"boolean cond9 = true;\n" +
//						"boolean res91 = false;\n" +
//						"boolean res92 = false;\n" +
//						"if (cond9) \n" + 
//						"	res91 = true;\n" +
//						"else \n" + 
//						"	res92 = true;\n" + 
//						// if with multiple statements + inactive else
//						// if with block, else with block
//						"print_err('case10: if-block with inactive else-block');\n" + 
//						"boolean cond10 = true;\n" +
//						"boolean res101 = false;\n" +
//						"boolean res102 = false;\n" +
//						"boolean res103 = false;\n" +
//						"boolean res104 = false;\n" +
//						"if (cond10) {\n" + 
//						"	res101 = true;\n" +
//						"	res102 = true;\n" +
//						"} else {\n" + 
//						"	res103 = true;\n"  +
//						"	res104 = true;\n" +
//						"}\n" +
//						// if with simple condition
//						"print_err('case 11: if with expression condition');\n" +
//						"int i=0;\n" +
//						"int j=1;\n" +
//						"boolean res11 = false;\n" + 
//						"if (i < j) {" +
//						"	print_err('i<j');\n" +
//						"	res11 = true;\n" + 
//						"}\n";
//						
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		// if with single statement
//		assertEquals("cond1", true, executor.getVariableValue("cond1"));
//		assertEquals("res1", true, executor.getVariableValue("res1"));
//
//		// if with mutliple statements (block)
//		assertEquals("cond2", true, executor.getVariableValue("cond2"));
//		assertEquals("res21", true, executor.getVariableValue("res21"));
//		assertEquals("res22", true, executor.getVariableValue("res22"));
//		
//		// else with single statement
//		assertEquals("cond3", false, executor.getVariableValue("cond3"));
//		assertEquals("res31", false, executor.getVariableValue("res31"));
//		assertEquals("res32", true, executor.getVariableValue("res32"));
//		
//		// else with multiple statements (block)
//		assertEquals("cond4", false, executor.getVariableValue("cond4"));
//		assertEquals("res41", false, executor.getVariableValue("res41"));
//		assertEquals("res42", true, executor.getVariableValue("res42"));
//		assertEquals("res43", true, executor.getVariableValue("res43"));
//		
//		// if with block, else with block
//		assertEquals("cond5", false, executor.getVariableValue("cond5"));
//		assertEquals("res51", false, executor.getVariableValue("res51"));
//		assertEquals("res52", false, executor.getVariableValue("res52"));
//		assertEquals("res53", true, executor.getVariableValue("res53"));
//		assertEquals("res54", true, executor.getVariableValue("res54"));
//		
//		// else-if with single statement
//		assertEquals("cond61", false, executor.getVariableValue("cond61"));
//		assertEquals("cond62", true, executor.getVariableValue("cond62"));
//		assertEquals("res61", false, executor.getVariableValue("res61"));
//		assertEquals("res2", true, executor.getVariableValue("res62"));
//		
//		// else-if with multiple statements
//		assertEquals("cond71", false, executor.getVariableValue("cond71"));
//		assertEquals("cond72", true, executor.getVariableValue("cond72"));
//		assertEquals("res71", false, executor.getVariableValue("res71"));
//		assertEquals("res72", true, executor.getVariableValue("res72"));
//		assertEquals("res73", true, executor.getVariableValue("res73"));
//		
//		// if-elseif-else test
//		assertEquals("cond81", false, executor.getVariableValue("cond81"));
//		assertEquals("cond82", false, executor.getVariableValue("cond82"));
//		assertEquals("res81", false, executor.getVariableValue("res81"));
//		assertEquals("res82", false, executor.getVariableValue("res82"));
//		assertEquals("res83", true, executor.getVariableValue("res83"));
//		
//		// if with single statement + inactive else
//		assertEquals("cond9", true, executor.getVariableValue("cond9"));
//		assertEquals("res91", true, executor.getVariableValue("res91"));
//		assertEquals("res92", false, executor.getVariableValue("res92"));
//		
//		// if with multiple statements + inactive else with block
//		assertEquals("cond10", true, executor.getVariableValue("cond10"));
//		assertEquals("res101", true, executor.getVariableValue("res101"));
//		assertEquals("res102", true, executor.getVariableValue("res102"));
//		assertEquals("res103", false, executor.getVariableValue("res103"));
//		assertEquals("res104", false, executor.getVariableValue("res104"));
//		
//		// if with condition
//		assertEquals("i", 0, executor.getVariableValue("i"));
//		assertEquals("j", 1, executor.getVariableValue("j"));
//		assertEquals("res11", true, executor.getVariableValue("res11"));
//		
//	}
//
//	
//	public void test_switch(){
//		System.out.println("\nSwitch test:");
//		String expStr =
//			// simple switch
//			"int cond1 = 1;\n" +
//			"boolean res11 = false;\n" + 
//			"boolean res12 = false;\n" + 
//			"boolean res13 = false;\n" +
//			"switch (cond1) {\n" +
//			"case 0: res11 = true;\n" +
//			"	break;\n" + 
//			"case 1: res12 = true;\n" +
//			"	break;\n" + 
//			"default: res13 = true;\n" +
//			"	break;\n" + 
//			"}\n" + 
//			// simple switch, no break
//			"int cond2 = 1;\n" +
//			"boolean res21 = false;\n" + 
//			"boolean res22 = false;\n" + 
//			"boolean res23 = false;\n" +
//			"switch (cond2) {\n" +
//			"case 0: res21 = true;\n" +
//			"	break;\n" + 
//			"case 1: res22 = true;\n" +
//			"default: res23 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// default branch
//			"int cond3 = 3;\n" +
//			"boolean res31 = false;\n" + 
//			"boolean res32 = false;\n" + 
//			"boolean res33 = false;\n" +
//			"switch (cond3) {\n" +
//			"case 0: res31 = true;\n" +
//			"	break;\n" + 
//			"case 1: res32 = true;\n" +
//			"	break;\n" + 
//			"default: res33 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// no-default branch => no match
//			"int cond4 = 3;\n" +
//			"boolean res41 = false;\n" + 
//			"boolean res42 = false;\n" + 
//			"boolean res43 = false;\n" +
//			"switch (cond4) {\n" +
//			"case 0: res41 = true;\n" +
//			"	break;\n" + 
//			"case 1: res42 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// multiple statements under single case
//			"int cond5 = 1;\n" +
//			"boolean res51 = false;\n" + 
//			"boolean res52 = false;\n" + 
//			"boolean res53 = false;\n" +
//			"boolean res54 = false;\n" + 
//			"switch (cond5) {\n" +
//			"case 0: res51 = true;\n" +
//			"	break;\n" + 
//			"case 1: \n" +
//			"	res52 = true;\n" +
//			"	res53 = true;\n" +
//			"	break;\n" + 
//			"default: res54 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// single statement for multiple cases
//			"int cond6 = 1;\n" +
//			"boolean res61 = false;\n" + 
//			"boolean res62 = false;\n" + 
//			"boolean res63 = false;\n" +
//			"boolean res64 = false;\n" + 
//			"switch (cond6) {\n" +
//			"case 0: " +
//			"	res61 = true;\n" +
//			"case 1: \n" +
//			"case 2: \n" +
//			"	res62 = true;\n" +
//			"case 3: \n" +
//			"case 4: \n" +
//			"	res63 = true;\n" +
//			"	break;\n" + 
//			"default: res64 = true;\n" +
//			"	break;\n" + 
//			"}\n";
//			
//
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		// simple switch
//		assertEquals("cond1", 1, executor.getVariableValue("cond1"));
//		assertEquals("res11", false, executor.getVariableValue("res11"));
//		assertEquals("res12", true, executor.getVariableValue("res12"));
//		assertEquals("res13", false, executor.getVariableValue("res13"));
//		
//		// switch, no break
//		assertEquals("cond2", 1, executor.getVariableValue("cond2"));
//		assertEquals("res21", false, executor.getVariableValue("res21"));
//		assertEquals("res22", true, executor.getVariableValue("res22"));
//		assertEquals("res23", true, executor.getVariableValue("res23"));
//		
//		// default branch
//		assertEquals("cond3", 3, executor.getVariableValue("cond3"));
//		assertEquals("res31", false, executor.getVariableValue("res31"));
//		assertEquals("res32", false, executor.getVariableValue("res32"));
//		assertEquals("res33", true, executor.getVariableValue("res33"));
//	
//		// no default branch => no match
//		assertEquals("cond4", 3, executor.getVariableValue("cond4"));
//		assertEquals("res41", false, executor.getVariableValue("res41"));
//		assertEquals("res42", false, executor.getVariableValue("res42"));
//		assertEquals("res43", false, executor.getVariableValue("res43"));
//		
//		// multiple statements in a single case-branch
//		assertEquals("cond5", 1, executor.getVariableValue("cond5"));
//		assertEquals("res51", false, executor.getVariableValue("res51"));
//		assertEquals("res52", true, executor.getVariableValue("res52"));
//		assertEquals("res53", true, executor.getVariableValue("res53"));
//		assertEquals("res54", false, executor.getVariableValue("res54"));
//		
//		// single statement shared by several case labels
//		assertEquals("cond6", 1, executor.getVariableValue("cond6"));
//		assertEquals("res61", false, executor.getVariableValue("res51"));
//		assertEquals("res62", true, executor.getVariableValue("res62"));
//		assertEquals("res63", true, executor.getVariableValue("res63"));
//		assertEquals("res64", false, executor.getVariableValue("res64"));
//	}
//	
//	public void test_int_switch(){
//		System.out.println("\nSwitch using non-int variable test:");
//		String expStr =
//			// simple switch using int
//			"int cond1 = 1;\n" +
//			"boolean res11 = false;\n" + 
//			"boolean res12 = false;\n" + 
//			"boolean res13 = false;\n" +
//			"switch (cond1) {\n" +
//			"case 1:\n" +
//			"	res11 = true;\n" +
//			"	break;\n" + 	
//			"case 12:\n" +
//			"	res12 = true;\n" +
//			"	break;\n" + 
//			"default:\n" +
//			"	res13 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// first case is not followed by a break
//			"int cond2 = 1;\n" +
//			"boolean res21 = false;\n" + 
//			"boolean res22 = false;\n" + 
//			"boolean res23 = false;\n" +
//			"switch (cond2) {\n" +
//			"case 1:\n" +
//			"	res21 = true;\n" +
//			"case 12:\n" +
//			"	res22 = true;\n" +
//			"	break;\n" + 
//			"default:\n" +
//			"	res23 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// first and second case have multiple labels
//			"int cond3 = 12;\n" +
//			"boolean res31 = false;\n" + 
//			"boolean res32 = false;\n" + 
//			"boolean res33 = false;\n" +
//			"switch (cond3) {\n" +
//			"case 10:\n" +
//			"case 11:\n" +
//			"	res31 = true;\n" +
//			"	break;\n" + 	
//			"case 12:\n" +
//			"case 13:\n" +
//			"	res32 = true;\n" +
//			"	break;\n" + 
//			"default:\n" +
//			"	res33 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// first and second case have multiple labels and no break after first group
//			"int cond4 = 11;\n" +
//			"boolean res41 = false;\n" + 
//			"boolean res42 = false;\n" + 
//			"boolean res43 = false;\n" +
//			"switch (cond4) {\n" +
//			"case 10:\n" +
//			"case 11:\n" +
//			"	res41 = true;\n" +
//			"case 12:\n" +
//			"case 13:\n" +
//			"	res42 = true;\n" +
//			"	break;\n" + 
//			"default:\n" +
//			"	res43 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// default case intermixed with other case labels in the second group
//			"int cond5 = 11;\n" +
//			"boolean res51 = false;\n" + 
//			"boolean res52 = false;\n" + 
//			"boolean res53 = false;\n" +
//			"switch (cond5) {\n" +
//			"case 10:\n" +
//			"case 11:\n" +
//			"	res51 = true;\n" +
//			"case 12:\n" +
//			"default:\n" +
//			"case 13:\n" +
//			"	res52 = true;\n" +
//			"case 14:\n" +
//			"	res53 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// default case intermixed, with break
//			"int cond6 = 16;\n" +
//			"boolean res61 = false;\n" + 
//			"boolean res62 = false;\n" + 
//			"boolean res63 = false;\n" +
//			"switch (cond6) {\n" +
//			"case 10:\n" +
//			"case 11:\n" +
//			"	res61 = true;\n" +
//			"case 12:\n" +
//			"default:\n" +
//			"case 13:\n" +
//			"	res62 = true;\n" +
//			"	break;\n" + 
//			"case 14:\n" +
//			"	res63 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// continue test
//			"int i = 0;\n" +
//			"boolean[]  res7;\n" + 
//			"boolean res71 = false;\n" +
//			"boolean res72 = false;\n" +
//			"boolean res73 = false;\n" + 
//			"while (i < 6) {\n" +
//			"	print_err(res7);\n" +
//			"	res7[i*3] = res71;\n" +
//			"	res7[i*3+1] = res72;\n" +
//			"	res7[i*3+2] = res73;\n" +
//			"						\n" +
//			"	res71 = false;\n" +
//			"	res72 = false;\n" +
//			"	res73 = false;\n" +
//			"						\n" +
//			"	switch (i) {\n" +
//			"	case 0:\n" +
//			"	case 1:\n" +
//			"		res71 = true;\n" +
//			"		print_err('res71: ' + res71);\n" +
//			"	case 2:\n" +
//			"	default:\n" +
//			"	case 3:\n" +
//			"		res72 = true;\n" +
//			"		print_err('res72: ' + res72);\n" +
//			"		i++;\n" + 
//			"		continue;\n" + 
//			"	case 4:\n" +
//			"		res73 = true;\n" +
//			"		print_err('res73: ' + res73);\n" +
//			"		break;\n" + 
//			"	}\n" + 
//			"	i++;\n" + 
//			"}\n" + 
//			"print_err(res7);\n" +			
//			// return test
//			"function string switchFunction(int cond) {\n" + 
//			"	string ret = '';\n" + 
//			"	switch (cond) {\n" +
//			"	case 0:\n" +
//			"		ret = ret + 0;\n" +
//			"	case 1:\n" +
//			"		ret = ret + 1;\n" +
//			"	case 2:\n" +
//			"		ret = ret + 2;\n" + 
//			"	default:\n" +
//			"	case 3:\n" +
//			"		ret = ret + 3;\n" + 
//			"		return ret;\n" + 
//			"	case 4:\n" +
//			"		ret = ret + 4;\n" +
//			"		return ret;\n" + 
//			"	}\n" + 
//			"}\n\n" +
//			"string[] res8;\n" +
//			"for (int i=0; i<6; i++) {\n" +
//			"	res8[i] = switchFunction(i);\n" +
//			"}\n";	
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		
//		// simple switch
//		assertEquals("cond1", 1, executor.getVariableValue("cond1"));
//		assertEquals("res11", true, executor.getVariableValue("res11"));
//		assertEquals("res12", false, executor.getVariableValue("res12"));
//		assertEquals("res13", false, executor.getVariableValue("res13"));
//		
//		// first case is not followed by a break
//		assertEquals("cond2", 1, executor.getVariableValue("cond2"));
//		assertEquals("res21", true, executor.getVariableValue("res21"));
//		assertEquals("res22", true, executor.getVariableValue("res22"));
//		assertEquals("res23", false, executor.getVariableValue("res23"));
//
//		// first and second case have multiple labels
//		assertEquals("cond3", 12, executor.getVariableValue("cond3"));
//		assertEquals("res31", false, executor.getVariableValue("res31"));
//		assertEquals("res32", true, executor.getVariableValue("res32"));
//		assertEquals("res33", false, executor.getVariableValue("res33"));
//
//		// first and second case have multiple labels and no break after first group
//		assertEquals("cond4", 11, executor.getVariableValue("cond4"));
//		assertEquals("res41", true, executor.getVariableValue("res41"));
//		assertEquals("res42", true, executor.getVariableValue("res42"));
//		assertEquals("res43", false, executor.getVariableValue("res43"));
//
//		// default case intermixed with other case labels in the second group
//		assertEquals("cond5", 11, executor.getVariableValue("cond5"));
//		assertEquals("res51", true, executor.getVariableValue("res51"));
//		assertEquals("res52", true, executor.getVariableValue("res52"));
//		assertEquals("res53", true, executor.getVariableValue("res53"));
//
//		// default case intermixed, with break
//		assertEquals("cond6", 16, executor.getVariableValue("cond6"));
//		assertEquals("res61", false, executor.getVariableValue("res61"));
//		assertEquals("res62", true, executor.getVariableValue("res62"));
//		assertEquals("res63", false, executor.getVariableValue("res63"));
//
//		// continue test
//		assertEquals("res7",createList(
//				/* i=0 */ false, false, false, 
//				/* i=1 */ true, true, false, 
//				/* i=2 */ true, true, false, 
//				/* i=3 */ false, true, false,
//				/* i=4 */ false, true, false, 
//				/* i=5 */ false, false, true
//					), executor.getVariableValue("res7"));
//
//		// return test
//		assertEquals("res8", createList("0123","123","23","3","4","3"), executor.getVariableValue("res8"));
//		
//	}
//
//
//	public void test_non_int_switch(){
//		System.out.println("\nSwitch using non-int variable test:");
//		String expStr =
//			// simple switch using int
//			"string cond1 = '1';\n" +
//			"boolean res11 = false;\n" + 
//			"boolean res12 = false;\n" + 
//			"boolean res13 = false;\n" +
//			"switch (cond1) {\n" +
//			"case '1':\n" +
//			"	res11 = true;\n" +
//			"	break;\n" + 	
//			"case '12':\n" +
//			"	res12 = true;\n" +
//			"	break;\n" + 
//			"default:\n" +
//			"	res13 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// first case is not followed by a break
//			"string cond2 = '1';\n" +
//			"boolean res21 = false;\n" + 
//			"boolean res22 = false;\n" + 
//			"boolean res23 = false;\n" +
//			"switch (cond2) {\n" +
//			"case '1':\n" +
//			"	res21 = true;\n" +
//			"case '12':\n" +
//			"	res22 = true;\n" +
//			"	break;\n" + 
//			"default:\n" +
//			"	res23 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// first and second case have multiple labels
//			"string cond3 = '12';\n" +
//			"boolean res31 = false;\n" + 
//			"boolean res32 = false;\n" + 
//			"boolean res33 = false;\n" +
//			"switch (cond3) {\n" +
//			"case '10':\n" +
//			"case '11':\n" +
//			"	res31 = true;\n" +
//			"	break;\n" + 	
//			"case '12':\n" +
//			"case '13':\n" +
//			"	res32 = true;\n" +
//			"	break;\n" + 
//			"default:\n" +
//			"	res33 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// first and second case have multiple labels and no break after first group
//			"string cond4 = '11';\n" +
//			"boolean res41 = false;\n" + 
//			"boolean res42 = false;\n" + 
//			"boolean res43 = false;\n" +
//			"switch (cond4) {\n" +
//			"case '10':\n" +
//			"case '11':\n" +
//			"	res41 = true;\n" +
//			"case '12':\n" +
//			"case '13':\n" +
//			"	res42 = true;\n" +
//			"	break;\n" + 
//			"default:\n" +
//			"	res43 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// default case stringermixed with other case labels in the second group
//			"string cond5 = '11';\n" +
//			"boolean res51 = false;\n" + 
//			"boolean res52 = false;\n" + 
//			"boolean res53 = false;\n" +
//			"switch (cond5) {\n" +
//			"case '10':\n" +
//			"case '11':\n" +
//			"	res51 = true;\n" +
//			"case '12':\n" +
//			"default:\n" +
//			"case '13':\n" +
//			"	res52 = true;\n" +
//			"case '14':\n" +
//			"	res53 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// default case intermixed, with break
//			"string cond6 = '16';\n" +
//			"boolean res61 = false;\n" + 
//			"boolean res62 = false;\n" + 
//			"boolean res63 = false;\n" +
//			"switch (cond6) {\n" +
//			"case '10':\n" +
//			"case '11':\n" +
//			"	res61 = true;\n" +
//			"case '12':\n" +
//			"default:\n" +
//			"case '13':\n" +
//			"	res62 = true;\n" +
//			"	break;\n" + 
//			"case '14':\n" +
//			"	res63 = true;\n" +
//			"	break;\n" + 
//			"}\n" +
//			// continue test
//			"int i = 0;\n" +
//			"boolean[]  res7;\n" + 
//			"boolean res71 = false;\n" +
//			"boolean res72 = false;\n" +
//			"boolean res73 = false;\n" + 
//			"while (i < 6) {\n" +
//			"	print_err(res7);\n" +
//			"	res7[i*3] = res71;\n" +
//			"	res7[i*3+1] = res72;\n" +
//			"	res7[i*3+2] = res73;\n" +
//			"						\n" +
//			"	res71 = false;\n" +
//			"	res72 = false;\n" +
//			"	res73 = false;\n" +
//			"						\n" +
//			"	string iValue = i + '';\n" + 
//			"	switch (iValue) {\n" +
//			"	case '0':\n" +
//			"	case '1':\n" +
//			"		res71 = true;\n" +
//			"		print_err('res71: ' + res71);\n" +
//			"	case '2':\n" +
//			"	default:\n" +
//			"	case '3':\n" +
//			"		res72 = true;\n" +
//			"		print_err('res72: ' + res72);\n" +
//			"		i++;\n" + 
//			"		continue;\n" + 
//			"	case '4':\n" +
//			"		res73 = true;\n" +
//			"		print_err('res73: ' + res73);\n" +
//			"		break;\n" + 
//			"	}\n" + 
//			"	i++;\n" + 
//			"}\n" + 
//			"print_err(res7);\n" +			
//			// return test
//			"function string switchFunction(string cond) {\n" + 
//			"	string ret = '';\n" + 
//			"	switch (cond) {\n" +
//			"	case '0':\n" +
//			"		ret = ret + 0;\n" +
//			"	case '1':\n" +
//			"		ret = ret + 1;\n" +
//			"	case '2':\n" +
//			"		ret = ret + 2;\n" + 
//			"	default:\n" +
//			"	case '3':\n" +
//			"		ret = ret + 3;\n" + 
//			"		return ret;\n" + 
//			"	case '4':\n" +
//			"		ret = ret + 4;\n" +
//			"		return ret;\n" + 
//			"	}\n" + 
//			"}\n\n" +
//			"string[] res8;\n" +
//			"for (int i=0; i<6; i++) {\n" +
//			"	res8[i] = switchFunction(i + '');\n" +
//			"}\n";	
//		
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//
//		// simple switch
//		assertEquals("cond1", "1", executor.getVariableValue("cond1"));
//		assertEquals("res11", true, executor.getVariableValue("res11"));
//		assertEquals("res12", false, executor.getVariableValue("res12"));
//		assertEquals("res13", false, executor.getVariableValue("res13"));
//		
//		// first case is not followed by a break
//		assertEquals("cond2", "1", executor.getVariableValue("cond2"));
//		assertEquals("res21", true, executor.getVariableValue("res21"));
//		assertEquals("res22", true, executor.getVariableValue("res22"));
//		assertEquals("res23", false, executor.getVariableValue("res23"));
//
//		// first and second case have multiple labels
//		assertEquals("cond3", "12", executor.getVariableValue("cond3"));
//		assertEquals("res31", false, executor.getVariableValue("res31"));
//		assertEquals("res32", true, executor.getVariableValue("res32"));
//		assertEquals("res33", false, executor.getVariableValue("res33"));
//
//		// first and second case have multiple labels and no break after first group
//		assertEquals("cond4", "11", executor.getVariableValue("cond4"));
//		assertEquals("res41", true, executor.getVariableValue("res41"));
//		assertEquals("res42", true, executor.getVariableValue("res42"));
//		assertEquals("res43", false, executor.getVariableValue("res43"));
//
//		// default case intermixed with other case labels in the second group
//		assertEquals("cond5", "11", executor.getVariableValue("cond5"));
//		assertEquals("res51", true, executor.getVariableValue("res51"));
//		assertEquals("res52", true, executor.getVariableValue("res52"));
//		assertEquals("res53", true, executor.getVariableValue("res53"));
//
//		// default case intermixed, with break
//		assertEquals("cond6", "16", executor.getVariableValue("cond6"));
//		assertEquals("res61", false, executor.getVariableValue("res61"));
//		assertEquals("res62", true, executor.getVariableValue("res62"));
//		assertEquals("res63", false, executor.getVariableValue("res63"));
//
//		// continue test
//		assertEquals("res7",createList(
//				/* i=0 */ false, false, false, 
//				/* i=1 */ true, true, false, 
//				/* i=2 */ true, true, false, 
//				/* i=3 */ false, true, false,
//				/* i=4 */ false, true, false, 
//				/* i=5 */ false, false, true
//					), executor.getVariableValue("res7"));
//
//		// return test
//		assertEquals("res8", createList("0123","123","23","3","4","3"), executor.getVariableValue("res8"));
//		
//						
//	}
//	
//	
//	public void test_while(){
//		System.out.println("\nWhile test:");
//		String expStr = 
//						"print_err('while1: simple loop');\n" + 
//						"int[] res1;\n" +
//						"int ctr1 = -1;\n" +
//						"while (++ctr1<3) {\n" +
//						"	res1 = res1 + ctr1;\n " +
//						"	print_err('Iteration ' + ctr1);\n" +
//						"}" +
//						// continue test
//						"print_err('while2: continue loop');\n" + 
//						"int[] res2;\n" +
//						"int ctr2 = -1;\n" +
//						"while (++ctr2<3) {\n" +
//						"	if (ctr2 == 1) {\n" +
//						"		continue;\n" +
//						"	}\n" + 	
//						"	res2 = res2 + ctr2;\n " +
//						"	print_err('Iteration ' + ctr2);\n" +
//						"}" +
//						// break test
//						"print_err('while3: break loop');\n" + 
//						"int[] res3;\n" +
//						"int ctr3 = -1;\n" +
//						"while (++ctr3<3) {\n" +
//						"	if (ctr3 == 1) {\n" +
//						"		break;\n" +
//						"	}\n" + 	
//						"	res3 = res3 + ctr3;\n " +
//						"	print_err('Iteration ' + ctr3);\n" +
//						"}\n";
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		// simple while
//		assertEquals(createList(0,1,2), executor.getVariableValue("res1"));
//		// continue
//		assertEquals(createList(0,2), executor.getVariableValue("res2"));
//		// break
//		assertEquals(createList(0), executor.getVariableValue("res3"));
//		
//	}
//
//	public void test_do_while(){
//		System.out.println("\nDo-while test:");
//		String expStr = "print_err('do-while1: simple loop');\n" + 
//						"int[] res1;\n" +
//						"int ctr1 = 0;\n" +
//						"do {\n" +
//						"	res1[ctr1]=ctr1;\n " +
//						"	print_err('Iteration ' + ctr1);\n" +
//						"} while (++ctr1<3)\n" + 
//						// continue test
//						"print_err('do-while2: continue loop');\n" + 
//						"int[] res2;\n" +
//						"int ctr2 = 0;\n" +
//						"do {\n" +
//						"	if (ctr2 == 1) {\n" +
//						"		continue;\n" +
//						"	}\n" + 	
//						"	res2[ctr2]=ctr2;\n " +
//						"	print_err('Iteration ' + ctr2);\n" +
//						"} while (++ctr2<3)\n" +
//						// break test
//						"print_err('do-while3: break loop');\n" + 
//						"int[] res3;\n" +
//						"int ctr3 = 0;\n" +
//						"do {\n" +
//						"	if (ctr3 == 1) {\n" +
//						"		break;\n" +
//						"	}\n" + 	
//						"	res3[ctr3]=ctr3;\n " +
//						"	print_err('Iteration ' + ctr3);\n" +
//						"} while (++ctr3<3)\n";
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		// simple loop
//		assertEquals(createList(0,1,2), executor.getVariableValue("res1"));
//		// continue
//		assertEquals(createList(0,null,2), executor.getVariableValue("res2"));
//		// break
//		assertEquals(createList(0), executor.getVariableValue("res3"));
//		
//	}
//	
//	
//	public void test_for(){
//		System.out.println("\nFor test:");
//		String expStr = "print_err('for4: simple loop');\n" + 
//						"int[] res1;\n" +
//						"for (int ctr1=0; ctr1 < 3; ctr1++) {\n" +
//						"	res1[ctr1]=ctr1;\n " +
//						"	print_err('Iteration ' + ctr1);\n" +
//						"}\n" + 
//						// continue test
//						"print_err('for2: continue loop');\n" + 
//						"int[] res2;\n" +
//						"for (int ctr2=0; ctr2<3; ctr2++) {\n" +
//						"	if (ctr2 == 1) {\n" +
//						"		continue;\n" +
//						"	}\n" + 	
//						"	res2[ctr2]=ctr2;\n " +
//						"	print_err('Iteration ' + ctr2);\n" +
//						"}\n" +
//						// break test
//						"print_err('for3: break loop');\n" + 
//						"int[] res3;\n" +
//						"for (int ctr3=0; ctr3<3; ctr3++) {\n" +
//						"	if (ctr3 == 1) {\n" +
//						"		break;\n" +
//						"	}\n" + 	
//						"	res3[ctr3]=ctr3;\n " +
//						"	print_err('Iteration ' + ctr3);\n" +
//						"}\n" +
//						// empty init
//						"print_err('for4: empty init');\n" + 
//						"int[] res4;\n" +
//						"int ctr4 = 0;\n" + 
//						"for (; ctr4 < 3; ctr4++) {\n" +
//						"	res4[ctr4]=ctr4;\n " +
//						"	print_err('Iteration ' + ctr4);\n" +
//						"}\n" +
//						// empty update
//						"print_err('for5: empty update');\n" + 
//						"int[] res5;\n" +
//						"for (int ctr5=0; ctr5 < 3;) {\n" +
//						"	res5[ctr5]=ctr5;\n " +
//						"	print_err('Iteration ' + ctr5);\n" +
//						"	ctr5++;\n" +
//						"}\n" +
//						// empty final condition
//						"print_err('for6: empty final condition');\n" + 
//						"int[] res6;\n" +
//						"for (int ctr6=0; ; ctr6++) {\n" +
//						"	if (ctr6 >= 3) {\n" +
//						"		break;\n" +
//						"	}\n" +
//						"	res6[ctr6]=ctr6;\n " +
//						"	print_err('Iteration ' + ctr6);\n" +
//						"}\n"  +
//						// all conditions empty
//						"print_err('for7: all conditions empty');\n" + 
//						"int[] res7;\n" +
//						"int ctr7=0;\n" + 
//						"for (;;) {\n" +
//						"	if (ctr7 >= 3) {\n" +
//						"		break;\n" +
//						"	}\n" +
//						"	res7[ctr7]=ctr7;\n " +
//						"	print_err('Iteration ' + ctr7);\n" +
//						"	ctr7++;\n" + 
//						"}\n";
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		// simple loop
//		assertEquals(createList(0,1,2), executor.getVariableValue("res1"));
//		// continue
//		assertEquals(createList(0,null,2), executor.getVariableValue("res2"));
//		// break
//		assertEquals(createList(0), executor.getVariableValue("res3"));
//		// empty init
//		assertEquals(createList(0,1,2), executor.getVariableValue("res4"));
//		// empty update
//		assertEquals(createList(0,1,2), executor.getVariableValue("res5"));
//		// empty final condition
//		assertEquals(createList(0,1,2), executor.getVariableValue("res6"));
//		// all conditions empty
//		assertEquals(createList(0,1,2), executor.getVariableValue("res7"));
//		
//	}
//	
//	public void test_foreach() {
//		System.out.println("\nForeach test:");
//		String expStr = 
//						// iterating over list
//						"string[] it = [ 'a', 'b', 'c' ];\n" +
//						"string ret = '';\n" +
//						"foreach (string s : it) {\n" +
//						"	ret = ret + s;\n" +
//						"}\n" +
//						"print_err(ret);\n"+
//						// integer fields
//						"print_err('foreach1: integer fields');\n" + 
//						"int[] intRes;\n" +
//						"int i=0;\n" + 
//						"foreach (int intVal : $firstInput.*) {\n" +
//						"	intRes[i++]=intVal;\n " +
//						"	print_err('int: ' + intRes);\n" +
//						"}\n" +
//						// long fields
//						"print_err('foreach2: long fields');\n" + 
//						"long[] longRes;\n" +
//						"i=0;\n" + 
//						"foreach (long longVal : $firstInput.*) {\n" +
//						"	longRes[i++]=longVal;\n " +
//						"	print_err('long: ' + longRes);\n" +
//						"}\n" +
//						// double fields
//						"print_err('foreach3: double fields');\n" + 
//						"double[] doubleRes;\n" +
//						"i=0;\n" + 
//						"foreach (double doubleVal : $firstInput.*) {\n" +
//						"	doubleRes[i++]=doubleVal;\n " +
//						"	print_err('double: ' + doubleRes);\n" +
//						"}\n" +
//						// decimal fields
//						"print_err('foreach5: decimal fields');\n" + 
//						"decimal[] decimalRes;\n" +
//						"i=0;\n" + 
//						"foreach (decimal decimalVal : $firstInput.*) {\n" +
//						"	decimalRes[i++]=decimalVal;\n " +
//						"	print_err('decimal: ' + decimalRes);\n" +
//						"}\n" +
//						// boolean fields
//						"print_err('foreach4: boolean fields');\n" + 
//						"boolean[] booleanRes;\n" +
//						"i=0;\n" + 
//						"foreach (boolean booleanVal : $firstInput.*) {\n" +
//						"	booleanRes[i++]=booleanVal;\n " +
//						"	print_err('boolean: ' + booleanRes);\n" +
//						"}\n" +
//						// string fields
//						"print_err('foreach6: string fields');\n" + 
//						"string[] stringRes;\n" +
//						"i=0;\n" + 
//						"foreach (string stringVal : $firstInput.*) {\n" +
//						"	stringRes[i++]=stringVal;\n " +
//						"	print_err('string: ' + stringRes);\n" +
//						"}\n" +
//						// date fields
//						"print_err('foreach7: date fields');\n" + 
//						"date[] dateRes;\n" +
//						"i=0;\n" + 
//						"foreach (date dateVal : $firstInput.*) {\n" +
//						"	dateRes[i++]=dateVal;\n " +
//						"	print_err('date: ' + dateRes);\n" +
//						"}\n";
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		assertEquals(createList("a","b","c"), executor.getVariableValue("it"));
//		assertEquals("abc",executor.getVariableValue("ret"));
//		assertEquals(createList(VALUE_VALUE), executor.getVariableValue("intRes"));
//		assertEquals(createList(BORN_MILLISEC_VALUE), executor.getVariableValue("longRes"));
//		assertEquals(createList(AGE_VALUE), executor.getVariableValue("doubleRes"));
//		assertEquals(createList(CURRENCY_VALUE), executor.getVariableValue("decimalRes"));
//		assertEquals(createList(FLAG_VALUE), executor.getVariableValue("booleanRes"));
//		assertEquals(createList(NAME_VALUE,CITY_VALUE), executor.getVariableValue("stringRes"));
//		assertEquals(createList(BORN_VALUE), executor.getVariableValue("dateRes"));
//	}
//	
//	public void test_return(){
//		System.out.println("\nReturn test:");
//		String expStr = "function int sum(int a, int b) {\n" +
//						"	return a+b;\n" +
//						"}\n\n" + 
//						"int lhs = 1;\n" +
//						"int rhs = 2;\n" +
//						"int res = sum(lhs,rhs);\n";
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		assertEquals(Integer.valueOf(1), executor.getVariableValue("lhs"));
//		assertEquals(Integer.valueOf(2), executor.getVariableValue("rhs"));
//		assertEquals(Integer.valueOf(3), executor.getVariableValue("res"));
//	}
//	
//	
//	public void test_overloading(){
//		System.out.println("\nReturn test:");
//		String expStr = "function int sum(int a, int b) {\n" +
//						"	return a+b;\n" +
//						"}\n\n" + 
//						"function string sum(string a, string b) {\n" +
//						"	return a+b;\n" +
//						"}\n\n" +
//						"int res1 = sum(1,2);\n" +
//						"print_err(res1);\n" +
//						"string res2 = sum('Memento ', 'mori');\n" +
//						"print_err(res2);";
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		assertEquals(Integer.valueOf(3), executor.getVariableValue("res1"));
//		assertEquals("Memento mori", executor.getVariableValue("res2"));
//	}
//	
//	public void test_built_in_functions(){
//		System.out.println("\nBuilt-in functions test:");
//		String expStr = 
//				"int notNullValue = 1; int nullValue = null;\n" +
//				"boolean isNullRes1 = isnull(notNullValue);\n" +
//				"boolean isNullRes2 = isnull(nullValue);\n" +
//				"int nvlRes1 = nvl(notNullValue,2);\n" +
//				"int nvlRes2 = nvl(nullValue,2);\n" +
//				"int nvl2Res1 = nvl2(notNullValue,1,2);\n" +
//				"int nvl2Res2 = nvl2(nullValue,1,2);\n" +
//				"int iifRes1 = iif(isnull(notNullValue),1,2);\n" +
//				"int iifRes2 = iif(isnull(nullValue),1,2);\n" +
//				"print_err('This message belongs to standard error');\n" +
//				"print_log(debug, 'This message belongs to DEBUG');\n" +
//				"print_log(info, 'This message belongs to INFO');\n" +
//				"print_log(warn, 'This message belongs to WARN');\n" +
//				"print_log(error, 'This message belongs to ERROR');\n" +
//				"print_log(fatal, 'This message belongs to FATAL');\n" +
//				"print_log(trace, 'This message belongs to TRACE');\n";
//				
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		assertEquals(Integer.valueOf(1), executor.getVariableValue("notNullValue"));
//		assertNull(executor.getVariableValue("nullValue"));
//		assertEquals(false, executor.getVariableValue("isNullRes1"));
//		assertEquals(true, executor.getVariableValue("isNullRes2"));
//		assertEquals(executor.getVariableValue("notNullValue"), executor.getVariableValue("nvlRes1"));
//		assertEquals(Integer.valueOf(2), executor.getVariableValue("nvlRes2"));
//		assertEquals(executor.getVariableValue("notNullValue"), executor.getVariableValue("nvl2Res1"));
//		assertEquals(Integer.valueOf(2), executor.getVariableValue("nvl2Res2"));
//		assertEquals(Integer.valueOf(2), executor.getVariableValue("iifRes1"));
//		assertEquals(Integer.valueOf(1), executor.getVariableValue("iifRes2"));
//	}
//	
//
//	public void test_mapping(){
//		System.out.println("\nMapping test:");
//		String expStr =
//			// different kinds of field access
//			"$0.0 = $0.0;\n" +
//			"$0.Age = $0.Age;\n" +
//			"$firstOutput.City = $firstInput.City;\n" +
//			"$firstOutput.3 = $firstInput.3;\n" +
//			// star-mapping
//			"$secondOutput.* = $secondInput.*;\n";
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		// simple mappings
//		assertEquals(NAME_VALUE, ((StringBuilder)((StringDataField)outputRecords[0].getField("Name")).getValue()).toString());
//		assertEquals(AGE_VALUE, ((NumericDataField)outputRecords[0].getField("Age")).getValue());
//		assertEquals(CITY_VALUE, ((StringBuilder)((StringDataField)outputRecords[0].getField("City")).getValue()).toString());
//		assertEquals(BORN_VALUE, ((DateDataField)outputRecords[0].getField("Born")).getValue());
//		
//		// * mapping
//		assertTrue(recordEquals(inputRecords[1],outputRecords[1]));
//	}
//	
//	public void test_sequence(){
//        System.out.println("\nSequence test:");
//        String expStr = 
//        				// int values
//        				"int[] intRes;\n" +
//        				"for (int i=0; i<3; i++) {\n" +
//        				"	intRes[i] = sequence(TestSequence,int).next();\n" + 
//        				"}\n" +
//        				// long values
//        				"sequence(TestSequence).reset();\n" +
//        				"long[] longRes;\n" +
//        				"for (int i=0; i<3; i++) {\n" +
//        				"	longRes[i] = sequence(TestSequence,long).next();\n" + 
//        				"}\n" +
//        				// string values
//        				"sequence(TestSequence).reset();\n" +
//        				"string[] stringRes;\n" +
//        				"for (int i=0; i<3; i++) {\n" +
//        				"	stringRes[i] = sequence(TestSequence,string).next();\n" + 
//        				"}\n" +
//        				// current
//        				"int intCurrent = sequence(TestSequence,int).current();\n" + 
//        				"long longCurrent = sequence(TestSequence,long).current();\n" + 
//        				"string stringCurrent = sequence(TestSequence,string).current();\n";
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		assertEquals(createList(1,2,3), executor.getVariableValue("intRes"));
//		assertEquals(createList(Long.valueOf(1),Long.valueOf(2),Long.valueOf(3)),executor.getVariableValue("longRes"));
//		assertEquals(createList("1","2","3"), executor.getVariableValue("stringRes"));
//		assertEquals(Integer.valueOf(3),executor.getVariableValue("intCurrent"));
//		assertEquals(Long.valueOf(3),executor.getVariableValue("longCurrent"));
//		assertEquals("3",executor.getVariableValue("stringCurrent"));
//	}
//	
//	
//	public void test_lookup(){
//        System.out.println("\nLookup test:");
//        String expStr = 
//        				"string[] alphaResult;\n" +
//        				"string[] bravoResult;\n" +
//        				"int[] countResult;\n" +
//        				"string[] charlieResult;\n" +
//        				"int idx = 0;\n" +
//        				"for (int i=0; i<2; i++) {\n" +
//        				"	alphaResult[i] = lookup(TestLookup).get('Alpha',1).City;\n" + 
//        				"	bravoResult[i] = lookup(TestLookup).get('Bravo',2).City;\n" +
//        				"	countResult[i] = lookup(TestLookup).count('Charlie',3);\n" +
//        				"	for (int count=0; count<countResult[i]; count++) {\n" +
//        				"		charlieResult[idx++] = lookup(TestLookup).next().City;\n" +
//        				"	}\n" +
//        				"	print_err('Freeing lookup table');\n" +
//        				"	lookup(TestLookup).free();\n" +
//        				"	print_err('Initializing lookup table');\n" +
//        				"	lookup(TestLookup).init();\n" +
//        				"}\n";
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		assertEquals(createList("Andorra la Vella","Andorra la Vella"), executor.getVariableValue("alphaResult"));
//		assertEquals(createList("Bruxelles","Bruxelles"),executor.getVariableValue("bravoResult"));
//		assertEquals(createList("Chamonix","Chomutov","Chamonix","Chomutov"), executor.getVariableValue("charlieResult"));
//		assertEquals(createList(2,2),executor.getVariableValue("countResult"));
//	}
//	
//	
//	@SuppressWarnings("unchecked")
//	public void test_container_lib(){
//		System.out.println("\nContainerLib test:");
//		String expStr = "int[] origList = [1,2,3,4,5];\n" +
//						//copy
//						"int[] copyList; copy(copyList,origList);\n" + 
//						// pop
//						"int popElem = pop(origList);\n" + 
//						"int[] popList; copy(popList,origList);\n" +
//						// poll
//						"int pollElem = poll(origList);\n" + 
//						"int[] pollList; copy(pollList,origList);\n" + 
//						// push
//						"int pushElem = 6;\n" +
//						"push(origList,pushElem);\n" + 
//						"int[] pushList; copy(pushList,origList);\n" +
//						// insert
//						"int insertElem = 7;\n" + 
//						"int insertIndex = 1;\n" +
//						"insert(origList,insertIndex,insertElem);\n" +
//						"int[] insertList; copy(insertList,origList);\n" +
//						// remove
//						"int removeIndex = 2;\n" +
//						"int removeElem = remove(origList,removeIndex);\n" + 
//						"int[] removeList; copy(removeList,origList);\n" +
//						// sort
//						"int[] sortList; sort(origList); copy(sortList,origList);\n" +
//						// reverse
//						"int[] reverseList; reverse(origList); copy(reverseList,origList);\n" +
//						"int[] removeAllList; remove_all(origList); copy(removeAllList,origList);\n";
//		
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
//		// copy
//		assertEquals(createList(1,2,3,4,5), executor.getVariableValue("copyList"));
//		// pop
//		assertEquals(Integer.valueOf(5), executor.getVariableValue("popElem"));
//		assertEquals(createList(1,2,3,4), executor.getVariableValue("popList"));
//		// poll
//		assertEquals(Integer.valueOf(1), executor.getVariableValue("pollElem"));
//		assertEquals(createList(2,3,4), executor.getVariableValue("pollList"));
//		// push
//		assertEquals(Integer.valueOf(6), executor.getVariableValue("pushElem"));
//		assertEquals(createList(2,3,4,6), executor.getVariableValue("pushList"));
//		// insert
//		assertEquals(Integer.valueOf(7), executor.getVariableValue("insertElem"));
//		assertEquals(Integer.valueOf(1), executor.getVariableValue("insertIndex"));
//		assertEquals(createList(2,7,3,4,6), executor.getVariableValue("insertList"));
//		// remove
//		assertEquals(Integer.valueOf(3), executor.getVariableValue("removeElem"));
//		assertEquals(Integer.valueOf(2), executor.getVariableValue("removeIndex"));
//		assertEquals(createList(2,7,4,6), executor.getVariableValue("removeList"));
//		// sort
//		assertEquals(createList(2,4,6,7), executor.getVariableValue("sortList"));
//		// reverse
//		assertEquals(createList(7,6,4,2), executor.getVariableValue("reverseList"));
//		// remove_all
//		assertTrue(((List<Integer>)executor.getVariableValue("removeAllList")).isEmpty());
//	}
//
//	
//	
//	
//	public void test_buildInFunctions(){
//		System.out.println("\nBuild-in functions test:");
//		String expStr = 
//			"string s;s='hello world';\n" +
//						"int lenght;lenght=5;\n" +
//						"string subs;subs=substring(s,1,lenght);\n" +
//						"print_err('original string:'+s );\n" +
//						"print_err('substring:'+subs );\n" +
//						"string upper;upper=uppercase(subs);\n" +
//						"print_err('to upper case:'+upper );\n"+
//						"string lower;lower=lowercase(subs+'hI   ');\n" +
//						"print_err('to lower case:'+lower );\n"+
//						"string t;t=trim('\t  im  '+lower);\n" +
//						"print_err('after trim:'+t );\n" +
//						"//print_stack();\n"+
//						"decimal l;l=length(upper);\n" +
//						"print_err('length of '+upper+':'+l );\n"+
//						"string c;c=concat(lower,upper);\n" + //,2,',today is ',today());\n" +
////						"print_err('concatenation \"'+lower+'\"+\"'+upper+'\"+2+\",today is \"+today():'+c );\n"+
////						"date datum; date born;born=nvl($Born,today()-365);\n" +
////						"print_err('born=' + born);\n" +
////						"datum=dateadd(born,100,millisec);\n" +
////						"print_err('dataum = ' + datum );\n"+
////						"long ddiff;date otherdate;otherdate=today();\n" +
////						"ddiff=datediff(born,otherdate,year);\n" +
////						"print_err('date diffrence:'+ddiff );\n" +
////						"print_err('born: '+born+' otherdate: '+otherdate);\n" +
////						"boolean isn;isn=isnull(ddiff);\n" +
////						"print_err(isn );\n" +
//						"decimal s1;s1=nvl(l+1,1);\n" +
//						"print_err(s1 );\n" +
//						"string rep;rep=replace(c,'[lL]','t');\n" +
//						"print_err(rep );\n" +
////						"decimal(10,5) stn;stn=str2num('2.5125e-1',decimal);\n" +
////						"print_err(stn );\n" +
////						"int i = str2num('1234');\n" +
////						"string nts;nts=num2str(10,4);\n" +
////						"print_err(nts );\n" +
//						"date newdate;newdate=2001-12-20 16:30:04;\n" +
//						"int dtn;dtn=date2num(newdate,month);\n" +
//						"print_err('month: ' + dtn );\n" +
//						"int ii;ii=iif(newdate<2000-01-01,20,21);\n" +
//						"print_err('ii:'+ii);\n" +
//						"print_stack();\n" +
////						"date ndate;ndate=2002-12-24;\n" +
////						"string dts;dts=date2str(ndate,'yy.MM.dd');\n" +
////						"print_err('date to string:'+dts);\n" +
////						"print_err(str2date(dts,'yy.MM.dd'));\n" +
////						"string lef=left(dts,5);\n" +
////						"string righ=right(dts,5);\n" +
//						"print_err('s=word, soundex='+soundex('word'));\n" +
//						"print_err('s=world, soundex='+soundex('world'));\n" +
//						"int j;for (j=0;j<length(s);j++){print_err(char_at(s,j));};\n" +
//						"int charCount = count_char('mimimichal','i');\n" +
//						"print_err(charCount);\n";
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//		
////		      assertEquals("subs","ello ",executor.getGlobalVariable(parser.getGlobalVariableSlot("subs")).getTLValue().toString());
////		      assertEquals("upper","ELLO ",executor.getGlobalVariable(parser.getGlobalVariableSlot("upper")).getTLValue().toString());
////		      assertEquals("lower","ello hi   ",executor.getGlobalVariable(parser.getGlobalVariableSlot("lower")).getTLValue().toString());
////		      assertEquals("t(=trim)","im  ello hi",executor.getGlobalVariable(parser.getGlobalVariableSlot("t")).getTLValue().toString());
////		      assertEquals("l(=length)",5,executor.getGlobalVariable(parser.getGlobalVariableSlot("l")).getTLValue().getNumeric().getInt());
////		      assertEquals("c(=concat)","ello hi   ELLO 2,today is "+new Date(),executor.getGlobalVariable(parser.getGlobalVariableSlot("c")).getTLValue().toString());
//////		      assertEquals("datum",record.getField("Born").getValue(),executor.getGlobalVariable(parser.getGlobalVariableSlot("datum")).getValue().getDate());
////		      assertEquals("ddiff",-1,executor.getGlobalVariable(parser.getGlobalVariableSlot("ddiff")).getTLValue().getNumeric().getLong());
////		      assertEquals("isn",false,executor.getGlobalVariable(parser.getGlobalVariableSlot("isn")).getTLValue()==TLBooleanValue.TRUE);
////		      assertEquals("s1",Double.valueOf(6),executor.getGlobalVariable(parser.getGlobalVariableSlot("s1")).getTLValue().getNumeric().getDouble());
////		      assertEquals("rep",("etto hi   EttO 2,today is "+new Date()).replaceAll("[lL]", "t"),executor.getGlobalVariable(parser.getGlobalVariableSlot("rep")).getTLValue().toString());
////		      assertEquals("stn",0.25125,executor.getGlobalVariable(parser.getGlobalVariableSlot("stn")).getTLValue().getNumeric().getDouble());
////		      assertEquals("i",1234,executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().getNumeric().getInt());
////		      assertEquals("nts","22",executor.getGlobalVariable(parser.getGlobalVariableSlot("nts")).getTLValue().toString());
////		      assertEquals("dtn",11.0,executor.getGlobalVariable(parser.getGlobalVariableSlot("dtn")).getTLValue().getNumeric().getDouble());
////		      assertEquals("ii",21,executor.getGlobalVariable(parser.getGlobalVariableSlot("ii")).getTLValue().getNumeric().getInt());
////		      assertEquals("dts","02.12.24",executor.getGlobalVariable(parser.getGlobalVariableSlot("dts")).getTLValue().toString());
////		      assertEquals("lef","02.12",executor.getGlobalVariable(parser.getGlobalVariableSlot("lef")).getTLValue().toString());
////		      assertEquals("righ","12.24",executor.getGlobalVariable(parser.getGlobalVariableSlot("righ")).getTLValue().toString());
////		      assertEquals("charCount",3,executor.getGlobalVariable(parser.getGlobalVariableSlot("charCount")).getTLValue().getNumeric().getInt());
//		      
//	}
//
//    public void test_functions2(){
//        System.out.println("\nFunctions test:");
//        String expStr = 
//        	"string test='test';\n" +
//			"boolean isBlank=is_blank(test);\n" +
//			"string blank = '';\n" + 
//			"boolean isBlank1=is_blank(blank);\n" +
//			"string nullValue=null; boolean isBlank2=is_blank(nullValue);\n" +
//			"boolean isAscii1=is_ascii('test');\n" +
//			"boolean isAscii2=is_ascii('aÄ™Ĺ™');\n" +
//			"boolean isNumber=is_number('t1');\n" +
//			"boolean isNumber1=is_number('1g');\n" +
//			"boolean isNumber2=is_number('1');\n" +
//			"print_err(str2int('1'));\n" +
//			"boolean isNumber3=is_number('-382.334');\n" +
//			"print_err(str2double('-382.334'));\n" +
//			"boolean isNumber4=is_number('+332e2');\n" +
//			"boolean isNumber5=is_number('8982.8992e-2');\n" +
//			"print_err(str2double('8982.8992e-2'));\n" +
//			"boolean isNumber6=is_number('-7888873.2E3');\n" +
//			"print_err(str2decimal('-7888873.2E3'));\n" +
//			"boolean isInteger=is_integer('h3');\n" +
//			"boolean isInteger1=is_integer('78gd');\n" +
//			"boolean isInteger2=is_integer('8982.8992');\n" +
//			"boolean isInteger3=is_integer('-766542378');\n" +
//			"print_err(str2int('-766542378'));\n" +
//			"boolean isLong=is_long('7864232568822234');\n" +
//			"boolean isDate5=is_date('20Jul2000','ddMMMyyyy','en.US');\n" +
//			"print_err(str2date('20Jul2000','ddMMMyyyy','en.GB'));\n" +
//			"boolean isDate6=is_date('20July    2000','ddMMMMMMMMyyyy','en.US');\n" +
//			"print_err(str2date('20July    2000','ddMMMyyyy','en.GB'));\n" +
//			"boolean isDate3=is_date('4:42','HH:mm');\n" +
//			"print_err(str2date('4:42','HH:mm'));\n" +
//			"boolean isDate=is_date('20.11.2007','dd.MM.yyyy');\n" +
//			"print_err(str2date('20.11.2007','dd.MM.yyyy'));\n" +
//			"boolean isDate1=is_date('20.11.2007','dd-MM-yyyy');\n" +
//			"boolean isDate2=is_date('24:00 20.11.2007','kk:mm dd.MM.yyyy');\n" +
//			"print_err(str2date('24:00 20.11.2007','HH:mm dd.MM.yyyy'));\n" +
//			"boolean isDate4=is_date('test 20.11.2007','hhmm dd.MM.yyyy');\n" +
//			"boolean isDate7=is_date('                ','HH:mm dd.MM.yyyy',true);\n"+
//			"boolean isDate8=is_date('                ','HH:mm dd.MM.yyyy');\n"+
//			"boolean isDate9=is_date('20-15-2007','dd-MM-yyyy');\n" +
//			"print_err(str2date('20-15-2007','dd-MM-yyyy'));\n" +
//			"boolean isDate10=is_date('20-15-2007','dd-MM-yyyy',false);\n" +
//			"boolean isDate11=is_date('20-15-2007','dd-MM-yyyy',true);\n" + 
//			"boolean isDate12=is_date('942-12-1996','dd-MM-yyyy','en.US',true);\n" +
//			"boolean isDate13=is_date('942-12-1996','dd-MM-yyyy','en.US',false);\n" +
//			"boolean isDate14=is_date('12-Prosinec-1996','dd-MMM-yyyy','cs.CZ',false);\n" +
//			"boolean isDate15=is_date('12-Prosinec-1996','dd-MMM-yyyy','en.US',false);\n" + 
//			"boolean isDate16=is_date('24:00 20.11.2007','HH:mm dd.MM.yyyy');\n" +
//			"print_err(str2date('24:00 20.11.2007','HH:mm dd.MM.yyyy'));\n" +
//			"boolean isDate17=is_date('','HH:mm dd.MM.yyyy',false);\n" +
//			"boolean isDate18=is_date('','HH:mm dd.MM.yyyy',true);\n";
//
//        		
//
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//        
//
//
//		assertEquals("test", executor.getVariableValue("test"));
//		assertEquals(Boolean.FALSE,executor.getVariableValue("isBlank"));
//		assertEquals("", executor.getVariableValue("blank"));
//		assertNull(executor.getVariableValue("nullValue"));
//		assertEquals(true, executor.getVariableValue("isBlank2"));
//		assertEquals(true,executor.getVariableValue("isAscii1"));
//		assertEquals(false,executor.getVariableValue("isAscii2"));
//		assertEquals(false,executor.getVariableValue("isNumber"));
//		assertEquals(false,executor.getVariableValue("isNumber1"));
//		assertEquals(true,executor.getVariableValue("isNumber2"));
//		assertEquals(true,executor.getVariableValue("isNumber3"));
//		assertEquals(false,executor.getVariableValue("isNumber4"));
//		assertEquals(true,executor.getVariableValue("isNumber5"));
//		assertEquals(true,executor.getVariableValue("isNumber6"));
//		assertEquals(false,executor.getVariableValue("isInteger"));
//		assertEquals(false,executor.getVariableValue("isInteger1"));
//		assertEquals(false,executor.getVariableValue("isInteger2"));
//		assertEquals(true,executor.getVariableValue("isInteger3"));
//		assertEquals(true,executor.getVariableValue("isLong"));
//		assertEquals(true,executor.getVariableValue("isDate"));
//		assertEquals(false,executor.getVariableValue("isDate1"));
//		// "kk" allows hour to be 1-24 (as opposed to HH allowing hour to be 0-23) 
//		assertEquals(true,executor.getVariableValue("isDate2"));
//		assertEquals(true,executor.getVariableValue("isDate3"));
//		assertEquals(false,executor.getVariableValue("isDate4"));
//		assertEquals(true,executor.getVariableValue("isDate5"));
//		assertEquals(true,executor.getVariableValue("isDate6"));
//		assertEquals(false,executor.getVariableValue("isDate7"));
//		// illegal month: 15
//		assertEquals(false,executor.getVariableValue("isDate9"));
//		assertEquals(false,executor.getVariableValue("isDate10"));
//		assertEquals(true,executor.getVariableValue("isDate11"));
//		assertEquals(true,executor.getVariableValue("isDate12"));
//		assertEquals(false,executor.getVariableValue("isDate13"));
//		assertEquals(true,executor.getVariableValue("isDate14"));
//		assertEquals(false,executor.getVariableValue("isDate15"));
//		// 24 is an illegal value for pattern HH (it allows only 0-23)
//		assertEquals(false,executor.getVariableValue("isDate16"));
//		// empty string in strict mode: invalid
//		assertEquals(false,executor.getVariableValue("isDate17"));
//		// empty string in lenient mode: valid
//		assertEquals(true,executor.getVariableValue("isDate18"));
//
//
//              
//    }
//
//    public void test_functions3(){
//        System.out.println("\nFunctions test:");
//        String expStr = "string test=remove_diacritic('teščik');\n" +
//						"print_err(test);\n" + 
//						"string test1=remove_diacritic('žabička');\n" +
//						"print_err(test1);\n" + 
//						"string r1=remove_blank_space(\"" + 
//						StringUtils.specCharToString(" a	b\nc\rd   e \u000Cf\r\n") + 
//						"\");\n" +
//						"print_err(r1);\n" + 
//						"string an1 = get_alphanumeric_chars(\"" +
//							StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + 
//						"\");\n" +
//						"print_err(an1);\n" + 
//							"string an2 = get_alphanumeric_chars(\"" +
//							StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + 
//						"\",true,true);\n" +
//						"print_err(an2);\n" + 
//							"string an3 = get_alphanumeric_chars(\"" +
//							StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + 
//						"\",true,false);\n" +
//						"print_err(an3);\n" + 
//							"string an4 = get_alphanumeric_chars(\"" +
//							StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + 
//						"\",false,true);\n" +
//						"print_err(an4);\n" + 
//						"string t=translate('hello','leo','pii');\n" +
//						"print_err(t);\n" + 
//						"string t1=translate('hello','leo','pi');\n" +
//						"print_err(t1);\n" + 
//						"string t2=translate('hello','leo','piims');\n" +
//						"print_err(t2);\n" + 
//						"string t3=translate('hello','hleo','');\n" +
//						"print_err(t3);\n" + 
//						"string t4=translate('my language needs the letter e', 'egms', 'X');\n" +
//						"print_err(t4);\n" + 
//						"string input='hello world';\n" +
//						"print_err(input);\n" + 
//						"int index=index_of(input,'l');\n" +
//						"print_err('index of l: ' + index);\n" + 
//						"int index1=index_of(input,'l',5);\n" +
//						"print_err('index of l since 5: ' + index1);\n" +
//						"int index2=index_of(input,'hello');\n" +
//						"print_err('index of hello: ' + index2);\n" +
//						"int index3=index_of(input,'hello',1);\n" +
//						"print_err('index of hello since 1: ' + index3);\n" +
//						"int index4=index_of(input,'world',1);\n" +
//						"print_err('index of world: ' + index4);\n";
//				        
//		TransformationGraph graph = createDefaultGraph();
//		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2) };
//		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2) };
//
//		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)) };
//
//		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)) };
//
//		print_code(expStr);
//		TLCompiler compiler = new TLCompiler(graph, inMetadata, outMetadata);
//		List<ErrorMessage> messages = compiler.validate(expStr);
//		printMessages(messages);
//
//		if (messages.size() > 0) {
//			throw new AssertionFailedError("Error in execution. Check standard output for details.");
//		}
//
//		CLVFStart parseTree = compiler.getStart();
//		parseTree.dump("");
//
//		TransformLangExecutor executor = createExecutor(compiler, graph, inputRecords, outputRecords);
//		executor.keepGlobalScope();
//		executor.init(parseTree);
//		executor.execute(parseTree);
//        
//    	assertEquals("tescik",executor.getVariableValue("test"));
//    	assertEquals("zabicka",executor.getVariableValue("test1"));
//		assertEquals("abcdef",executor.getVariableValue("r1"));
//		assertEquals("a1bcde2f",executor.getVariableValue("an1"));
//		assertEquals("a1bcde2f",executor.getVariableValue("an2"));
//		assertEquals("abcdef",executor.getVariableValue("an3"));
//		assertEquals("12",executor.getVariableValue("an4"));
//		assertEquals("hippi",executor.getVariableValue("t"));
//		assertEquals("hipp",executor.getVariableValue("t1"));
//		assertEquals("hippi",executor.getVariableValue("t2"));
//		assertEquals("",executor.getVariableValue("t3"));
//		assertEquals("y lanuaX nXXd thX lXttXr X",executor.getVariableValue("t4"));
//		assertEquals(2,executor.getVariableValue("index"));
//		assertEquals(9,executor.getVariableValue("index1"));
//		assertEquals(0,executor.getVariableValue("index2"));
//		assertEquals(-1,executor.getVariableValue("index3"));
//		assertEquals(6,executor.getVariableValue("index4"));
//
//              
//    }
//
//    public void test_functions4(){
//        System.out.println("\nFunctions test:");
//        DecimalFormat format = (DecimalFormat)NumberFormat.getInstance();
//        String expStr = 
//	    		"string stringNo='12';\n" +
//	    		"int No;\n" +
//        		"function print_result(from,to, format){\n" +
//        		"	if (isnull(format)) {\n" +
//        		"		if (try_convert(from,to)) print_err('converted:'+from+'-->'+to);\n" +
//        		"		else print_err('cant convert:'+from+'-->'+to);\n" +
//        		"	}else{\n" +
//        		"		if (try_convert(from,to, format)) print_err('converted:'+from+'-->'+to);\n" +
//        		"		else print_err('cant convert:'+from+'-->'+to+' with pattern '+format);\n" +
//        		"	}\n" +
//        		"};\n" +
//        		"print_result(stringNo,No,null);\n" +
//        		"stringNo='128a';\n" +
//        		"print_result(stringNo,No,null);\n" +
//        		"stringNo='" + format.format(1285.455) + "';\n" +
//        		"double no1=1.34;\n" +
//        		"print_result(stringNo,no1,'" + format.toPattern() + "');\n" +
//        		"decimal(10,3) no2;\n" +
//        		"print_result(no1,no2,null);\n" +
//        		"print_result(34542.3,no2,null);\n" +
//        		"int no3;\n" +
//        		"print_result(34542.7,no3,null);\n" +
//        		"print_result(345427,no3,null);\n" +
//        		"print_result(3454876434468927,no3,null);\n" +
//        		"date date1 = $Born;\n" +
//        		"long no4;\n" +
//        		"print_result(date1,no4,null);\n" +
//        		"no4 = no4 + 1000*60*60*24;\n" +
//        		"print_result(no4,date1,null);\n" +
//        		"date date2;\n" +
//        		"print_result('20.9.2007',date2,'dd.MM.yyyy');\n" +
//        		"decimal(6,4) d1=73.8474;\n" +
//        		"decimal(4,2) d2;\n" +
//        		"print_result(d1,d2,null);\n" +
//        		"d2 = 75.32;\n" +
//        		"print_result(d2,d1,null);\n" +
//        		"boolean b;\n" +
//        		"print_result(1,b,null);\n" +
//        		"print_result(b,d2,null);\n" +
//        		"boolean b1;\n" +
//        		"print_result('prawda',b1,null);\n";
//
////		      assertEquals(12,(executor.getGlobalVariable(parser.getGlobalVariableSlot("No")).getTLValue().getNumeric().getInt()));
////		      assertEquals(1285.455,(executor.getGlobalVariable(parser.getGlobalVariableSlot("no1")).getTLValue().getNumeric().getDouble()));
////		      assertEquals(DecimalFactory.getDecimal(34542.3, 10, 3),(executor.getGlobalVariable(parser.getGlobalVariableSlot("no2")).getTLValue().getNumeric()));
////		      assertEquals(345427,(executor.getGlobalVariable(parser.getGlobalVariableSlot("no3")).getTLValue().getNumeric().getInt()));
////		      today.add(Calendar.DATE, 1);
////		      assertEquals(today.getTime(),(executor.getGlobalVariable(parser.getGlobalVariableSlot("date1")).getTLValue().getDate()));
////		      assertEquals(today.getTimeInMillis(),(executor.getGlobalVariable(parser.getGlobalVariableSlot("no4")).getTLValue().getNumeric().getLong()));
////		      assertEquals(new GregorianCalendar(2007,8,20).getTime(),(executor.getGlobalVariable(parser.getGlobalVariableSlot("date2")).getTLValue().getDate()));
////		      assertEquals(DecimalFactory.getDecimal(75.32, 6, 4),(executor.getGlobalVariable(parser.getGlobalVariableSlot("d1")).getTLValue().getNumeric()));
////		      assertEquals(DecimalFactory.getDecimal(75.32, 6, 4),(executor.getGlobalVariable(parser.getGlobalVariableSlot("d1")).getTLValue().getNumeric()));
////		      assertEquals(DecimalFactory.getDecimal(1), executor.getGlobalVariable(parser.getGlobalVariableSlot("d2")).getTLValue().getNumeric());
//		      
//    }
//
//    public void test_string_functions(){
//		System.out.println("\nString functions test:");
//		String expStr = "string s1=chop(\"hello\\n\");\n" +
//						"string s6=chop(\"hello\\r\");\n" +
//						"string s5=chop(\"hello\\n\\n\");\n" +
//						"string s2=chop(\"hello\\r\\n\");\n" +
//						"string s7=chop(\"hello\\nworld\\r\\n\");\n" +
//						"string s3=chop(\"hello world\",'world');\n" +
//						"string s4=chop(\"hello world\",' world');\n"; 
////			      assertEquals("s1","hello",executor.getGlobalVariable(parser.getGlobalVariableSlot("s1")).getTLValue().getValue().toString());
////			      assertEquals("s6","hello",executor.getGlobalVariable(parser.getGlobalVariableSlot("s6")).getTLValue().getValue().toString());
////			      assertEquals("s5","hello",executor.getGlobalVariable(parser.getGlobalVariableSlot("s5")).getTLValue().getValue().toString());
////			      assertEquals("s2","hello",executor.getGlobalVariable(parser.getGlobalVariableSlot("s2")).getTLValue().getValue().toString());
////			      assertEquals("s7","hello\nworld",executor.getGlobalVariable(parser.getGlobalVariableSlot("s7")).getTLValue().getValue().toString());
////			      assertEquals("s3","hello ",executor.getGlobalVariable(parser.getGlobalVariableSlot("s3")).getTLValue().getValue().toString());
////			      assertEquals("s4)","hello",executor.getGlobalVariable(parser.getGlobalVariableSlot("s4")).getTLValue().getValue().toString());
//    }
//    
//    public void test_math_functions(){
//		System.out.println("\nMath functions test:");
//		String expStr = "number original;original=pi();\n" +
//						"print_err('pi='+original);\n" +
//						"number ee=e();\n" +
//						"number result;result=sqrt(original);\n" +
//						"print_err('sqrt='+result);\n" +
//						"int i;i=9;\n" +
//						"number p9;p9=sqrt(i);\n" +
//						"number ln;ln=log(p9);\n" +
//						"print_err('sqrt(-1)='+sqrt(-1));\n" +
//						"decimal d;d=0;\n"+
//						"print_err('log(0)='+log(d));\n" +
//						"number l10;l10=log10(p9);\n" +
//						"number ex;ex =exp(l10);\n" +
//						"number po;po=pow(p9,1.2);\n" +
//						"number p;p=pow(-10,-0.3);\n" +
//						"print_err('power(-10,-0.3)='+p);\n" +
//						"int r;r=round(-po);\n" +
//						"print_err('round of '+(-po)+'='+r);\n"+
//						"int t;t=trunc(-po);\n" +
//						"print_err('truncation of '+(-po)+'='+t);\n" +
//						"date date1;date1=2004-01-02 17:13:20;\n" +
//						"date tdate1; tdate1=trunc(date1);\n" +
//						"print_err('truncation of '+date1+'='+tdate1);\n" +
//						"print_err('Random number: '+random());\n";
//
//			
////FIXME: enable		      
////		      assertEquals("pi",Double.valueOf(Math.PI),executor.getGlobalVariable(parser.getGlobalVariableSlot("original")).getTLValue().getNumeric().getDouble());
////		      assertEquals("e",Double.valueOf(Math.E),executor.getGlobalVariable(parser.getGlobalVariableSlot("ee")).getTLValue().getNumeric().getDouble());
////		      assertEquals("sqrt",Double.valueOf(Math.sqrt(Math.PI)),executor.getGlobalVariable(parser.getGlobalVariableSlot("result")).getTLValue().getNumeric().getDouble());
////		      assertEquals("sqrt(9)",Double.valueOf(3),executor.getGlobalVariable(parser.getGlobalVariableSlot("p9")).getTLValue().getNumeric().getDouble());
////		      assertEquals("ln",Double.valueOf(Math.log(3)),executor.getGlobalVariable(parser.getGlobalVariableSlot("ln")).getTLValue().getNumeric().getDouble());
////		      assertEquals("log10",Double.valueOf(Math.log10(3)),executor.getGlobalVariable(parser.getGlobalVariableSlot("l10")).getTLValue().getNumeric().getDouble());
////		      assertEquals("exp",Double.valueOf(Math.exp(Math.log10(3))),executor.getGlobalVariable(parser.getGlobalVariableSlot("ex")).getTLValue().getNumeric().getDouble());
////		      assertEquals("power",Double.valueOf(Math.pow(3,1.2)),executor.getGlobalVariable(parser.getGlobalVariableSlot("po")).getTLValue().getNumeric().getDouble());
////		      assertEquals("power--",Double.valueOf(Math.pow(-10,-0.3)),executor.getGlobalVariable(parser.getGlobalVariableSlot("p")).getTLValue().getNumeric().getDouble());
////		      assertEquals("round",Integer.parseInt("-4"),executor.getGlobalVariable(parser.getGlobalVariableSlot("r")).getTLValue().getNumeric().getInt());
////		      assertEquals("truncation",Integer.parseInt("-3"),executor.getGlobalVariable(parser.getGlobalVariableSlot("t")).getTLValue().getNumeric().getInt());
////		      assertEquals("date truncation",new GregorianCalendar(2004,00,02).getTime(),executor.getGlobalVariable(parser.getGlobalVariableSlot("tdate1")).getTLValue().getDate());
////		      
//	}


	public void test_num2str_function(){
		System.out.println("num2str() test:");
		doCompile("test_num2str_function");
		
		check("intOutput", createList("16","10000","20","10"));
		check("longOutput", createList("16","10000","20","10"));
		check("doubleOutput", createList("16.16","0x1.028f5c28f5c29p4"));
		check("decimalOutput", createList("16.16"));
	}

	public void test_pow_function(){
		System.out.println("pow() test:");
		doCompile("test_pow_function");
	
		
	}
    
	public void test_round_function() {
		System.out.println("round() test:");
		doCompile("test_round_function");
	}
	
	public void test_length_function() {
		System.out.println("length() test:");
		doCompile("test_length_function");
	}
	

	private static final int NORMALIZE_RETURN_OK = 0;
		
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
		doCompileExpectError("test_case_unique_check", Arrays.asList("Duplicate case", "Duplicate case"));
	}
	
	public void test_case_unique_check2() {
		// test case for issue 2515
		doCompileExpectError("test_case_unique_check2", Arrays.asList("Duplicate case", "Duplicate case"));
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
	
	public void test_list_concatenate() {
		// test case for issue 
		doCompile("test_list_concatenate");
	}

	
	
}
