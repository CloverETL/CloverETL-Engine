package org.jetel.ctl;

import java.util.List;

import junit.framework.AssertionFailedError;

import org.jetel.component.CTLRecordTransform;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.data.DataRecord;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

public class InterpreterTest extends CompilerTestCase {

	public InterpreterTest() {
		super(false);
	}
	
	private TransformLangExecutor executor;
	
	@Override
	public void executeCode(ITLCompiler compiler) {
		executor = (TransformLangExecutor)compiler.getCompiledCode();
		
		executor.setNode(node);
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
	
	@Override
	protected Object getVariable(String varName) {
		Object result = executor.getVariableValue(varName);
		if (result instanceof CharSequence) {
			return result.toString();
		}
		return result;
	}
	
	// This method tests fix of issue 4140. Copying failed in case the input data records were swapped.
	public void test_dataFieldCopy() {
		String expStr = 
			"function integer transform() {\n" + 
				"$0.Name = $0.Name;\n" + 
				"return 0;\n" + 
			"}\n";

		TransformationGraph graph = createDefaultGraph();
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(INPUT_1), graph.getDataRecordMetadata(INPUT_2), graph.getDataRecordMetadata(INPUT_3) };
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { graph.getDataRecordMetadata(OUTPUT_1), graph.getDataRecordMetadata(OUTPUT_2), graph.getDataRecordMetadata(OUTPUT_3) };

		print_code(expStr);

		ITLCompiler compiler = TLCompilerFactory.createCompiler(graph, inMetadata, outMetadata, "UTF-8");
		List<ErrorMessage> messages = compiler.compile(expStr, CTLRecordTransform.class, "test_dataFieldCopy");
		printMessages(messages);
		if (compiler.errorCount() > 0) {
			throw new AssertionFailedError("Error in execution. Check standard output for details.");
		}

		DataRecord[] inputRecords = new DataRecord[] { createDefaultRecord(graph.getDataRecordMetadata(INPUT_1)), createDefaultRecord(graph.getDataRecordMetadata(INPUT_2)), createEmptyRecord(graph.getDataRecordMetadata(INPUT_3)) };
		DataRecord[] outputRecords = new DataRecord[] { createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_1)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_2)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_3)), createEmptyRecord(graph.getDataRecordMetadata(OUTPUT_4)) };

		executor = (TransformLangExecutor) compiler.getCompiledCode();

		executor.setRuntimeLogger(graph.getLogger());

		executor.keepGlobalScope();
		executor.init();
		executor.execute();
		CLVFFunctionDeclaration transform = executor.getFunction("transform");
		if (transform == null) {
			System.err.println("Function 'transform' not found and will not be executed");
			return;
		}

		for (int i = 0; i < inputRecords.length; i++) {
			inputRecords[i].getField("Name").setValue(Integer.toString(i + 1));
		}

		// execute the transform() function multiple times for different input data records
		for (int i = 0; i < inputRecords.length; i++) {
			executor.setInputRecords(new DataRecord[] { inputRecords[i] });
			executor.setOutputRecords(new DataRecord[] { outputRecords[i] });
			executor.executeFunction(transform, new Object[0]);

			if (!inputRecords[i].getField("Name").equals(outputRecords[i].getField("Name"))) {
				fail("Field copy failed!");
			}
		}
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		//FIXME: fix for memory leak - find that memory leak :-)
		executor = null;
	}
}
