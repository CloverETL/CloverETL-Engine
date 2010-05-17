package org.jetel.ctl;

import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.data.DataRecord;
import org.jetel.graph.TransformationGraph;

public class InterpreterTest extends CompilerTestCase {

	public InterpreterTest() {
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
	
	protected Object getVariable(String varName) {
		return executor.getVariableValue(varName);
	}
}
