package org.jetel.ctl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.jetel.component.WrapperTL;
import org.jetel.ctl.ASTnode.CLVFStart;
import org.jetel.ctl.ASTnode.CLVFStartExpression;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.extensions.TLFunctionPluginRepository;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * CTL frontend of compiler. It performs parsing, semantic pass, type checking
 * and flow control.
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 *
 */
public class TLCompiler implements ITLCompiler {

	protected TransformationGraph graph;
	protected DataRecordMetadata[] inMetadata;
	protected DataRecordMetadata[] outMetadata;
	protected String encoding;
	protected TransformLangParser parser;
	protected ProblemReporter problemReporter;
	protected SimpleNode ast;
	protected int tabSize = 6;
	protected Log logger;
	protected String componentId;

	
	/**
	 * Creates reusable CTL compiler instance. 
	 * This is identical to {@link #TLCompiler(TransformationGraph, DataRecordMetadata[], DataRecordMetadata[], String)}
	 * with <code>encoding="UTF-8"</code>
	 * 
	 * @param graph			Graph to validate against (must not be null)
	 * @param inMetadata	Component's input metadata
	 * @param outMetadata	Component's output metadata
	 */
	public TLCompiler(TransformationGraph graph, DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata) {
		this(graph,inMetadata,outMetadata,"UTF-8");
	}
	
	
	/**
	 * Creates reusable CTL compiler instance using specified encoding for parsing
	 * @param graph			Graph to validate against (must not be null)
	 * @param inMetadata	Component's input metadata
	 * @param outMetadata	Component's output metadata
	 * @param encoding		Encoding to use when reading from the input stream
	 */
	public TLCompiler(TransformationGraph graph, DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata, String encoding) {
		this.graph = graph;
		this.inMetadata = inMetadata;
		this.outMetadata = outMetadata;
		this.encoding = encoding;
		this.problemReporter = new ProblemReporter();
		this.logger = graph.getLogger();
	}
	
	
	/**
	 * Validate given (Filter) expression. 
	 * Result of this method is identical to {@link #validate(InputStream)} with UTF-8 encoding
	 * @param code
	 * @return list of error messages (empty when no errors)
	 */
	public List<ErrorMessage> validateExpression(String code) {
		try {
			return validateExpression(new ByteArrayInputStream(code.getBytes("UTF-8")));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("UTF-8 encoding not availabe for conversion");
		}
	}
	
	
	/**
	 * Validate given (Filter) expression stored in the InputStream
	 * @param input
	 * @return list of error messages (empty when no errors)
	 */
	public List<ErrorMessage> validateExpression(InputStream input) {
		if (parser == null) {
			parser = new TransformLangParser(problemReporter, input, encoding);
		} else {
			reset(input);
		}
	
		parser.setTabSize(tabSize);
		parser.enable_tracing();
		CLVFStartExpression parseTree = null;
		try {
			ast = parseTree = parser.StartExpression();
			if (problemReporter.errorCount() > 0) {
				return getDiagnosticMessages();
			}
		} catch (ParseException e) {
			problemReporter.error(1, 1, 1, 2, e.getMessage(), null);
			return getDiagnosticMessages();
		}
		
		ASTBuilder astBuilder = new ASTBuilder(graph,inMetadata,outMetadata,parser.getFunctions(),problemReporter);
		astBuilder.resolveAST(parseTree);
		if (problemReporter.errorCount() > 0) {
			return getDiagnosticMessages();
		}
		
		TypeChecker typeChecker = new TypeChecker(problemReporter,parser.getFunctions(), TLFunctionPluginRepository.getAllFunctions());
		typeChecker.check(parseTree);
		if (problemReporter.errorCount() > 0) {
			return getDiagnosticMessages();
		}
		
		return getDiagnosticMessages();
	}
	
	/**
	 * Validate complex CTL code
	 * @param code
	 * @return	list of error messages (empty when no errors)
	 */
	public List<ErrorMessage> validate(String code) {
		try {
			return validate(new ByteArrayInputStream(code.getBytes("UTF-8")));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("UTF-8 encoding not available for conversion");
		}
	}
	
	/**
	 * Validate complex CTL code stored in the input stream.
	 * Can be called repeatedly with different input stream.
	 * @param input
	 * @return list of error messages (empty when no errors)
	 */
	public List<ErrorMessage> validate(InputStream input) {
		if (parser == null) {
			parser = new TransformLangParser(problemReporter, input, encoding);
		} else {
			reset(input);
		}
		
		parser.setTabSize(tabSize);
		CLVFStart parseTree = null;
		try {
			ast = parseTree = parser.Start();  
			if (problemReporter.errorCount() > 0) {
				return getDiagnosticMessages();
			}
		} catch (ParseException e) {
			problemReporter.error(1, 1, 1, 2, e.getMessage(),null);
			return getDiagnosticMessages();
		}
		
		ASTBuilder astBuilder = new ASTBuilder(graph,inMetadata,outMetadata,parser.getFunctions(),problemReporter);
		astBuilder.resolveAST(parseTree);
		if (problemReporter.errorCount() > 0) {
			return getDiagnosticMessages();
		}
		
		TypeChecker typeChecker = new TypeChecker(problemReporter,parser.getFunctions(),TLFunctionPluginRepository.getAllFunctions());
		typeChecker.check(parseTree);
		if (problemReporter.errorCount() > 0) {
			return getDiagnosticMessages();
		}
		
		FlowControl flowControl = new FlowControl(problemReporter);
		flowControl.check(parseTree);
		if (problemReporter.errorCount() > 0) {
			return getDiagnosticMessages();
		}
		
		return getDiagnosticMessages();
		
	}
	
	/**
	 * Compiles the code into target interface.
	 * The result is identical to calling {@link #compile(InputStream, Class)} 
	 * with encoding=UTF-8
	 */
	public List<ErrorMessage> compile(String code, Class<?> targetInterface, String componentId) {
		try {
			return compile(new ByteArrayInputStream(code.getBytes("UTF-8")), targetInterface, componentId);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("UTF-8 encoding not availabe for conversion");
		}
	}
	
	/**
	 * Compiles the code into target interface.
	 * Encoding of input must match to the encoding specified when TLCompiler was created.
	 * 
	 * @param input
	 * @param targetInterface
	 * @return
	 */
	public List<ErrorMessage> compile(InputStream input, Class<?> targetInterface, String componentId) {
		setComponentId(componentId);
		validate(input);
		return getDiagnosticMessages();
	}

	
	
	/**
	 * Compiles CTL expression by wrapping it into synthetic function with specified name and return type,
	 * then compiles the resulting code by calling {@link #compile(String, Class, String)}.
	 * 
	 * @param code	CTL expression
	 * @param targetInterface	Java interface into which the code should be compiled.
	 * @param componentId	Identifier of calling component (will become part of Java class name)
	 * @param syntheticFunctionName	Name of synthetic function to create.
	 * @param syntheticReturnType	Expected type of expression (as well as return type of synthetic function).
	 * @return Compilation error messages as return by {@link #compile(String, Class, String)}.
	 */
	public List<ErrorMessage> compileExpression(String code, Class<?> targetInterface, String componentId, String syntheticFunctionName, Class<?> syntheticReturnType) {
		final String wrappedCode = wrapExpression(code,syntheticFunctionName, syntheticReturnType);
		logger.info("Component '" + componentId + "' uses CTL expression. Creating synthetic function '" + syntheticFunctionName + "'");
		logger.info(wrappedCode);
		return compile(wrappedCode,targetInterface,componentId);
	}
	
	/**
	 * Wraps CTL expression into synthetic CTL function with specified name and return type.
	 * 
	 * @param expression	CTL expression to wrap
	 * @param syntheticFunctionName	Name of synthetic function
	 * @param returnType	Type of CTL expression as well as expected return type of synthetic function.
	 * @return	CTL expression wrapped into function evaluating it.
	 */
	protected String wrapExpression(String expression, String syntheticFunctionName, Class<?> returnType) {
		// compute return type
		final TLType type = TLType.fromJavaType(returnType);
		return "function " + type.name() +  " " + syntheticFunctionName + "() { " +
				"return " + expression + ";" +
				" }";
	}
	
	/**
	 * @return instance of <code>targetInterface</code> specified in {@link #compile(String, Class)}
	 * or an instance of {@link WrapperTL}
	 */
	public Object getCompiledCode() {
		final TransformLangExecutor executor = new TransformLangExecutor(parser,graph);
		if (this.ast instanceof CLVFStart ) {
			executor.setAst((CLVFStart)ast);
		} else {
			executor.setAst((CLVFStartExpression)ast);
		}
		
		if (logger != null) {
			logger.info("Component '" + getComponentId() + "' runs in INTERPRETED mode"); 
		}
			
		executor.setRuntimeLogger(graph.getLogger());
		return executor;
	}
	
	/** 
	 * Sets tabulator size (in characters) for the parser to correctly calculate
	 * error position.
	 */
	public void setTabSize(int size) {
		this.tabSize = size;
	}


	/**
	 * @return	Expression AST root created during {@link #validateExpression(InputStream)}
	 */
	public CLVFStartExpression getExpression() {
		return (CLVFStartExpression)ast;
	}
	
	/**
	 * @return	AST root created during {@link #validate(InputStream)}
	 */
	public CLVFStart getStart() {
		return (CLVFStart)ast;
	}


	/**
	 * @return Number of critical errors from the last validate call.
	 * 
	 */
	public int errorCount() {
		return problemReporter.errorCount();
	}

	/**
	 * @return List of errors/warnings from the last validate call.
	 */
	public List<ErrorMessage> getDiagnosticMessages() {
		return problemReporter.getDiagnosticMessages();
	}


	public int warningCount() {
		return problemReporter.warningCount();
	}
	
	private void reset(InputStream input) {
		parser.reset(input);
		problemReporter.reset();
		ast = null;
	}
	
	protected String getComponentId() {
		return componentId;
	}
	
	protected void setComponentId(String componentId) {
		this.componentId = componentId;
	}
	
	
}
