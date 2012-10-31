/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.ctl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.ctl.ASTnode.CLVFStart;
import org.jetel.ctl.ASTnode.CLVFStartExpression;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.extensions.TLFunctionCallContext;
import org.jetel.ctl.extensions.TLFunctionPluginRepository;
import org.jetel.data.Defaults;
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

	protected TransformationGraph graph; /* may be null */
	protected DataRecordMetadata[] inMetadata; /* may be null */
	protected DataRecordMetadata[] outMetadata; /* may be null */
	protected String encoding;
	protected TransformLangParser parser;
	protected ProblemReporter problemReporter;
	protected SimpleNode ast;
	protected int tabSize = 6;
	protected Log logger;
	protected String componentId;
	private List<TLFunctionCallContext> functionContexts;

	
	/**
	 * Constructs TLCompiler to run graph-less, in standalone mode.
	 * 
	 * This constructor should be only used by clients wishing to interpret simple CTL
	 * expression (i.e. no compilation), as the compilation will complain about unresolved 
	 * metadata, lookups and sequences due to missing graph reference.
	 * 
	 */
	public TLCompiler() {
		this(null,null,null);
	}
	
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
		this(graph,inMetadata,outMetadata,Defaults.DEFAULT_SOURCE_CODE_CHARSET);
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
		this.logger = LogFactory.getLog(TLCompiler.class);
	}
	
	/**
	 * @param inMetadata the inMetadata to set
	 */
	public void setInMetadata(DataRecordMetadata[] inMetadata) {
		this.inMetadata = inMetadata;
	}

	/**
	 * Validate given (Filter) expression. 
	 * Result of this method is identical to {@link #validate(InputStream)} with encoding of this compiler
	 * @param code
	 * @return list of error messages (empty when no errors)
	 */
	@Override
	public List<ErrorMessage> validateExpression(String code) {
		try {
			return validateExpression(new ByteArrayInputStream(code.getBytes(encoding)));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(encoding + " encoding not availabe for conversion");
		}
	}
	
	
	/**
	 * Validate given (Filter) expression stored in the InputStream
	 * @param input
	 * @return list of error messages (empty when no errors)
	 */
	@Override
	public List<ErrorMessage> validateExpression(InputStream input) {
		if (parser == null) {
			parser = new TransformLangParser(graph, problemReporter, input, encoding);
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
		
		functionContexts = typeChecker.getFunctionCalls();
		
		return getDiagnosticMessages();
	}
	
	/**
	 * Validate complex CTL code
	 * @param code
	 * @return	list of error messages (empty when no errors)
	 */
	@Override
	public List<ErrorMessage> validate(String code) {
		try {
			return validate(new ByteArrayInputStream(code.getBytes(encoding)));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(encoding+" encoding not available for conversion");
		}
	}
	
	/**
	 * Validate complex CTL code stored in the input stream.
	 * Can be called repeatedly with different input stream.
	 * @param input
	 * @return list of error messages (empty when no errors)
	 */
	@Override
	public List<ErrorMessage> validate(InputStream input) {
		if (parser == null) {
			parser = new TransformLangParser(graph, problemReporter, input, encoding);
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
		
		functionContexts = typeChecker.getFunctionCalls();
		
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
	 * with encoding of this component
	 */
	@Override
	public List<ErrorMessage> compile(String code, Class<?> targetInterface, String componentId) {
		try {
			return compile(new ByteArrayInputStream(code.getBytes(encoding)), targetInterface, componentId);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(encoding + " encoding not availabe for conversion");
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
	@Override
	public List<ErrorMessage> compileExpression(String code, Class<?> targetInterface, String componentId, String syntheticFunctionName, Class<?> syntheticReturnType) {
		final String wrappedCode = wrapExpression(code,syntheticFunctionName, syntheticReturnType);
		logger.trace("Component '" + componentId + "' uses CTL expression. Creating synthetic function '" + syntheticFunctionName + "'");
		logger.trace(wrappedCode);
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

	@Override
	public String convertToJava(String ctlCode, Class<?> targetInterface, String componentId) throws ErrorMessageException {
		throw new UnsupportedOperationException();
	}

	/**
	 * @return Instance of {@link TransformLangExecutor} that can be used to interpret the CTL code.
	 * 			This method calls {@link TransformLangExecutor#init()} automatically.  
	 * 
	 */
	@Override
	public Object getCompiledCode() {
		final TransformLangExecutor executor = new TransformLangExecutor(parser,graph);
		if (this.ast instanceof CLVFStart ) {
			executor.setAst((CLVFStart)ast);
		} else {
			executor.setAst((CLVFStartExpression)ast);
		}
		
		// compiler can be started standalone (i.e. from PropertyRefResolver without component reference)
		// in that case do not report the execution mode
		if (logger != null && getComponentId() != null) {
			logger.debug("Component '" + getComponentId() + "' runs in INTERPRETED mode"); 
		}
		
		// perform initialization of the executor for runtime
		executor.setRuntimeLogger(logger);
		executor.init();
		
		return executor;
	}
	
	/** 
	 * Sets tabulator size (in characters) for the parser to correctly calculate
	 * error position.
	 */
	@Override
	public void setTabSize(int size) {
		this.tabSize = size;
	}


	/**
	 * @return	Expression AST root created during {@link #validateExpression(InputStream)}
	 */
	@Override
	public CLVFStartExpression getExpression() {
		return (CLVFStartExpression)ast;
	}
	
	/**
	 * @return	AST root created during {@link #validate(InputStream)}
	 */
	@Override
	public CLVFStart getStart() {
		return (CLVFStart)ast;
	}


	/**
	 * @return Number of critical errors from the last validate call.
	 * 
	 */
	@Override
	public int errorCount() {
		return problemReporter.errorCount();
	}

	/**
	 * @return List of errors/warnings from the last validate call.
	 */
	@Override
	public List<ErrorMessage> getDiagnosticMessages() {
		return problemReporter.getDiagnosticMessages();
	}


	@Override
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
	
	protected List<TLFunctionCallContext> getFunctionContexts() {
		return functionContexts; 
	}
	
	public DataRecordMetadata[] getInputMetadata() {
		return inMetadata;
	}
	
	public DataRecordMetadata[] getOutputMetadata() {
		return outMetadata;
	}

}
