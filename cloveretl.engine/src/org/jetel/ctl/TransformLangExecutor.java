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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.ctl.ASTnode.CLVFAddNode;
import org.jetel.ctl.ASTnode.CLVFAnd;
import org.jetel.ctl.ASTnode.CLVFArguments;
import org.jetel.ctl.ASTnode.CLVFArrayAccessExpression;
import org.jetel.ctl.ASTnode.CLVFAssignment;
import org.jetel.ctl.ASTnode.CLVFBlock;
import org.jetel.ctl.ASTnode.CLVFBreakStatement;
import org.jetel.ctl.ASTnode.CLVFBreakpointNode;
import org.jetel.ctl.ASTnode.CLVFCaseStatement;
import org.jetel.ctl.ASTnode.CLVFComparison;
import org.jetel.ctl.ASTnode.CLVFConditionalExpression;
import org.jetel.ctl.ASTnode.CLVFContinueStatement;
import org.jetel.ctl.ASTnode.CLVFDateField;
import org.jetel.ctl.ASTnode.CLVFDeleteDictNode;
import org.jetel.ctl.ASTnode.CLVFDivNode;
import org.jetel.ctl.ASTnode.CLVFDoStatement;
import org.jetel.ctl.ASTnode.CLVFEvalNode;
import org.jetel.ctl.ASTnode.CLVFFieldAccessExpression;
import org.jetel.ctl.ASTnode.CLVFForStatement;
import org.jetel.ctl.ASTnode.CLVFForeachStatement;
import org.jetel.ctl.ASTnode.CLVFFunctionCall;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.ASTnode.CLVFIIfNode;
import org.jetel.ctl.ASTnode.CLVFIdentifier;
import org.jetel.ctl.ASTnode.CLVFIfStatement;
import org.jetel.ctl.ASTnode.CLVFImportSource;
import org.jetel.ctl.ASTnode.CLVFInFunction;
import org.jetel.ctl.ASTnode.CLVFIsNullNode;
import org.jetel.ctl.ASTnode.CLVFListOfLiterals;
import org.jetel.ctl.ASTnode.CLVFLiteral;
import org.jetel.ctl.ASTnode.CLVFLogLevel;
import org.jetel.ctl.ASTnode.CLVFLookupNode;
import org.jetel.ctl.ASTnode.CLVFMemberAccessExpression;
import org.jetel.ctl.ASTnode.CLVFModNode;
import org.jetel.ctl.ASTnode.CLVFMulNode;
import org.jetel.ctl.ASTnode.CLVFNVL2Node;
import org.jetel.ctl.ASTnode.CLVFNVLNode;
import org.jetel.ctl.ASTnode.CLVFOr;
import org.jetel.ctl.ASTnode.CLVFParameters;
import org.jetel.ctl.ASTnode.CLVFPostfixExpression;
import org.jetel.ctl.ASTnode.CLVFPrintErrNode;
import org.jetel.ctl.ASTnode.CLVFPrintLogNode;
import org.jetel.ctl.ASTnode.CLVFPrintStackNode;
import org.jetel.ctl.ASTnode.CLVFRaiseErrorNode;
import org.jetel.ctl.ASTnode.CLVFReadDictNode;
import org.jetel.ctl.ASTnode.CLVFReturnStatement;
import org.jetel.ctl.ASTnode.CLVFSequenceNode;
import org.jetel.ctl.ASTnode.CLVFStart;
import org.jetel.ctl.ASTnode.CLVFStartExpression;
import org.jetel.ctl.ASTnode.CLVFSubNode;
import org.jetel.ctl.ASTnode.CLVFSwitchStatement;
import org.jetel.ctl.ASTnode.CLVFType;
import org.jetel.ctl.ASTnode.CLVFUnaryExpression;
import org.jetel.ctl.ASTnode.CLVFVariableDeclaration;
import org.jetel.ctl.ASTnode.CLVFWhileStatement;
import org.jetel.ctl.ASTnode.CLVFWriteDictNode;
import org.jetel.ctl.ASTnode.CastNode;
import org.jetel.ctl.ASTnode.Node;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.ctl.data.LogLevelEnum;
import org.jetel.ctl.data.Scope;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.ctl.data.TLType.TLDateField;
import org.jetel.ctl.data.TLType.TLLogLevel;
import org.jetel.ctl.data.TLType.TLTypeRecord;
import org.jetel.ctl.extensions.TLFunctionPrototype;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DecimalDataField;
import org.jetel.data.NullRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.StringDataField;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.CharSequenceReader;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * CTL interpreter implementation. It represents a simple stack-machine performing
 * computation as it traverses the AST tree. 
 * 
 * @author David Pavlis <david.pavlis@centrum.cz>,
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 *
 */
public class TransformLangExecutor implements TransformLangParserVisitor, TransformLangParserConstants {

	/**
	 * Magic header to recognize CTL code
	 */
	public static final String CTL_TRANSFORM_CODE_ID = "//#CTL2";
	
	/**
	 *  Limits the precision of result to #DECIMAL_MAX_PRECISION digits
	 *  This applies in the following cases:
	 *  <ul>
	 *  <li>decimal division</li>
	 *  <li>assigning double to decimal</li>
	 *  </ul>
	 */
	public static final int DECIMAL_MAX_PRECISION = 32;
	public static final MathContext MAX_PRECISION = new MathContext(DECIMAL_MAX_PRECISION,RoundingMode.DOWN);
	
	
	public static final int BREAK_BREAK = 1;
	public static final int BREAK_CONTINUE = 2;
	public static final int BREAK_RETURN = 3;

	/** Stack of partial computation values */
	protected Stack stack;

	/** When true, Indicates that a break statement was executed  */
	protected boolean breakFlag;
	
	/** Type of break statement */
	protected int breakType;
	
	/** Parameters: not used now */
	protected Properties globalParameters;

	/** Input records */
	protected DataRecord[] inputRecords;
	/** Output records: targets of $record = ... */
	protected DataRecord[] outputRecords;

	/** Instance of running transformation graph where code executes */
	protected TransformationGraph graph;
	
	protected Log runtimeLogger;
	
	protected TransformLangParser parser;
	
	/** Array of lookups shared by all lookup operations */
	private Lookup[] lookups;

	static Log logger = LogFactory.getLog(TransformLangExecutor.class);

	private SimpleNode /* CLVFStart or CLVFStartExpression */ ast;
	private boolean keepGlobalScope;
	private Object lastReturnValue = null;
	
	/**
	 * Allocates runtime data structures within the AST tree necessary for execution
	 * 
	 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
	 */
	private class InterpretedRuntimeInitializer extends NavigatingVisitor {
		
		private final ArrayList<Lookup> collectedLookups = new ArrayList<Lookup>();
		private final ArrayList<DataRecord> collectedRecord = new ArrayList<DataRecord>();
		private int lookupCounter = 0;
		private HashMap<String,Integer> lkpNameToIndex = new HashMap<String,Integer>();
		
		
		/**
		 * Entry method for initialization
		 * @param tree	root of the AST tree to initialize (CLVFStart or CLVFStartExpression)
		 * @throws TransformLangExecutorRuntimeException when initialization fails for any reason
		 */
		public void initialize(SimpleNode tree) throws TransformLangExecutorRuntimeException  {
			tree.jjtAccept(this, null);
			
			// save collected lookups to executor field
			lookups = collectedLookups.toArray(new Lookup[collectedLookups.size()]);
		}
		
		
		/**
		 * Compute fast-access indexes for LookupNodes to quickly access their lookups.
		 * Single lookup is shared among all LookupNodes with the same index (i.e. accessing the same lookup table).
		 * Also allocates lookup record. Again the same instance of record is used for all nodes having the same lookup (i.e. the same lookup table).
		 * The records however may be replaced during runtime in case user uses lookup(LKP).init() call.
		 */
		@Override
		public Object visit(CLVFLookupNode node, Object data) {
			super.visit(node, data);
			
			int index = -1;
			DataRecord keyRecord = null;
			
			if (lkpNameToIndex.containsKey(node.getLookupName())) {
				// we already know this table -> we have index and lookup record allocated
				index = lkpNameToIndex.get(node.getLookupName());
				keyRecord = collectedRecord.get(index);
			} else {
				index = lookupCounter;
				lkpNameToIndex.put(node.getLookupName(),index);	
				lookupCounter += 1;
				
				// unknown lookup table -> let's compute the lookup record
				Object[] ret = createLookupRecord((CLVFArguments)node.jjtGetChild(0),
						"_lookupRecord_" + node.getLookupName() + "_" + index, node.getDecimalPrecisions());
				final int[] keyFields = (int[])ret[0];
				final DataRecordMetadata keyRecordMetadata = (DataRecordMetadata)ret[1];
				try {
					// no need to call LookupTable.init() as it was already called in AST building
					// phase to receive information about key fields
					keyRecord = new DataRecord(keyRecordMetadata);
					keyRecord.init();
					
					collectedLookups.add(node.getLookupTable().createLookup(new RecordKey(keyFields,keyRecordMetadata),keyRecord));
					collectedRecord.add(keyRecord);
					// create a reusable record that will be used in all queries
				} catch (ComponentNotReadyException e) {
					throw new TransformLangExecutorRuntimeException("Unable to initialize lookup table for execution", e);
				}
			}
			
			node.setLookupIndex(index);
			node.setLookupRecord(keyRecord);
			
			return data;
		}
		
		@Override
		/**
		 * Creates executable proxies for external functions
		 */
		public Object visit(CLVFFunctionCall node, Object data) {
			super.visit(node, data);
			
			
			try {
				if (node.isExternal()) {
					TLFunctionPrototype executable = node.getExternalFunction().getExecutable();
					node.setExecutable(executable);
					executable.init(node.getFunctionCallContext());
				}
			} catch (IllegalArgumentException e) {
				throw new TransformLangExecutorRuntimeException("Interpreter intialization failed",e);
			}
			
			return data;
		}
		
		
	}
	
	
	/**
	 * Constructor
	 */
	public TransformLangExecutor(TransformLangParser parser, TransformationGraph graph, Properties globalParameters) {
		this.globalParameters = globalParameters;
		this.parser = parser;
		this.graph = graph;
		stack = new Stack();
		breakFlag = false;
	}
	
	public TransformLangExecutor(TransformLangParser parser, TransformationGraph graph) {
		this(parser,graph,null);
	}

	public TransformationGraph getGraph() {
		return graph;
	}

	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	public Log getRuntimeLogger() {
		return runtimeLogger;
	}

	public void setRuntimeLogger(Log runtimeLogger) {
		this.runtimeLogger = runtimeLogger;
	}

	/**
	 * Set input data records for processing.<br>
	 * Referenced input data fields will be resolved from these data records.
	 * 
	 * @param inputRecords
	 *            array of input data records carrying values
	 */
	@SuppressWarnings(value = "EI2")
	public void setInputRecords(DataRecord[] inputRecords) {
		this.inputRecords = inputRecords;
		for (DataRecord record : this.inputRecords) {
			if (record == null)
				record = NullRecord.NULL_RECORD;
		}
	}

	/**
	 * Set output data records for processing.<br>
	 * Referenced output data fields will be resolved from these data records - assignment (in code) to output data
	 * field will result in assignment to one of these data records.
	 * 
	 * @param outputRecords
	 *            array of output data records for setting values
	 */
	@SuppressWarnings(value = "EI2")
	public void setOutputRecords(DataRecord[] outputRecords) {
		this.outputRecords = outputRecords;
	}

	/**
	 * Set global parameters which may be reference from within the transformation source code
	 * 
	 * @param parameters
	 */
	public void setGlobalParameters(Properties parameters) {
		this.globalParameters = parameters;
	}

	/**
	 * Method which returns result of executing parse tree.<br>
	 * Basically, it returns whatever object was left on top of executor's stack (usually as a result of last executed
	 * expression/operation).<br>
	 * It can be called repetitively in order to read all objects from stack.
	 * 
	 * @return Object saved on stack or NULL if no more objects are available
	 */
	public Object getResult() {
		return stack.pop();
	}

	/**
	 * Returns value of variable defined in the <em>global</em> scope 
	 * 
	 * @param varSlot
	 * @return value of given variable
	 * @since 6.12.2006
	 */
	public Object getVariableValue(String variableName) {
		Scope globalScope = null;
		if (this.ast instanceof CLVFStart) {
			globalScope = ((CLVFStart)ast).getScope();
		} else {
			globalScope = ((CLVFStartExpression)ast).getScope();
		}
		
		final CLVFVariableDeclaration vd = globalScope.get(variableName);
		if (vd == null) {
			throw new IllegalArgumentException("Variable '" + variableName + "' not found/declared");
		}
		
		return this.stack.getGlobalVariables()[vd.getVariableOffset()];
	}
	
	public void setParser(TransformLangParser parser) {
		this.parser = parser;
	}
	
	/**
	 * Contract: {@link #init()} method must be called prior to calling any of the {@link #execute()} methods
	 */
	public void init() {
		initInternal(this.ast);
	}
	
	/**
	 * Contract: {@link #init()} method must be called prior to calling any of the {@link #execute()} methods
	 */
	public void init(CLVFStart ast) {
		initInternal(ast);
	}
	
	/**
	 * Contract: {@link #init()} method must be called prior to calling any of the {@link #execute()} methods
	 */
	public void init(CLVFStartExpression ast) {
		initInternal(ast);
	}
	
	private void initInternal(SimpleNode ast) throws TransformLangExecutorRuntimeException {
		if (ast == null) {
			throw new TransformLangExecutorRuntimeException("AST tree to initialize is null");
		}
		
		this.ast = ast;
		InterpretedRuntimeInitializer init = new InterpretedRuntimeInitializer();
		init.initialize(ast);
	}
	
	
	/**
	 * Causes interpreter to keep the global scope.
	 * Use for debugging purposes when investigating variable values
	 */
	public void keepGlobalScope() {
		this.keepGlobalScope = true;
	}
	
	/**
	 * Contract: {@link #init()} method must be called prior to calling any of the {@link #execute()} methods
	 */
	public void execute() {
		executeInternal(this.ast);
	}
	
	/**
	 * Contract: {@link #init()} method must be called prior to calling any of the {@link #execute()} methods
	 */
	public void execute(CLVFStart ast) {
		executeInternal(ast);
	}
	
	/**
	 * Contract: {@link #init()} method must be called prior to calling any of the {@link #execute()} methods
	 */
	public void execute(CLVFStartExpression ast) {
		executeInternal(ast);
	}
	
	/**
	 * Execute statements (returning no value)
	 * @param node
	 */
	private void executeInternal(SimpleNode node) {
		this.ast = node;
		node.jjtAccept(this, null);
	}
	
	/**
	 * Execute a CTL expression
	 * @param expression
	 * @return	value of the expression
	 */
	public Object executeExpression(SimpleNode expression) {
		expression.jjtAccept(this, null);
		return stack.pop();
	}

	/* *********************************************************** */

	/* implementation of visit methods for each class of AST node */

	/* *********************************************************** */
	/* it seems to be necessary to define a visit() method for SimpleNode */

	public Object visit(SimpleNode node, Object data) {
		// throw new TransformLangExecutorRuntimeException(node,
		// "Error: Call to visit for SimpleNode");
		return data;
	}

	public Object visit(CLVFStart node, Object data) {

		stack.enteredBlock(node.getScope());
		
		final int childCount = node.jjtGetNumChildren();
		for (int i = 0; i < childCount; i++) {
			// to save some execution time, we will skip function declarations
			// because there is nothing to do in there anyway
			final SimpleNode child = (SimpleNode)node.jjtGetChild(i);
			if (child.getId() == TransformLangParserTreeConstants.JJTFUNCTIONDECLARATION) {
				continue;
			}
			// block is responsible for cleaning up results of statement expression
			executeAndCleanup(node.jjtGetChild(i), data);
			
			// catch and reset any break interrupts
			if (breakFlag) {
				breakFlag = false;
			}
		}

		if (!keepGlobalScope) {
			// debugging is off: throw away the global scope
			stack.exitedBlock();
		}
		
		return data; // this value is ignored in this example
	}

	public Object visit(CLVFStartExpression node, Object data) {

		int i, k = node.jjtGetNumChildren();
		stack.enteredBlock(node.getScope());

		for (i = 0; i < k; i++)
			node.jjtGetChild(i).jjtAccept(this, data);

		if (!keepGlobalScope) {
			// debugging is off: throw away the global scope
			stack.exitedBlock();
		}
		
		return data; // this value is ignored in this example
	}
	
	public Object visit(CLVFAnd node, Object data) {
		// LHS 
		node.jjtGetChild(0).jjtAccept(this, data);
		boolean a = stack.popBoolean();

		// lazy evaluation 
		if (!a) {
			stack.push(false);
			return data;
		}

		// RHS
		node.jjtGetChild(1).jjtAccept(this, data);
		
		// whatever is on the stack is result of the operation
		// if null it will fail when next op tries to use it
		return data;
	}
	
	public Object visit(CLVFAddNode node, Object data) {
		node.jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(1).jjtAccept(this, data);
		
		if (node.getType().isInteger()) {
			final int rhs = stack.popInt();
			final int lhs = stack.popInt();
			stack.push(lhs + rhs);
		} else if (node.getType().isLong()) {
			final long rhs = stack.popLong();
			final long lhs = stack.popLong();
			stack.push(lhs + rhs);
		} else if (node.getType().isDouble()) {
			final double rhs = stack.popDouble();
			final double lhs = stack.popDouble();
			stack.push(lhs + rhs);
		} else if(node.getType().isDecimal()) {
			final BigDecimal rhs = stack.popDecimal();
			final BigDecimal lhs = stack.popDecimal();
			stack.push(lhs.add(rhs,MAX_PRECISION));
		} else if (node.getType().isString()) {
			final String rhs = stack.popString();
			final String lhs = stack.popString();
			stack.push(lhs + rhs);
		} else if (node.getType().isList()) {
			List<Object> lhs = null;
			List<Object> result = new ArrayList<Object>();
			if (((SimpleNode)node.jjtGetChild(1)).getType().isList()) {
				final List<Object> rhs = stack.popList();
				lhs = stack.popList();
				result.addAll(lhs);
				result.addAll(rhs);
			} else {
				final Object rhs = stack.pop();
				lhs = stack.popList();
				result.addAll(lhs);
				result.add(rhs);
			}
			stack.push(result);
		} else if (node.getType().isMap()) {
			final Map<Object,Object> rhs = stack.popMap();
			final Map<Object,Object> lhs = stack.popMap();
			Map<Object,Object> result = new HashMap<Object,Object>();
			result.putAll(lhs);
			result.putAll(rhs);
			stack.push(result);
		} else {
			throw new TransformLangExecutorRuntimeException("add: unknown type");
		}
		
		return data;
	}
	
	public Object visit(CLVFOr node, Object data) {
		//LHS
		node.jjtGetChild(0).jjtAccept(this, data);
		boolean lhs = stack.popBoolean();

		// lazy evaluation
		if (lhs) {
			stack.push(true);
			return data;
		}

		// RHS
		node.jjtGetChild(1).jjtAccept(this, data);
		return data;
	}

	
	public Object visit(CLVFComparison node, Object data) {
		switch (node.getOperator()) {
		case REGEX_EQUAL:
			// TODO: optimize by compiling literals
			node.jjtGetChild(0).jjtAccept(this, data);
			String input = stack.popString();
			node.jjtGetChild(1).jjtAccept(this, data);
			String pattern = stack.popString();
			
			stack.push(Pattern.matches(pattern, input));
			break;
		case REGEX_CONTAINS:
			node.jjtGetChild(0).jjtAccept(this, data);
			input = stack.popString();
			node.jjtGetChild(1).jjtAccept(this, data);
			pattern = stack.popString();
			
			Pattern p = Pattern.compile(pattern);
			stack.push(p.matcher(input).find());

			break;
		default:
			node.jjtGetChild(0).jjtAccept(this, data);
			final Object lhs = stack.pop();
			node.jjtGetChild(1).jjtAccept(this, data);
			final Object rhs = stack.pop();
			compare(lhs,rhs,node.getOperationType(),node.getOperator());
			break;
		}
		
		
		return data;
	}
		
	
	
	/**
	 * Comparison for relational operators
	 * @param lhs
	 * @param rhs
	 * @param operationType
	 * @param operator
	 */	
	private void compare(Object lhsValue, Object rhsValue, TLType operationType, int operator) {
			// Integer
			if (operationType.isInteger()) {
				final int lhs = (Integer)lhsValue;
				final int rhs = (Integer)rhsValue;
				
				switch (operator) {
				case EQUAL:
					stack.push(lhs == rhs);
					break;// equal
				case LESS_THAN:
					stack.push(lhs < rhs);
					break;// less than
				case GREATER_THAN:
					stack.push(lhs > rhs);
					break;// grater than
				case LESS_THAN_EQUAL:
					stack.push(lhs <= rhs);
					break;// less than equal
				case GREATER_THAN_EQUAL:
					stack.push(lhs >= rhs);
					break;// greater than equal
				case NON_EQUAL:
					stack.push(lhs != rhs);
					break;
				default:
					// this should never happen !!!
					logger.fatal("Internal error: Unsupported comparison operator !");
					throw new RuntimeException("Internal error - Unsupported comparison operator !");
				}
	
				return;
			}
			
			// Long
			if (operationType.isLong()) {
				final long lhs = (Long)lhsValue;
				final long rhs = (Long)rhsValue;
				
				switch (operator) {
				case EQUAL:
					stack.push(lhs == rhs);
					break;// equal
				case LESS_THAN:
					stack.push(lhs < rhs);
					break;// less than
				case GREATER_THAN:
					stack.push(lhs > rhs);
					break;// grater than
				case LESS_THAN_EQUAL:
					stack.push(lhs <= rhs);
					break;// less than equal
				case GREATER_THAN_EQUAL:
					stack.push(lhs >= rhs);
					break;// greater than equal
				case NON_EQUAL:
					stack.push(lhs != rhs);
					break;
				default:
					// this should never happen !!!
					logger.fatal("Internal error: Unsupported comparison operator !");
					throw new RuntimeException("Internal error - Unsupported comparison operator !");
				}
	
				return;
			}
		
			// Double
			if (operationType.isDouble()) {
				final double lhs = (Double)lhsValue;
				final double rhs = (Double)rhsValue;
				
				switch (operator) {
				case EQUAL:
					stack.push(lhs == rhs);
					break;// equal
				case LESS_THAN:
					stack.push(lhs < rhs);
					break;// less than
				case GREATER_THAN:
					stack.push(lhs > rhs);
					break;// grater than
				case LESS_THAN_EQUAL:
					stack.push(lhs <= rhs);
					break;// less than equal
				case GREATER_THAN_EQUAL:
					stack.push(lhs >= rhs);
					break;// greater than equal
				case NON_EQUAL:
					stack.push(lhs != rhs);
					break;
				default:
					// this should never happen !!!
					logger.fatal("Internal error: Unsupported comparison operator !");
					throw new RuntimeException("Internal error - Unsupported comparison operator !");
				}
	
				return;
			}
			
			// Boolean
			if (operationType.isBoolean()) {
				final boolean lhs = (Boolean)lhsValue;
				final boolean rhs = (Boolean)rhsValue;
				
				switch (operator) {
				case EQUAL:
					stack.push(lhs == rhs);
					break;// equal
				case NON_EQUAL:
					stack.push(lhs != rhs);
					break;
				default:
					// this should never happen !!!
					logger.fatal("Internal error: Unsupported comparison operator !");
					throw new RuntimeException("Internal error - Unsupported comparison operator !");
				}
	
				return;
			}
			
			// Decimal
			if (operationType.isDecimal()) {
				final BigDecimal lhs = (BigDecimal)lhsValue;
				final BigDecimal rhs = (BigDecimal)rhsValue;
				
				switch (operator) {
				case EQUAL:
					stack.push(lhs.compareTo(rhs) == 0);
					break;// equal
				case LESS_THAN:
					stack.push(lhs.compareTo(rhs) < 0);
					break;// less than
				case GREATER_THAN:
					stack.push(lhs.compareTo(rhs) > 0);
					break;// grater than
				case LESS_THAN_EQUAL:
					stack.push(lhs.compareTo(rhs) <= 0);
					break;// less than equal
				case GREATER_THAN_EQUAL:
					stack.push(lhs.compareTo(rhs) >= 0);
					break;// greater than equal
				case NON_EQUAL:
					stack.push(!lhs.equals(rhs));
					break;
				default:
					// this should never happen !!!
					logger.fatal("Internal error: Unsupported comparison operator !");
					throw new RuntimeException("Internal error - Unsupported comparison operator !");
				}
	
				return;
			}
			
			// Datetime
			if (operationType.isDate()) {
				final Date lhs = (Date)lhsValue;
				final Date rhs = (Date)rhsValue;
				
				switch (operator) {
				case EQUAL:
					stack.push(lhs.compareTo(rhs) == 0);
					break;// equal
				case LESS_THAN:
					stack.push(lhs.compareTo(rhs) < 0);
					break;// less than
				case GREATER_THAN:
					stack.push(lhs.compareTo(rhs) > 0);
					break;// grater than
				case LESS_THAN_EQUAL:
					stack.push(lhs.compareTo(rhs) <= 0);
					break;// less than equal
				case GREATER_THAN_EQUAL:
					stack.push(lhs.compareTo(rhs) >= 0);
					break;// greater than equal
				case NON_EQUAL:
					stack.push(!lhs.equals(rhs));
					break;
				default:
					// this should never happen !!!
					logger.fatal("Internal error: Unsupported comparison operator !");
					throw new RuntimeException("Internal error - Unsupported comparison operator !");
				}
	
				return;
			}
			
			// String
			if (operationType.isString()) {
				final String lhs = (String)lhsValue;
				final String rhs = (String)rhsValue;
				
				switch (operator) {
				case EQUAL:
					stack.push(lhs.compareTo(rhs) == 0);
					break;// equal
				case LESS_THAN:
					stack.push(lhs.compareTo(rhs) < 0);
					break;// less than
				case GREATER_THAN:
					stack.push(lhs.compareTo(rhs) > 0);
					break;// grater than
				case LESS_THAN_EQUAL:
					stack.push(lhs.compareTo(rhs) <= 0);
					break;// less than equal
				case GREATER_THAN_EQUAL:
					stack.push(lhs.compareTo(rhs) >= 0);
					break;// greater than equal
				case NON_EQUAL:
					stack.push(!lhs.equals(rhs));
					break;
				default:
					// this should never happen !!!
					logger.fatal("Internal error: Unsupported comparison operator !");
					throw new RuntimeException("Internal error - Unsupported comparison operator !");
				}
	
				return;
			}
			
			throw new IllegalArgumentException("Unknwon type or operator");
			
	}

	

	public Object visit(CLVFSubNode node, Object data) {
		node.jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(1).jjtAccept(this, data);
		
		if (node.getType().isInteger()) {
			final int rhs = stack.popInt();
			final int lhs = stack.popInt();
			stack.push(lhs - rhs);
		} else if (node.getType().isLong()) {
			final long rhs = stack.popLong();
			final long lhs = stack.popLong();
			stack.push(lhs - rhs);
		} else if (node.getType().isDouble()) {
			final double rhs = stack.popDouble();
			final double lhs = stack.popDouble();
			stack.push(lhs - rhs);
		} else if(node.getType().isDecimal()) {
			final BigDecimal rhs = stack.popDecimal();
			final BigDecimal lhs = stack.popDecimal();
			stack.push(lhs.subtract(rhs,MAX_PRECISION));
		} else {
			throw new TransformLangExecutorRuntimeException("substract: unknown type");
		}
		
		return data;
	}

	public Object visit(CLVFMulNode node, Object data) {
		node.jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(1).jjtAccept(this, data);
		
		if (node.getType().isInteger()) {
			final int rhs = stack.popInt();
			final int lhs = stack.popInt();
			stack.push(lhs * rhs);
		} else if (node.getType().isLong()) {
			final long rhs = stack.popLong();
			final long lhs = stack.popLong();
			stack.push(lhs * rhs);
		} else if (node.getType().isDouble()) {
			final double rhs = stack.popDouble();
			final double lhs = stack.popDouble();
			stack.push(lhs * rhs);
		} else if(node.getType().isDecimal()) {
			final BigDecimal rhs = stack.popDecimal();
			final BigDecimal lhs = stack.popDecimal();
			stack.push(lhs.multiply(rhs,MAX_PRECISION));
		} else {
			throw new TransformLangExecutorRuntimeException("multiply: unknown type");
		}
		
		return data;
	}

	public Object visit(CLVFDivNode node, Object data) {
		node.jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(1).jjtAccept(this, data);
		
		if (node.getType().isInteger()) {
			final int rhs = stack.popInt();
			final int lhs = stack.popInt();
			stack.push(lhs/rhs);
		} else if (node.getType().isLong()) {
			final long rhs = stack.popLong();
			final long lhs = stack.popLong();
			stack.push(lhs/rhs);
		} else if (node.getType().isDouble()) {
			final double rhs = stack.popDouble();
			final double lhs = stack.popDouble();
			stack.push(lhs/rhs);
		} else if(node.getType().isDecimal()) {
			final BigDecimal rhs = stack.popDecimal();
			final BigDecimal lhs = stack.popDecimal();
			stack.push(lhs.divide(rhs,MAX_PRECISION));
		} else {
			throw new TransformLangExecutorRuntimeException("divide: unknown type");
		}
		
		return data;
	}

	public Object visit(CLVFModNode node, Object data) {
		node.jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(1).jjtAccept(this, data);
		
		if (node.getType().isInteger()) {
			final int rhs = stack.popInt();
			final int lhs = stack.popInt();
			stack.push(lhs % rhs);
		} else if (node.getType().isLong()) {
			final long rhs = stack.popLong();
			final long lhs = stack.popLong();
			stack.push(lhs % rhs);
		} else if (node.getType().isDouble()) {
			final double rhs = stack.popDouble();
			final double lhs = stack.popDouble();
			stack.push(lhs % rhs);
		} else if(node.getType().isDecimal()) {
			final BigDecimal rhs = stack.popDecimal();
			final BigDecimal lhs = stack.popDecimal();
			stack.push(lhs.remainder(rhs,MAX_PRECISION));
		} else {
			throw new TransformLangExecutorRuntimeException("remainder: unknown type");
		}
		
		return data;
	}

	public Object visit(CLVFIsNullNode node, Object data) {
		final Node args = node.jjtGetChild(0);
		args.jjtGetChild(0).jjtAccept(this, data);
		stack.push(stack.pop() == null);
		return data;
	}

	public Object visit(CLVFNVLNode node, Object data) {
		final Node args = node.jjtGetChild(0);
		args.jjtGetChild(0).jjtAccept(this, data);
		Object value = stack.peek();
		if (value == null) {
			stack.pop();
			args.jjtGetChild(1).jjtAccept(this,data);
		}

		return data;
	}

	public Object visit(CLVFNVL2Node node, Object data) {
		final Node args = node.jjtGetChild(0);
		args.jjtGetChild(0).jjtAccept(this, data);
		Object value = stack.pop();
		
		if (value != null ) {
			args.jjtGetChild(1).jjtAccept(this, data);
		} else {
			args.jjtGetChild(2).jjtAccept(this, data);
		}

		return data;
	}

	public Object visit(CLVFLiteral node, Object data) {
		stack.push(node.getValue());
		return data;
	}


	public Object visit(CLVFIIfNode node, Object data) {
		final Node args = node.jjtGetChild(0);
		args.jjtGetChild(0).jjtAccept(this, data);
		boolean condition = stack.popBoolean();

		if (condition) {
			args.jjtGetChild(1).jjtAccept(this, data);
		} else {
			args.jjtGetChild(2).jjtAccept(this, data);
		}

		return data;
	}
	
	public Object visit(CLVFInFunction node, Object data) {
		final Node args = node.jjtGetChild(0);

		// LHS: item to look for
		args.jjtGetChild(0).jjtAccept(this, data); 
		Object item = stack.pop();
		
		// RHS: list to search in
		SimpleNode rhsNode = (SimpleNode)args.jjtGetChild(1);
		rhsNode.jjtAccept(this, data); 

		/* 
		 * List.contains() as well as Map.containsKey() accepts Object
		 * so exact typing is not necessary
		 */
		if (rhsNode.getType().isList()) {
			List<?> list = stack.popList();
			stack.push(list.contains(item));
		} else {
			Map<?,?> map = stack.popMap();
			stack.push(map.containsKey(item));
		}
		
		return data;
	}
	
	public Object visit(CLVFReadDictNode node, Object data) {
		node.jjtGetChild(0).jjtGetChild(0).jjtAccept(this, data);
		stack.push(graph.getDictionary().getValue(stack.popString()));
		return data;
	}

	public Object visit(CLVFWriteDictNode node, Object data) {
		node.jjtGetChild(0).jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(0).jjtGetChild(1).jjtAccept(this, data);
		
		final String value = stack.popString();
		final String key = stack.popString();
		try {
			graph.getDictionary().setValue(key, value);
			//stack.push(Boolean.TRUE);
		} catch (ComponentNotReadyException e) {
			//stack.push(Boolean.FALSE);
			throw new TransformLangExecutorRuntimeException(node, e.getMessage());
		}
		return data;
	}

	public Object visit(CLVFDeleteDictNode node, Object data) {
		node.jjtGetChild(0).jjtGetChild(0).jjtAccept(this, data);
		
		final String key = stack.popString();
		try {
			graph.getDictionary().setValue(key, null);
			//stack.push(Boolean.TRUE); if somebody changes his mind and 
			//TODO 
		} catch (ComponentNotReadyException e) {
			//stack.push(Boolean.FALSE);
			throw new TransformLangExecutorRuntimeException(node, e.getMessage()); 
		}
		return data;
	}

	public Object visit(CLVFPrintErrNode node, Object data) {
		final Node args = node.jjtGetChild(0);
		args.jjtGetChild(0).jjtAccept(this, data); 
		
		Object argument = stack.pop();
		if (node.printLine) {
			StringBuilder buf = new StringBuilder((argument != null ? argument.toString() : "<null>"));
			buf.append(" (on line: ").append(node.getBegin().getLine());
			buf.append(" col: ").append(node.getBegin().getColumn()).append(")");
			System.err.println(buf);
		} else {
			System.err.println(argument != null ? argument : "<null>");
		}

		return data;
	}

	public Object visit(CLVFPrintStackNode node, Object data) {
		final Object[] contents = stack.getStackContents();
		for (int i = stack.length()-1; i >= 0; i--) {
			System.err.println("[" + i + "] : " + contents[i]);
		}

		return data;
	}

	public Object visit(CLVFForStatement node, Object data) {
		stack.enteredBlock(node.getScope());
		
		SimpleNode forInit = node.getForInit();
		SimpleNode forFinal = node.getForFinal();
		SimpleNode forUpdate = node.getForUpdate();
		SimpleNode forBody = node.getForBody();
		
		// execute init if exists
		if (forInit != null) {
			executeAndCleanup(forInit, data);
		}
		
		// evaluate condition if exists, infinite loop if it does not exist
		boolean condition = true;
		if (forFinal != null) {
			forFinal.jjtAccept(this, data); 
			condition = stack.popBoolean();
		}

		// loop execution
		while (condition) {
			// loops always have (possibly fake) body
			forBody.jjtAccept(this, data);
			// check for break or continue statements
			if (breakFlag) {
				if (breakType == BREAK_BREAK || breakType == BREAK_CONTINUE) {
					breakFlag = false;
				}
				
				if (breakType == BREAK_BREAK || breakType == BREAK_RETURN) {
					stack.exitedBlock();
					return data;
				}
			}
			
			// evaluate update clause
			if (forUpdate != null) {
				forUpdate.jjtAccept(this, data);
			}
			
			// check final condition
			if (forFinal != null) {
				forFinal.jjtAccept(this, data);
				condition = stack.popBoolean();
			}
		}

		stack.exitedBlock();
		return data;
	}

	public Object visit(CLVFForeachStatement node, Object data) {
		stack.enteredBlock(node.getScope());
		
		final CLVFVariableDeclaration var = (CLVFVariableDeclaration)node.jjtGetChild(0);
		
		// loops always have (possibly fake) body
		final SimpleNode body = (SimpleNode)node.jjtGetChild(2);
		
		SimpleNode composite = (SimpleNode)node.jjtGetChild(1);
		
		
		if (composite.getType().isList()) {
			// iterable composite is a list - iterate over elements
			// iterating over record fields
			node.jjtGetChild(1).jjtAccept(this, data);
			final List<Object> list = stack.popList();
			
			for (Object o : list) {
				setVariable(var,o);
				// block is responsible for cleanup
				body.jjtAccept(this, data);
				// check for break or return statements
				if (breakFlag) {
					if (breakType == BREAK_BREAK || breakType == BREAK_CONTINUE) {
						breakFlag = false;
					}
					
					if (breakType == BREAK_BREAK || breakType == BREAK_RETURN) {
						stack.exitedBlock();
						return data;
					}
				}

			}

		} else {
			// iterable composite is a record - iterate over type-safe fields
			
			if (node.getTypeSafeFields() != null) {
				// iterating over record fields
				node.jjtGetChild(1).jjtAccept(this, data);
				final DataRecord record = stack.popRecord();
				
				for (int field : node.getTypeSafeFields()) {
					setVariable(var,fieldValue(record.getField(field)));
					// block is responsible for cleanup
					body.jjtAccept(this, data);
					// check for break or return statements
					if (breakFlag) {
						if (breakType == BREAK_BREAK || breakType == BREAK_CONTINUE) {
							breakFlag = false;
						}
						
						if (breakType == BREAK_BREAK || breakType == BREAK_RETURN) {
							stack.exitedBlock();
							return data;
						}
					}
	
				}
				
			}
		}
		stack.exitedBlock();
		return data;
	}

	public Object visit(CLVFWhileStatement node, Object data) {
		stack.enteredBlock(node.getScope());
		final SimpleNode loopCondition = (SimpleNode)node.jjtGetChild(0);
		
		// loops always have (possibly fake) body
		SimpleNode body = (SimpleNode)node.jjtGetChild(1);

		// evaluate the condition
		loopCondition.jjtAccept(this, data); 
		boolean condition = stack.popBoolean();

		// loop execution
		while (condition) {
			// block is responsible for cleanup
			body.jjtAccept(this, data);
			// check for break or continue statements
			if (breakFlag) {
				if (breakType == BREAK_BREAK || breakType == BREAK_CONTINUE) {
					breakFlag = false;
				}
				
				if (breakType == BREAK_BREAK || breakType == BREAK_RETURN) {
					stack.exitedBlock();
					return data;
				}
			}

			// evaluate the condition
			loopCondition.jjtAccept(this, data);
			condition = stack.popBoolean();
		}

		stack.exitedBlock();
		return data;
	}

	public Object visit(CLVFIfStatement node, Object data) {
		node.jjtGetChild(0).jjtAccept(this, data); 
		boolean condition = stack.popBoolean();

		// first if
		if (condition) {
			// block is responsible for cleanup
			stack.enteredBlock(node.getThenScope());
			node.jjtGetChild(1).jjtAccept(this, data);
			stack.exitedBlock();
		} else { 
			// else part exists
			if (node.jjtGetNumChildren() > 2) {
				stack.enteredBlock(node.getElseScope());
				node.jjtGetChild(2).jjtAccept(this, data);
				stack.exitedBlock();
			}
		}

		return data;
	}

	public Object visit(CLVFConditionalExpression node, Object data) {
		node.jjtGetChild(0).jjtAccept(this, data); // evaluate the
		boolean condition = stack.popBoolean();

		// first if
		if (condition) {
			node.jjtGetChild(1).jjtAccept(this, data);
		} else { // if else part exists
			if (node.jjtGetNumChildren() > 2) {
				node.jjtGetChild(2).jjtAccept(this, data);
			}
		}

		return data;
	}

	public Object visit(CLVFDoStatement node, Object data) {
		stack.enteredBlock(node.getScope());
		
		boolean condition = false;
		final Node loopCondition = node.jjtGetChild(1);
		// loops always have (possibly fake) body
		final Node body = node.jjtGetChild(0);

		
		// loop execution
		do {
			body.jjtAccept(this, data);
			// check for break or continue statements
			if (breakFlag) {
				if (breakType == BREAK_BREAK || breakType == BREAK_CONTINUE) {
					breakFlag = false;
				}
				
				if (breakType == BREAK_BREAK || breakType == BREAK_RETURN) {
					stack.exitedBlock();
					return data;
				}
			}
			
			// evaluate the condition
			loopCondition.jjtAccept(this, data);
			condition = stack.popBoolean();
		} while (condition);

		stack.exitedBlock();
		return data;
	}

	public Object visit(CLVFSwitchStatement node, Object data) {
		// switch statement is a block itself
		stack.enteredBlock(node.getScope());
		
		// compute switch expression
		SimpleNode switchExpression = (SimpleNode)node.jjtGetChild(0);
		switchExpression.jjtAccept(this, data);
		final Object switchVal = stack.pop();
		
		
		for (int caseIdx : node.getCaseIndices()) {
			node.jjtGetChild(caseIdx).jjtAccept(this, data);
			final Object caseVal = stack.pop();
			compare(switchVal,caseVal,switchExpression.getType(),TransformLangParserConstants.EQUAL);
			boolean caseMatches = stack.popBoolean();
			if (caseMatches) {
				/*
				 *  Case expression matches switch expression.
				 *  Start executing until we hit a break or process all children.
				 *  We don't need to evaluate case statements as they act just like labels.
				 */ 
				for (int i=caseIdx+1; i<node.jjtGetNumChildren(); i++) {
					final SimpleNode child =(SimpleNode)node.jjtGetChild(i); 
					if (child.getId() != TransformLangParserTreeConstants.JJTCASESTATEMENT) {
						// switch is a block itself so it must clean-up after expression
						executeAndCleanup(child, data);
						if (breakFlag) {
							// catch break interrupt
							if (breakType == BREAK_BREAK) {
								breakFlag = false;
							}
							// any of break/continue/return jumps away from switch body (to the block above)
							stack.exitedBlock();
							return data;
						}

					}
				}
				
				return data;
			}
		}
		
		/*
		 * No match - default case processing
		 */
		if (node.hasDefaultClause()) {
			for (int i=node.getDefaultCaseIndex()+1; i<node.jjtGetNumChildren(); i++) {
				final SimpleNode child =(SimpleNode)node.jjtGetChild(i); 
				if (child.getId() != TransformLangParserTreeConstants.JJTCASESTATEMENT) {
					// switch is a block itself so it must clean-up after expression
					executeAndCleanup(child, data);
					if (breakFlag) {
						// catch break interrupt
						if (breakType == BREAK_BREAK) {
							breakFlag = false;
						}
						// any of break/continue/return jumps away from switch body (to the block above)
						stack.exitedBlock();
						return data;
					}
				}
			}
		}

		stack.exitedBlock();
		return data;
	}

	public Object visit(CLVFCaseStatement node, Object data) {
		// execute the case expression and leave it on the stack for parent switch
		node.jjtGetChild(0).jjtAccept(this, data);
		return data;
	}

	public Object visit(CLVFBlock node, Object data) {
		// blocks inherit the scope from their parent statement so we don't need to push anything
		// we also do no allow loose block statements so there is no problem with variable visibility
		
		final int childCount = node.jjtGetNumChildren();
		for (int i = 0; i < childCount; i++) {
			// block is responsible for cleaning up results of statement expression
			executeAndCleanup(node.jjtGetChild(i), data);
			if (breakFlag) {
				return data;
			}
		}
		return data;
	}

	/*
	 * Loop & block & function control nodes
	 */
	public Object visit(CLVFBreakStatement node, Object data) {
		breakFlag = true; // we encountered break statement;
		breakType = BREAK_BREAK;
		return data;
	}

	public Object visit(CLVFContinueStatement node, Object data) {
		breakFlag = true; // we encountered continue statement;
		breakType = BREAK_CONTINUE;
		return data;
	}

	public Object visit(CLVFReturnStatement node, Object data) {
		if (node.jjtHasChildren()) {
			node.jjtGetChild(0).jjtAccept(this, data);
		}
		// save return value of the function to avoid losing it when stack is cleared
		this.lastReturnValue = stack.pop();
		
		// set interrupt flag
		breakFlag = true;
		breakType = BREAK_RETURN;
		return data;
	}

	public Object visit(CLVFBreakpointNode node, Object data) {
		// list all variables
		System.err.println("** list of global variables ***");
		final Object[] globalVariables = stack.getGlobalVariables();
		for (int i = 0; i < globalVariables.length; i++) {
			System.out.println(globalVariables[i]);
		}
		System.err.println("** list of local variables ***");
		final Object[] localVariables = stack.getLocalVariables();
		for (int i = 0; i < localVariables.length; i++) {
			System.out.println(localVariables[i]);
		}

		return data;
	}

	/*
	 * Variable declarations
	 */
	public Object visit(CLVFVariableDeclaration node, Object data) {
		if (node.jjtGetNumChildren() > 1) {
			// variable with initializer
			SimpleNode rhs = (SimpleNode)node.jjtGetChild(1);
			rhs.jjtAccept(this, data);
			
			Object value = stack.pop();
			// handle implicit type conversion for primitive numeric types
			if (! node.getType().equals(rhs.getType())) {
				value = convertValue(rhs.getType(),node.getType(),value);
			}
			
			// this will possibly also provide container for lists/maps
			setVariable(node,value);
			return data;
		}
		
		// default initializers
		final TLType varType = node.getType();
		if (varType.isInteger()) {
			setVariable(node,Integer.valueOf(0));
		} else if (varType.isBoolean()) {
			setVariable(node,false);
		} else if (varType.isString()) {
			setVariable(node,"");
		} else if (varType.isList()) {
			setVariable(node,new ArrayList<Object>());
		} else if (varType.isDouble()) {
			setVariable(node,Double.valueOf(0)); 
		} else if (varType.isLong()) {
			setVariable(node,Long.valueOf(0));
		} else if (varType.isDate()) {
			setVariable(node,new Date(0));
		} else if (varType.isDecimal()) {
			setVariable(node,new BigDecimal(0));
		}  else if (varType.isMap()) {
			setVariable(node,new HashMap<Object,Object>());
		}  else if (varType.isRecord()) {
			final DataRecordMetadata metaData = ((TLTypeRecord)varType).getMetadata();
			final DataRecord record = new DataRecord(metaData);
			record.init();
			setVariable(node, record);
		} 
		
		return data;
	}

	public Object visit(CLVFAssignment node, Object data) {
		
		SimpleNode lhs = (SimpleNode)node.jjtGetChild(0);
		SimpleNode rhs = (SimpleNode)node.jjtGetChild(1);
		Object value = null;
		switch (lhs.getId()) {
		case TransformLangParserTreeConstants.JJTIDENTIFIER:
			rhs.jjtAccept(this, data);
			value = stack.pop();
			if (! lhs.getType().equals(rhs.getType())) {
				value = convertValue(rhs.getType(),lhs.getType(),value);
			}
			setVariable(lhs, value);
			break;
		case TransformLangParserTreeConstants.JJTARRAYACCESSEXPRESSION:
			final SimpleNode argNode = (SimpleNode)lhs.jjtGetChild(0);
			argNode.jjtAccept(this, data);
			
			if (argNode.getType().isList()) {
				// accessing list
				final List<Object> list = (List<Object>)stack.popList();
				lhs.jjtGetChild(1).jjtAccept(this, data);
				final int index = stack.popInt();

				rhs.jjtAccept(this, data);
				value = stack.pop();
				if (! lhs.getType().equals(rhs.getType())) {
					value = convertValue(rhs.getType(),lhs.getType(),value);
				}

				
				if (index < list.size()) {
					list.set(index, value);
				} else {
					// this prevents IndexOutOfBoundsException when index > size
					for (; list.size() <= index; list.add(null));
					list.set(index,value);
				}
			} else {
				// accessing map
				final Map<Object,Object> map = (Map<Object,Object>)stack.popMap();
				lhs.jjtGetChild(1).jjtAccept(this, data);
				final Object index = stack.pop();
				
				rhs.jjtAccept(this, data);
				value = stack.pop();
				if (! lhs.getType().equals(rhs.getType())) {
					value = convertValue(rhs.getType(),lhs.getType(),value);
				}
				
				map.put(index,value);
			}
			break;
		case TransformLangParserTreeConstants.JJTFIELDACCESSEXPRESSION:
			final CLVFFieldAccessExpression accessNode = (CLVFFieldAccessExpression)lhs;
			final DataRecord record = outputRecords[accessNode.getRecordId()];
			if (accessNode.isWildcard()) {
				// $record.* allows copying by position for equal metadata
				rhs.jjtAccept(this, data);
				value = stack.pop();
				if (value != null) {
					// RHS must be a record -> copy fields
					record.copyFieldsByPosition((DataRecord)value);
				} else {
					// value is null -> set all fields to null
					record.reset();
				}
			} else {
				// otherwise we populate the target field
				rhs.jjtAccept(this, data);
				value = stack.pop();
				if (! lhs.getType().equals(rhs.getType())) {
					value = convertValue(rhs.getType(),lhs.getType(),value);
				}
				record.getField(accessNode.getFieldId()).setValue(value);
			}
			break;
		case TransformLangParserTreeConstants.JJTMEMBERACCESSEXPRESSION:
			lhs.jjtGetChild(0).jjtAccept(this,data);
			final DataRecord varRecord = stack.popRecord();
			final CLVFMemberAccessExpression memberAccNode = (CLVFMemberAccessExpression)lhs;
			if (memberAccNode.isWildcard()) {
				// $myVar.* = $other.*   allows copying by position
				rhs.jjtAccept(this, data);
				value = stack.pop();
				if (value != null) {
					// RHS must be a record -> copy fields by value
					varRecord.copyFieldsByPosition((DataRecord)value);
				} else {
					// RHS is null -> set all fields to null
					varRecord.reset();
				}
			} else {
				// otherwise we populate the target fields
				rhs.jjtAccept(this, data);
				value = stack.pop();
				if (! lhs.getType().equals(rhs.getType())) {
					value = convertValue(rhs.getType(),lhs.getType(),value);
				}
				varRecord.getField(memberAccNode.getFieldId()).setValue(value);
			}
			break;
		default:
			throw new IllegalArgumentException("Invalid LHS for assignment");
		}
		
		// the value of assignment is a value of RHS
		stack.push(value);
		
		return data;
	}

	
	private Object convertValue(final TLType fromType, final TLType toType, final Object value) {
		// null has separate type but nothing to do here
		if (fromType.isNull()) {
			return null;
		}
		
		// int -> long
		if (toType.isLong()) {
			if (fromType.isInteger()) {
				return new Long((Integer)value);
			}
		}
		
		if (toType.isDouble()) {
			// int -> double
			if (fromType.isInteger()) {
				return new Double((Integer)value);
			}
			
			// long -> double
			if (fromType.isLong()) {
				return new Double((Long)value);
			}
		}
		
		if (toType.isDecimal()) {
			// int -> decimal
			if (fromType.isInteger()) {
				return new BigDecimal((Integer)value,MAX_PRECISION);
			}
			
			// long -> decimal
			if (fromType.isLong()) {
				return new BigDecimal((Long)value,MAX_PRECISION);
			}
			
			// double -> decimal
			if (fromType.isDouble()) {
				return new BigDecimal((Double)value,MAX_PRECISION);
			}
		}
		
		throw new IllegalArgumentException("Cannot convert value '" + value 
				+ "' from '" + fromType.name() + "' to '" + toType.name() + "'");
		
	}
	
	public Object visit(CLVFFunctionDeclaration node, Object data) {
		// nothing to do
		return data;
	}

	public Object visit(CLVFRaiseErrorNode node, Object data) {
		node.jjtGetChild(0).jjtAccept(this, data);
		String message = stack.popString();
		throw new TransformLangExecutorRuntimeException(node, null, "Exception raised by user: " + ((message != null) ? message.toString() : "no message"));

	}

	public Object visit(CLVFEvalNode node, Object data) {
		// get TL expression
		node.jjtGetChild(0).jjtAccept(this, data);
		String src = stack.pop().toString();
		Node parseTree;
		// construct parser
		try {
			((TransformLangParser) parser).ReInit(new CharSequenceReader(src));
			if (node.expMode)
				parseTree = ((TransformLangParser) parser).StartExpression();
			else
				parseTree = ((TransformLangParser) parser).Start();
		} catch (ParseException ex) {
			throw new TransformLangExecutorRuntimeException(node, "Can't parse \"eval\" expression:" + ex.getMessage());
		} catch (NullPointerException ex) {
			throw new RuntimeException("Error in \"eval\" execution/parsing (parser is missing).", ex);
		}

		/*
		 * option to permanently store parsed expression in this tree if (true){ //add this subtree to eclosing AST }
		 */

		// execute eval
		if (node.expMode)
			visit((CLVFStartExpression) parseTree, data);
		else
			visit((CLVFStart) parseTree, data);

		return data;
	}

	public Object visit(CLVFSequenceNode node,Object data){
        Sequence seq = node.getSequence();
		
        switch(node.opType){
        case CLVFSequenceNode.OP_NEXT:
        	if (node.getType().isInteger()) {
        		stack.push(seq.nextValueInt());
        	} else if (node.getType().isLong()) {
        		stack.push(seq.nextValueLong());
        	} else if (node.getType().isString()) {
        		stack.push(seq.nextValueString());
        	}
        	break;
        case CLVFSequenceNode.OP_CURRENT:
        	if (node.getType().isInteger()) {
        		stack.push(seq.currentValueInt());
        	} else if (node.getType().isLong()) {
        		stack.push(seq.currentValueLong());
        	} else if (node.getType().isString()) {
        		stack.push(seq.currentValueString());
        	}
        	break;
        case CLVFSequenceNode.OP_RESET:
    		node.getSequence().resetValue();
            stack.push(0);
            break;
        default: 
        	throw new TransformLangExecutorRuntimeException("Illegal operation for sequence:  '" +  node.opType + "\'" );
        }
        
        return data;
    }

	public Object visit(CLVFLookupNode node, Object data) {
		final int index = node.getLookupIndex();
		
		switch (node.getOperation()) {
		case CLVFLookupNode.OP_INIT:
			try {
				// reinitialize a free-d lookup table
				node.getLookupTable().init();
				
				/*
				 * Use the DataRecord stored within this node.
				 * The SAME record is shared among all nodes accessing the same lookup table.
				 * That allows calling seek() method without argument and some performance speedup
				 * 
				 * We also use the information from old Lookup instance. That is why free() does not throw the
				 * reference away.
				 */
				final DataRecord keyRecord = node.getLookupRecord();
				lookups[index] = node.getLookupTable().createLookup(lookups[index].getKey(),keyRecord);
			} catch (ComponentNotReadyException ex) {
				throw new TransformLangExecutorRuntimeException(node,"Cannot initialize lookup table '"	+ node.getLookupName()+ "' :", ex);
			}
			return data;
		case CLVFLookupNode.OP_FREE:
			node.getLookupTable().free();
			// we do NOT throw away the lookup[index] so that possible init() call can access it
			// see above how the new lookup is recreated
			return data;
		case CLVFLookupNode.OP_COUNT:
			// get parameters are stored as function parameters
			final CLVFArguments a = (CLVFArguments)node.jjtGetChild(0);
			final DataRecord lkpRec = node.getLookupRecord();
			for (int i = 0; i < lkpRec.getNumFields(); i++) {
				a.jjtGetChild(i).jjtAccept(this, data);
				lkpRec.getField(i).setValue(stack.pop());
			}
			lookups[index].seek(lkpRec);
			stack.push(lookups[index].getNumFound());
			return data;
		case CLVFLookupNode.OP_GET:
			// get parameters are stored as function parameters
			final CLVFArguments args = (CLVFArguments)node.jjtGetChild(0);
			final DataRecord lookupRecord = node.getLookupRecord();
			for (int i = 0; i < lookupRecord.getNumFields(); i++) {
				args.jjtGetChild(i).jjtAccept(this, data);
				lookupRecord.getField(i).setValue(stack.pop());
			}
			lookups[index].seek(lookupRecord);
			stack.push(lookups[index].hasNext() ? lookups[index].next() : null);
			return data;
		case CLVFLookupNode.OP_NEXT: 
			final Lookup l = lookups[node.getLookupIndex()];
			stack.push(l.hasNext() ? l.next() : null);
			return data;
		default:
			throw new TransformLangExecutorRuntimeException(node,"Illegal lookup operation '" + node.getOperation() + "'");
		}

	}

	public Object visit(CLVFPrintLogNode node, Object data) {
		if (runtimeLogger == null) {
			throw new TransformLangExecutorRuntimeException(node, "No runtime logger available");
		}
		CLVFArguments args = (CLVFArguments)node.jjtGetChild(0);
		
		LogLevelEnum logLevel = LogLevelEnum.INFO;
		Object message = null;
		
		if (args.jjtGetNumChildren() == 2) {
			args.jjtGetChild(0).jjtAccept(this, data);
			logLevel = (LogLevelEnum)stack.pop();
			args.jjtGetChild(1).jjtAccept(this, data);
			message = stack.pop();
		} else {
			args.jjtGetChild(0).jjtAccept(this, data);
			message = stack.pop();
		}
		
		switch (logLevel) {
		case DEBUG:
			runtimeLogger.debug(message);
			break;
		case INFO: 
			runtimeLogger.info(message);
			break;
		case WARN: 
			runtimeLogger.warn(message);
			break;
		case ERROR:
			runtimeLogger.error(message);
			break;
		case FATAL: 
			runtimeLogger.fatal(message);
			break;
		case TRACE:
			runtimeLogger.trace(message);
			break;
		default:
			throw new TransformLangExecutorRuntimeException(node,"Unknown log level '" + logLevel + "'");
		}
		
		return data;
	}

	public Object visit(CLVFImportSource node, Object data) {
		node.childrenAccept(this, data);
		return data;
	}

	public Object visit(CLVFPostfixExpression node, Object data) {
		// get variable && put value on stack by executing child node
		SimpleNode child = (SimpleNode) node.jjtGetChild(0);
		child.jjtAccept(this, data);
		
		final TLType opType = node.getType();
		
		
		/*
		 * To avoid unnecessary pop/push we just peek the value on the stack
		 * and increment the corresponding variable 
		 */
		switch (node.getOperator()) {
		case INCR:
			if (opType.isDecimal()) {
				setVariable(child, ((BigDecimal)stack.peek()).add(BigDecimal.ONE,MAX_PRECISION));
			} else if (opType.isDouble()) {
				setVariable(child, (Double)stack.peek() + 1);
			} else if (opType.isInteger()) {
				setVariable(child, (Integer)stack.peek() + 1);
			} else if (opType.isLong()) {
				setVariable(child, (Long)stack.peek() + 1);
			} 
			break;
		case DECR:
			if (opType.isDecimal()) {
				setVariable(child, ((BigDecimal)stack.peek()).subtract(BigDecimal.ONE,MAX_PRECISION));
			} else if (opType.isDouble()) {
				setVariable(child, (Double)stack.peek() - 1);
			} else if (opType.isInteger()) {
				setVariable(child, (Integer)stack.peek() - 1);
			} else if (opType.isLong()) {
				setVariable(child, (Long)stack.peek() - 1);
			} 
			break;	
		default:
			throw new TransformLangExecutorRuntimeException("Unkown postfix operator '" + node.getOperator() + "'");
		}
		
		return data;
	}

	public Object visit(CLVFUnaryExpression node, Object data) {
		final SimpleNode child = (SimpleNode)node.jjtGetChild(0);
		child.jjtAccept(this, data);
		
		final TLType opType = node.getType();
		
		switch (node.getOperator()) {
		case INCR:
			if (opType.isDecimal()) {
				final BigDecimal result = stack.popDecimal().add(BigDecimal.ONE,MAX_PRECISION);
				stack.push(result);
				setVariable(child, result);
			} else if (opType.isDouble()) {
				final double result = stack.popDouble() + 1;
				stack.push(result);
				setVariable(child, result);
			} else if (opType.isInteger()) {
				final int result = stack.popInt() + 1;
				stack.push(result);
				setVariable(child, result);
			} else if (opType.isLong()) {
				final long result = stack.popLong() + 1;
				stack.push(result);
				setVariable(child, result);
			} 
			break;
		case DECR:
			if (opType.isDecimal()) {
				final BigDecimal result = stack.popDecimal().subtract(BigDecimal.ONE,MAX_PRECISION);
				stack.push(result);
				setVariable(child, result);
			} else if (opType.isDouble()) {
				final double result = stack.popDouble() - 1;
				stack.push(result);
				setVariable(child, result);
			} else if (opType.isInteger()) {
				final int result = stack.popInt() - 1;
				stack.push(result);
				setVariable(child, result);
			} else if (opType.isLong()) {
				final long result = stack.popLong() - 1;
				stack.push(result);
				setVariable(child, result);
			} 
			break;
		case NOT:
			stack.push(! stack.popBoolean());
			break;
		case MINUS:
			if (opType.isDecimal()) {
				stack.push(stack.popDecimal().negate());
			} else if (opType.isDouble()) {
				stack.push(- stack.popDouble());
			} else if (opType.isInteger()) {
				stack.push(- stack.popInt());
			} else if (opType.isLong()) {
				stack.push(- stack.popLong());
			} 
			break;
		default:
			throw new TransformLangExecutorRuntimeException(node, "Unknown prefix operator '" + node.getOperator() + "'");
		}

		return data;
	}

	public Object visit(CLVFListOfLiterals node, Object data) {
		
		if (!node.areAllItemsLiterals() || node.getValue() == null) {
			List<Object> value = new ArrayList<Object>();
			for (int i=0; i<node.jjtGetNumChildren(); i++) {
				node.jjtGetChild(i).jjtAccept(this, data);
				value.add(stack.pop());
			}
			node.setValue(value);
		}
		
		// we have to guarantee the list literal value remains constant
		stack.push(new ArrayList<Object>(node.getValue()));
		return data;
	}

	public Object visit(CLVFFieldAccessExpression node, Object data) {
		/*
		 * Output fields are handled in assignment via setVariable()
		 * They should never be visited 
		 */
		if (node.isOutput()) {
			throw new IllegalArgumentException("Output field should never be visited explicitely!!");
		}
		
		// record.* handling
		if (node.isWildcard()) {
			stack.push(inputRecords[node.getRecordId()]);
			return data;
		}
		
		final DataField field = inputRecords[node.getRecordId()].getField(node.getFieldId());
		stack.push(fieldValue(field));
		
		return data;
	}
	
	private Object fieldValue(DataField field) {
		
		if (field.isNull()) {
			return null;
		}
		
		// we must convert from the field's mutable type to our static types used
		switch (field.getType()) {
		case DataFieldMetadata.DECIMAL_FIELD:
			// we want the decimal within the field to undergo satisfyPrecision() check 
			// so that out-of-precision errors are discovered early
			return ((DecimalDataField)field).getDecimal().getBigDecimalOutput();
		case DataFieldMetadata.STRING_FIELD:
			// StringBuilder -> String
			return ((StringDataField)field).getValue().toString();

		case DataFieldMetadata.BOOLEAN_FIELD:
		case DataFieldMetadata.BYTE_FIELD:
		case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
		case DataFieldMetadata.DATE_FIELD:
		case DataFieldMetadata.INTEGER_FIELD:
		case DataFieldMetadata.LONG_FIELD:
		case DataFieldMetadata.NUMERIC_FIELD:
			// relevant numeric object
			return field.getValue();

		default:
			throw new IllegalArgumentException("Unknown field type: '" + field.getType() + "'" );	
		}
		
	}

	public Object visit(CLVFMemberAccessExpression node, Object data) {
		node.jjtGetChild(0).jjtAccept(this, data);

		final DataRecord record = stack.popRecord();

		// member access may occur when accessing lookup tables and thus record can be null
		// in that case we return null as field value
		// this is also result in case user accesses a record-type variable with null value
		if (record == null) {
			stack.push(null);
			return data;
		}
		
		// if node is wildcard the whole record is the value
		if (node.isWildcard()) {
			stack.push(record);
			return data;
		}
		
		// else the value of a specific field is the value
		stack.push(fieldValue(record.getField(node.getFieldId())));
		return data;
	}

	public Object visit(CLVFArrayAccessExpression node, Object data) {
		SimpleNode composite = (SimpleNode)node.jjtGetChild(0);
		composite.jjtAccept(this, data);
		
		if (composite.getType().isMap()) {
			Map<Object,Object> m = stack.popMap();
			
			// compute index and push the value stored within map
			node.jjtGetChild(1).jjtAccept(this, data);
			stack.push(m.get(stack.pop()));
		} else if (composite.getType().isList()) {
			List<Object> list = stack.popList();
			
			// compute index and push value stored in the list
			node.jjtGetChild(1).jjtAccept(this, data);
			stack.push(list.get(stack.popInt()));
		}
		
		return data;
	}

	public Object visit(CLVFArguments node, Object data) {
		final int childCount = node.jjtGetNumChildren();
		for (int i=0; i<childCount; i++) {
			node.jjtGetChild(i).jjtAccept(this, data);
		}
		
		return data;
	}

	public Object visit(CLVFIdentifier node, Object data) {
		stack.push(stack.getVariable(node.getBlockOffset(), node.getVariableOffset()));
		return data;
	}

	public Object visit(CLVFType node, Object data) {
		// nothing to do
		return data;
	}

	public Object visit(CLVFDateField node, Object data) {
		stack.push(((TLDateField)node.getType()).getSymbol());
		return data;
	}

	public Object visit(CLVFParameters node, Object data) {
		// formal parameters - nothing to do
		return data;
	}

	public Object visit(CLVFFunctionCall node, Object data) {
		node.jjtGetChild(0).jjtAccept(this, data);
		if (node.isExternal()) {
			node.getExtecutable().execute(stack, node.getFunctionCallContext());
		} else {
			executeFunction(node);
		}
		return data;
	}

	
	public Object visit(CLVFLogLevel node, Object data) {
		stack.push(((TLLogLevel)node.getType()).getSymbol());
		return data;
	}
	
	public Object visit(CastNode node, Object data) {
		node.jjtGetChild(0).jjtAccept(this, data);

		// value is null - no conversions are applicable (or necessary)
		if (stack.peek() == null) {
			return data;
		}
		
		// implicit conversion to string
		if (node.getToType().isString()) {
			Object value = stack.pop();
			stack.push(value.toString());
			return data;
		}
		
		// long
		if (node.getToType().isLong()) {
			final TLType fromType = node.getFromType();
			if (fromType.isInteger()) {
				stack.push(new Long(stack.popInt()));
			}
			
			return data;
		}
		
		// double
		if (node.getToType().isDouble()) {
			final TLType fromType = node.getFromType();
			if (fromType.isInteger()) {
				stack.push(new Double(stack.popInt()));
			} else if (fromType.isLong()) {
				stack.push(new Double(stack.popLong()));
			}
			return data;
		}
		
		
		// numeric conversion to decimal (results in BigDecimal allocation)
		if (node.getToType().isDecimal()) {
			final TLType fromType = node.getFromType();
			if (fromType.isInteger()) {
				stack.push(new BigDecimal(stack.popInt(),MAX_PRECISION));
			} else if (fromType.isLong()) {
				stack.push(new BigDecimal(stack.popLong(),MAX_PRECISION));
			} else if (fromType.isDouble()) {
				stack.push(new BigDecimal(stack.popDouble(),MAX_PRECISION));
			} 
			
			return data;
		}
		
		throw new TransformLangExecutorRuntimeException(
				"Unknown type cast from '" + node.getFromType().name() 
				+ "' to '" + node.getToType().name()
		);
	}
	
	public void setAst(CLVFStart ast) {
		setASTInternal(ast);
	}
	
	public void setAst(CLVFStartExpression ast) {
		setASTInternal(ast);
	}

	private void setASTInternal(SimpleNode ast) {
		this.ast = ast;
	}
	
	public void executeFunction(CLVFFunctionDeclaration node, Object[] data) {
		final CLVFParameters formal = (CLVFParameters)node.jjtGetChild(1);
		
		stack.enteredBlock(node.getScope());
		
		for (int i=0; i<data.length; i++) {
			setVariable((SimpleNode)formal.jjtGetChild(i), data[i]);
		}
		
		// function return value will be saved in this.lastReturnValue
		node.jjtGetChild(2).jjtAccept(this, null);
		

		
		// clear all break flags
		if (breakFlag) {
			breakFlag = false;
		}
		
		stack.exitedBlock();
	}

	/**
	 * Executes function with specified name declared within CTL code.
	 * First performs a lookup to retrieve function declaration.
	 * If function is overloaded in CTL, first declared function with such name is executed.
	 * Therefore this method should be used only with functions that are not overloaded.
	 * 
	 * @param functionName	name of function to execute
	 * @param arguments	arguments to pass to the executed function
	 * @param inputRecords	global array of input records
	 * @param outputRecords	global array of output records
	 * @return	return value of executed function
	 * @throws TransformLangExecutorRuntimeException	if function with such a name does not exist
	 */
	public Object executeFunction(String functionName, Object[] arguments, TLType[] dataTypes, DataRecord[] inputRecords, DataRecord[] outputRecords) 
	throws TransformLangExecutorRuntimeException {
		CLVFFunctionDeclaration d = getFunction(functionName, dataTypes);

		if (d == null ) {
			throw new TransformLangExecutorRuntimeException("Function " + functionName + ": declaration not found");
		}
		
		return executeFunction(d,arguments,inputRecords,outputRecords);

	}
	
	
	/**
	 * Executes AST node containing a function declaration
	 * @param node	function declaration
	 * @param arguments	arguments to be passed to the function
	 * @param inputRecords	global array of input records
	 * @param outputRecords global array of output records
	 * @return return value of executed function or <code>null</code> if <code>void</code>
	 */
	public Object executeFunction(CLVFFunctionDeclaration node, Object[] arguments, DataRecord[] inputRecords, DataRecord[] outputRecords) {
		
		
		//set input and output records (if given)
		this.inputRecords = inputRecords;
		this.outputRecords = outputRecords;

		//clean previous return value
		this.lastReturnValue = null;
		
		//execute function
		executeFunction(node,arguments);

		//return result
        return this.lastReturnValue;
	}
	
	
	/**
	 * Executes function without passing input or output records.
	 * If function accesses them it will fail with NullPointerException
	 * 
	 * @param functionName
	 * @param arguments
	 * @return
	 */
	public Object executeFunction(String functionName, Object[] arguments, TLType[] argumentTypes) {
		return executeFunction(functionName, arguments, argumentTypes, null, null);
	}
	
	/**
	 * Executes local function. Actual function parameters must be already computed on the stack
	 * when this method is called.
	 * 
	 * @param node
	 */
	private void executeFunction(CLVFFunctionCall node) {
		final CLVFFunctionDeclaration callTarget = node.getLocalFunction();
		final CLVFParameters formal = (CLVFParameters)callTarget.jjtGetChild(1);
		final CLVFArguments actual = (CLVFArguments)node.jjtGetChild(0);
		
		// activate function scope
		stack.enteredBlock(callTarget.getScope());
		
		// set function parameters - they are already computed on stack
		for (int i=actual.jjtGetNumChildren()-1; i>=0; i--) {
			setVariable((SimpleNode)formal.jjtGetChild(i), stack.pop());
		}
		
		// execute function body
		callTarget.jjtGetChild(2).jjtAccept(this, null);
		
		// set the saved return value back onto stack
		if (!node.getType().isVoid()) {
			stack.push(this.lastReturnValue);
			this.lastReturnValue = null;
		}
		
		// clear all break flags
		if (breakFlag) {
			breakFlag = false;
		}
		
		// clear function scope
		stack.exitedBlock();
	}
	

	
	/**
	 * Executes an expression statement and removes its possible value
	 * that is still stored on stack.
	 * 
	 * E.g.
	 *   int i = 2;
	 *   i++; <-- we must get rid of this value
	 *   i = i + 1;
	 * @param node
	 */
	private void executeAndCleanup(Node node, Object data) {
		final int stackSize = stack.length();
		node.jjtAccept(this, data);
		// cleanup and values from the previous call
		// result of ++, assignment, non-void function call statements etc.
		if (stack.length() > stackSize) {
			stack.pop();
		}
	}
	
	
	private void setVariable(SimpleNode node, Object value) {
		if (node.getId() == TransformLangParserTreeConstants.JJTMEMBERACCESSEXPRESSION) {
			final CLVFIdentifier recId = (CLVFIdentifier) node.jjtGetChild(0);
			final int fieldId = ((CLVFMemberAccessExpression) node).getFieldId();

			DataRecord record = (DataRecord) stack.getVariable(recId.getBlockOffset(), recId.getVariableOffset());
			record.getField(fieldId).setValue(value);
		} else {
			int blockOffset = -1;
			int varOffset = -1;

			switch (node.getId()) {
				case TransformLangParserTreeConstants.JJTIDENTIFIER:
					final CLVFIdentifier id = (CLVFIdentifier) node;
					blockOffset = id.getBlockOffset(); // jump N blocks back, -1 indicates global scope
					varOffset = id.getVariableOffset(); // jump to M-th slot within block
					break;
				case TransformLangParserTreeConstants.JJTVARIABLEDECLARATION:
					final CLVFVariableDeclaration var = (CLVFVariableDeclaration) node;
					blockOffset = 0; // current block
					varOffset = var.getVariableOffset(); // jump to M-th slot within block
					break;
				default:
					throw new TransformLangExecutorRuntimeException("Unknown variable type: " + node);
			}

			stack.setVariable(blockOffset, varOffset, value);
		}
	}
	
	
	private List<?> createListFor(TLType varType) {
		if (varType.isBoolean()) {
			return new ArrayList<Boolean>();
		} else if (varType.isDate()) {
			return new ArrayList<Date>();
		} else if (varType.isDecimal()) {
			return new ArrayList<BigDecimal>();
		} else if (varType.isString()) {
			return new ArrayList<String>();
		} else if (varType.isRecord()) {
			return new ArrayList<DataRecord>();
		} else if (varType.isInteger()) {
			return new ArrayList<Integer>();
		} else if (varType.isLong()){
			return new ArrayList<Long>();
		} else if (varType.isDouble()) {
			return new ArrayList<Double>();
		}
	
		
		throw new IllegalArgumentException("Cannot create list for type " + varType.name());
		
	}

	/**
	 * Returns keyFields and corresponding metadata created to match lookup arguments
	 * @param arguments		lookup table arguments
	 * @param recordName	name of metadata record to use
	 * @return array with two elements: (int[], DataRecordMetadata) which are keyFields for
	 * 			RecordKey allocations and metadata for the lookup records matching with keyFields
	 */
	private Object[/* 2 */] createLookupRecord(CLVFArguments arguments, String recordName, List<Integer> decimalPrecisions) {
		final int numArgs = arguments.jjtGetNumChildren();
		final DataRecordMetadata keyRecordMetadata = new DataRecordMetadata(recordName);
		final int[] keyFields = new int[numArgs];
		Iterator<Integer> decimalIter = decimalPrecisions.iterator();
		for (int i=0; i<arguments.jjtGetNumChildren(); i++) {
			SimpleNode arg = (SimpleNode)arguments.jjtGetChild(i);
			final TLType argType = arg.getType();
			final DataFieldMetadata field = new DataFieldMetadata("_field" + i, TLTypePrimitive.toCloverType(argType),"|"); 
			keyRecordMetadata.addField(field);
			keyFields[i] = i;
			if (argType.isDecimal()) {
				field.setProperty(DataFieldMetadata.LENGTH_ATTR, String.valueOf(decimalIter.next()));
				field.setProperty(DataFieldMetadata.SCALE_ATTR, String.valueOf(decimalIter.next()));
			}
		}
		
		return new Object[]{keyFields,keyRecordMetadata};
	}

	public CLVFFunctionDeclaration getFunction(String functionName, TLType... params) {
		final List<CLVFFunctionDeclaration> l = parser.getFunctions().get(functionName);
		if (l == null || l.isEmpty()) {
			return null;
		}
		for (CLVFFunctionDeclaration function : l) {
			TLType[] formalParams = function.getFormalParameters();
			if (equalParameters(formalParams, params)) {
				return function;
			}
		}
		return null;
	}
	
	private boolean equalParameters(TLType[] params1, TLType[] params2) {
		if (params1 == params2)
			return true;
		if (params1 == null || params2 == null)
			return false;
		int length = params1.length;
		if (params2.length != length)
			return false;

		for (int i = 0; i < length; i++) {
			TLType o1 = params1[i];
			TLType o2 = params2[i];
			if (!(o1 == null ? o2 == null : o1.getClass().equals(o2.getClass())))
				return false;
		}
		return true;
	}
}
