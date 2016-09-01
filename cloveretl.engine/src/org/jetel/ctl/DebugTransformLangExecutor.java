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

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.ObjectUtils;
import org.jetel.component.Freeable;
import org.jetel.ctl.ASTnode.CLVFAnd;
import org.jetel.ctl.ASTnode.CLVFArrayAccessExpression;
import org.jetel.ctl.ASTnode.CLVFAssignment;
import org.jetel.ctl.ASTnode.CLVFBreakStatement;
import org.jetel.ctl.ASTnode.CLVFCaseStatement;
import org.jetel.ctl.ASTnode.CLVFComparison;
import org.jetel.ctl.ASTnode.CLVFConditionalExpression;
import org.jetel.ctl.ASTnode.CLVFConditionalFailExpression;
import org.jetel.ctl.ASTnode.CLVFContinueStatement;
import org.jetel.ctl.ASTnode.CLVFDictionaryNode;
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
import org.jetel.ctl.ASTnode.CLVFIsNullNode;
import org.jetel.ctl.ASTnode.CLVFLookupNode;
import org.jetel.ctl.ASTnode.CLVFMemberAccessExpression;
import org.jetel.ctl.ASTnode.CLVFNVL2Node;
import org.jetel.ctl.ASTnode.CLVFNVLNode;
import org.jetel.ctl.ASTnode.CLVFOr;
import org.jetel.ctl.ASTnode.CLVFParameters;
import org.jetel.ctl.ASTnode.CLVFPostfixExpression;
import org.jetel.ctl.ASTnode.CLVFPrintErrNode;
import org.jetel.ctl.ASTnode.CLVFPrintLogNode;
import org.jetel.ctl.ASTnode.CLVFPrintStackNode;
import org.jetel.ctl.ASTnode.CLVFRaiseErrorNode;
import org.jetel.ctl.ASTnode.CLVFReturnStatement;
import org.jetel.ctl.ASTnode.CLVFSequenceNode;
import org.jetel.ctl.ASTnode.CLVFStartExpression;
import org.jetel.ctl.ASTnode.CLVFSwitchStatement;
import org.jetel.ctl.ASTnode.CLVFUnaryExpression;
import org.jetel.ctl.ASTnode.CLVFUnaryNonStatement;
import org.jetel.ctl.ASTnode.CLVFUnaryStatement;
import org.jetel.ctl.ASTnode.CLVFVariableDeclaration;
import org.jetel.ctl.ASTnode.CLVFWhileStatement;
import org.jetel.ctl.ASTnode.Node;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.debug.Breakpoint;
import org.jetel.ctl.debug.CTLExpression;
import org.jetel.ctl.debug.CTLExpressionHelper;
import org.jetel.ctl.debug.DebugCommand;
import org.jetel.ctl.debug.DebugCommand.CommandType;
import org.jetel.ctl.debug.DebugJMX;
import org.jetel.ctl.debug.DebugStack;
import org.jetel.ctl.debug.DebugStack.FunctionCallFrame;
import org.jetel.ctl.debug.DebugStatus;
import org.jetel.ctl.debug.IdSequence;
import org.jetel.ctl.debug.ListVariableOptions;
import org.jetel.ctl.debug.ListVariableResult;
import org.jetel.ctl.debug.RunToMark;
import org.jetel.ctl.debug.SerializedDataRecord;
import org.jetel.ctl.debug.StackFrame;
import org.jetel.ctl.debug.Thread;
import org.jetel.ctl.debug.Variable;
import org.jetel.ctl.debug.condition.Condition;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ExceptionUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 * @author jan.michalica
 *
 * @created Nov 3, 2014
 */
public class DebugTransformLangExecutor extends TransformLangExecutor implements Freeable {
	
	public static final DebugStep INITIAL_DEBUG_STATE = DebugStep.STEP_RUN;
	
	public static final Object VOID_RESULT_MARKER = new Object();
	
	private static final int SYNTHETIC_FUNCTION_CALL_ID = -1;
	private static final int IMPLICIT_FUCTION_CALL_ID = -2;
	
	private volatile DebugStep step = INITIAL_DEBUG_STATE;
	private int stepTarget = -1;
	private int prevLine = -1;
	private String prevSourceId = null;
	private BlockingQueue<DebugCommand> commandQueue;
	private BlockingQueue<DebugStatus> statusQueue;
	private DebugJMX debugJMX;
	private Breakpoint curpoint;
	private volatile RunToMark runToMark;
	private Thread ctlThread;
	private java.lang.Thread lastActiveThread;
	private boolean inExecution;
	private boolean debugDisabled;
	private boolean initialized;
	private volatile boolean resumed = false;
	private DebugStack debugStack;
	private IdSequence idSequence;
	private long expressionTimeout = Long.MIN_VALUE;
	
	private long inputRecordIds[];
	private long outputRecordIds[];

	private static final Pattern KEYWORD_PATTERN = Pattern.compile("ALL|OK|SKIP|STOP");
	private static final Pattern RETURN_PATTERN = Pattern.compile("[ \t]*(return)[ \t]*");

	public enum DebugStep {
		STEP_SUSPEND,
		STEP_INTO,
		STEP_OVER,
		STEP_OUT,
		STEP_RUN;
	}
	
	public DebugTransformLangExecutor(TransformLangParser parser, TransformationGraph graph, Properties globalParameters) {
		super(parser, graph, globalParameters);
		this.curpoint = new Breakpoint(null, -1);
		setStack(new DebugStack());
	}
	
	public DebugTransformLangExecutor(TransformLangParser parser, TransformationGraph graph) {
		this(parser, graph, null);
	}
	
	public DataRecord[] getInputDataRecords() {
		return inputRecords;
	}
	
	public DataRecord[] getOutputDataRecords() {
		return outputRecords;
	}
	
	public void suspend() {
		step = DebugStep.STEP_SUSPEND;
		resumed = false;
	}

	public void resume() {
		resumed = true;
	}
	
	@Override
	public void init() {
		if (!initialized) {
			initialized = true;
			super.init();
			initDebug();
		}
	}
	
	@Override
	public void free() {
		lastActiveThread = null;
		if (debugJMX != null) {
			debugJMX.unregisterTransformLangExecutor(this);
		}
	}
	
	@Override
	public void postExecute() {
		super.postExecute();
		commandQueue = null;
		statusQueue = null;
	}
	
	public DebugStatus executeCommand(DebugCommand command) throws InterruptedException {
		commandQueue.put(command);
		return statusQueue.take();
	}
	
	public void putCommand(DebugCommand command) throws InterruptedException {
		commandQueue.put(command);
	}
	
	public DebugStatus takeStatus() throws InterruptedException {
		return statusQueue.take();
	}
	
	public Set<Breakpoint> getCtlBreakpoints() {
		return graph.getRuntimeContext().getCtlBreakpoints();
	}
	
	@Override
	@SuppressFBWarnings("EI2")
	public void setInputRecords(DataRecord[] inputRecords) {
		super.setInputRecords(inputRecords);
		this.inputRecordIds = createRecordIds(inputRecords);
	}
	
	@Override
	@SuppressFBWarnings("EI2")
	public void setOutputRecords(DataRecord[] outputRecords) {
		super.setOutputRecords(outputRecords);
		this.outputRecordIds = createRecordIds(outputRecords);
	}
	
	@Override
	public void executeFunction(CLVFFunctionDeclaration node, Object[] data) {
		/*
		 * re-implementing super type method to push implicit function call onto stack
		 */
		try {
			beforeExecute();
			final CLVFParameters formal = (CLVFParameters)node.jjtGetChild(1);
			
			CLVFFunctionCall implicitCall = new CLVFFunctionCall(IMPLICIT_FUCTION_CALL_ID);
			implicitCall.setName(node.getName());
			implicitCall.setCallTarget(node);
			
			stack.enteredBlock(node.getScope(), implicitCall);
			
			for (int i=0; i<data.length; i++) {
				setVariable((SimpleNode)formal.jjtGetChild(i), data[i]);
			}
			
			// function return value will be saved in this.lastReturnValue
			node.jjtGetChild(2).jjtAccept(this, null);
			
			// clear all break flags
			if (breakFlag) {
				breakFlag = false;
			}
			stack.exitedBlock(implicitCall);
		} finally {
			afterExecute();
		}
	};
	
	@Override
	public Object executeExpression(SimpleNode expression) {
		try {
			beforeExecute();
			CLVFFunctionCall synthCall = new CLVFFunctionCall(SYNTHETIC_FUNCTION_CALL_ID);
			synthCall.setName("<expression>");
			CLVFFunctionDeclaration synthFunc = new CLVFFunctionDeclaration(0);
			synthFunc.setName(synthCall.getName());
			synthCall.setCallTarget(synthFunc);
			debugStack.enteredSyntheticBlock(synthCall);
			Object value = super.executeExpression(expression);
			debugStack.exitedSyntheticBlock(synthCall);
			return value;
		} finally {
			afterExecute();
		}
	}
	
	public Object executeExpressionOutsideDebug(List<Node> expression, long timeoutNs) {
		try {
			debugDisabled = true;
			if (timeoutNs > 0) {
				expressionTimeout = System.nanoTime() + timeoutNs;
			}
			Object result = null;
			for (Node node : expression) {
				int stackSize = stack.length();
				node.jjtAccept(this, null);
				if (stack.length() > stackSize) {
					result = stack.pop();
				}
			}
			return result;
		} finally {
			debugDisabled = false;
			expressionTimeout = Long.MIN_VALUE;
		}
	}
	
	public void setIdSequence(IdSequence sequence) {
		this.idSequence = sequence;
		if (debugStack != null) {
			debugStack.setIdSequence(sequence);
		}
	}
	
	@Override
	protected void executeInternal(SimpleNode node) {
		try {
			beforeExecute();
			CLVFFunctionCall synthCall = new CLVFFunctionCall(SYNTHETIC_FUNCTION_CALL_ID);
			synthCall.setName("<global>");
			CLVFFunctionDeclaration synthFunc = new CLVFFunctionDeclaration(0);
			synthFunc.setName(synthCall.getName());
			synthCall.setCallTarget(synthFunc);
			debugStack.enteredSyntheticBlock(synthCall);
			super.executeInternal(node);
			debugStack.exitedSyntheticBlock(synthCall);
		} finally {
			afterExecute();
		}
	}
	
	@Override
	protected void executeFunction(CLVFFunctionCall node) {
		super.executeFunction(node);
		if (!debugDisabled && ctlThread.isStepping()) {
			prevLine = node.getLine();
			prevSourceId = node.getSourceId();
			stepAfter(node.getLine(), node);
		}
	}
	
	@Override
	protected void setVariable(SimpleNode lhs, Object value) {
		if (lhs.getId() == TransformLangParserTreeConstants.JJTMEMBERACCESSEXPRESSION) {
			SimpleNode firstChild = (SimpleNode) lhs.jjtGetChild(0);
			if (firstChild.getId() == TransformLangParserTreeConstants.JJTDICTIONARYNODE) {
				try {
					graph.getDictionary().setValue(((CLVFMemberAccessExpression) lhs).getName(), value);
				} catch (ComponentNotReadyException e) {
					throw new TransformLangExecutorRuntimeException("Dictionary is not initialized",e);
				}
			} else if (firstChild.getId() == TransformLangParserTreeConstants.JJTIDENTIFIER) {
				final CLVFIdentifier recId = (CLVFIdentifier) firstChild;
				final int fieldId = ((CLVFMemberAccessExpression) lhs).getFieldId();

				DataRecord record = (DataRecord) stack.getVariable(recId.getBlockOffset(), recId.getVariableOffset());
				record.getField(fieldId).setValue(value);
			} else {
				// TODO? TransformLangParserTreeConstants.JJTARRAYACCESSEXPRESSION
				// too difficult to implement - we would need to re-evaluate LHS to get the variable
				// containing the list or map, to be fixed with CL-2580
				throw new TransformLangExecutorRuntimeException(firstChild, "variable initialization failed");
			}
		} else if(lhs.getId() == TransformLangParserTreeConstants.JJTFIELDACCESSEXPRESSION) {
			final CLVFFieldAccessExpression faNode = (CLVFFieldAccessExpression) lhs;
			DataRecord record = faNode.isOutput() ? outputRecords[faNode.getRecordId()] : inputRecords[faNode.getRecordId()];
			record.getField(faNode.getFieldId()).setValue(value);
		} else {
			int blockOffset = -1;
			int varOffset = -1;

			switch (lhs.getId()) {
				case TransformLangParserTreeConstants.JJTIDENTIFIER:
					final CLVFIdentifier id = (CLVFIdentifier) lhs;
					blockOffset = id.getBlockOffset(); // jump N blocks back, -1 indicates global scope
					varOffset = id.getVariableOffset(); // jump to M-th slot within block
					break;
				case TransformLangParserTreeConstants.JJTVARIABLEDECLARATION:
					final CLVFVariableDeclaration var = (CLVFVariableDeclaration) lhs;
					blockOffset = 0; // current block
					varOffset = var.getVariableOffset(); // jump to M-th slot within block
					break;
				default:
					throw new TransformLangExecutorRuntimeException("Unknown variable type: " + lhs);
			}
			/*
			 * this is difference from super type
			 */
			if (lhs instanceof CLVFVariableDeclaration){
				CLVFVariableDeclaration var = (CLVFVariableDeclaration)lhs;
				debugStack.setVariable(blockOffset, varOffset, value, var.getName(), var.getType());
			} else if (lhs instanceof CLVFIdentifier) {
				CLVFIdentifier ident = (CLVFIdentifier)lhs;
				debugStack.setVariable(blockOffset, varOffset, value, ident.getName(), ident.getType());
			} else {
				throw new TransformLangExecutorRuntimeException("Unknown variable type: " + lhs);
			}
		}
	}
	
	@Override
	protected void checkInterrupt() {
		super.checkInterrupt();
		checkTimeout();
	}
	
	protected void checkTimeout() {
		if (debugDisabled /* executing expression */ && expressionTimeout > Long.MIN_VALUE) {
			if (System.nanoTime() > expressionTimeout) {
				throw new JetelRuntimeException("Timeout exceeded while evaluating expression");
			}
		}
	}
	
	private void debug(SimpleNode node, Object data) {
		if (debugDisabled || !inExecution) {
			return;
		} 
		final int curLine = node.getLine();
		if (curLine == prevLine && ObjectUtils.equals(prevSourceId, node.getSourceId())) { 
			return;
		} 
		prevLine = curLine;
		prevSourceId = node.getSourceId();
		step(curLine, node, data);
	}
	
	private long[] createRecordIds(DataRecord records[]) {
		long result[] = new long[records != null ? records.length : 0];
		for (int i = 0; i < result.length; ++i) {
			result[i] = idSequence.nextId();
		}
		return result;
	}
	
	private void beforeExecute() {
		if (debugDisabled) {
			return;
		}
		prevLine = -1;
		prevSourceId = null;
		if (lastActiveThread != java.lang.Thread.currentThread()) {
			runToMark = null;
			step = INITIAL_DEBUG_STATE;
			if (!resumed) {
				if (graph.getRuntimeContext().isSuspendThreads()) {
					step = DebugStep.STEP_SUSPEND;
				}
			}
		} else {
			resumed = false;
		}
		registerCurrentThread();
		inExecution = true;
	}
	
	private void afterExecute() {
		if (debugDisabled) {
			return;
		}
		lastActiveThread = ctlThread.getJavaThread();
		unregisterCurrentThread();
		stepTarget = -1;
		if (step == DebugStep.STEP_OVER || step == DebugStep.STEP_OUT) {
			/* we have left top level function while stepping - that is like stepping in */
			step = DebugStep.STEP_INTO;
		}
		inExecution = false;
	}
	
	private void initDebug() {
		this.commandQueue = new SynchronousQueue<>(true);
		this.statusQueue = new SynchronousQueue<>(true);
		debugJMX = graph.getDebugJMX();
		debugJMX.registerTransformLangExecutor(this);
	}
	
	private StackFrame[] getCallStack(SimpleNode node) {
		List<StackFrame> callStack = new ArrayList<StackFrame>();
		ListIterator<FunctionCallFrame> iter = debugStack.getFunctionCallStack();
		CLVFFunctionCall functionCall = null;
		int line = node.getLine();
		String sourceId = node.getSourceId();
		if (iter.hasPrevious()) {
			while (iter.hasPrevious()) {
				FunctionCallFrame callFrame = iter.previous();
				functionCall = callFrame.getFunctionCall();
				StackFrame stackFrame = new StackFrame();
				stackFrame.setCallId(callFrame.getId());
				stackFrame.setName(functionCall.getName());
				stackFrame.setSynthetic(functionCall.getId() == SYNTHETIC_FUNCTION_CALL_ID);
				stackFrame.setGenerated(functionCall.getLocalFunction() != null ? functionCall.getLocalFunction().isGenerated() : false);
				stackFrame.setLineNumber(line); 
				stackFrame.setFile(sourceId);
				stackFrame.setParamTypes(getArgumentTypeNames(functionCall.getLocalFunction()));
				callStack.add(stackFrame);
				line = functionCall.getLine();
				sourceId = functionCall.getSourceId();
			}
		}
		return callStack.toArray(new StackFrame[callStack.size()]);
	}
	
	private ListVariableResult listVariables(int frameIndex, boolean includeGlobal) {
		
		ListVariableResult result = new ListVariableResult();
		if (frameIndex >= 0) {
			final Object localVariables[] = debugStack.getLocalVariables(frameIndex);
			result.setLocalVariables(new ArrayList<Variable>(localVariables.length));
			for (int i = 0; i < localVariables.length; i++) {
				result.getLocalVariables().add(((Variable) localVariables[i]).serializableCopy());
			}
		}
		if (includeGlobal) {
			final Object globalVariables[] = debugStack.getGlobalVariables();
			result.setGlobalVariables(new ArrayList<Variable>(globalVariables.length));
			for (int i = 0; i < globalVariables.length; i++) {
				if (globalVariables[i] instanceof Variable) {
					Variable variable = (Variable)globalVariables[i];
					result.getGlobalVariables().add(variable.serializableCopy());
				}
			}
			
			if (inputRecords != null && inputRecords.length > 0) {
				List<SerializedDataRecord> records = new ArrayList<>(inputRecords.length);
				for (int i = 0; i < inputRecords.length; ++i) {
					if (inputRecords[i] == null) {
						continue;
					}
					SerializedDataRecord record = SerializedDataRecord.fromDataRecord(inputRecords[i]);
					record.setId(inputRecordIds[i]);
					records.add(record);
				}
				result.setInputRecords(records);
			}
			if (outputRecords != null && outputRecords.length > 0) {
				List<SerializedDataRecord> records = new ArrayList<>(outputRecords.length);
				for (int i = 0; i < outputRecordIds.length; ++i) {
					if (outputRecords[i] == null) {
						continue;
					}
					SerializedDataRecord record = SerializedDataRecord.fromDataRecord(outputRecords[i]);
					record.setId(outputRecordIds[i]);
					records.add(record);
				}
				result.setOutputRecords(records);
			}
		}
		return result;
	}
	
	private void stepRun(final int curLine, SimpleNode node, Object data) {
		curpoint.setLine(curLine);
		curpoint.setSource(node.getSourceId());
		if (runToMark != null && runToMark.getTo().equals(curpoint)) {
			ctlThread.setStepping(false);
			runToMark = null;
			handleSuspension(node, CommandType.RUN_TO_LINE);
			handleCommand(node);
		} else if ((runToMark == null || !runToMark.isSkipBreakpoints()) && isActiveBreakpoint(this.curpoint, node)) {
			ctlThread.setStepping(false);
			runToMark = null;
			handleSuspension(node, null);
			handleCommand(node);
		}
	}
	
	private void stepOver(final int curLine, SimpleNode node, Object data) {
		if (stepTarget == debugStack.getCurrentFunctionCallIndex()) {
			stepTarget = -1;
			ctlThread.setStepping(false);
			handleSuspension(node, CommandType.STEP_OVER);
			handleCommand(node);
			ctlThread.setSuspended(false);
		} else {
			stepRun(curLine, node, data);
		}
	}
	
	private void stepOut(final int curLine, SimpleNode node, Object data) {
		if (stepTarget == debugStack.getCurrentFunctionCallIndex()) {
			stepTarget = -1;
			ctlThread.setStepping(false);
			handleSuspension(node, CommandType.STEP_OUT);
			handleCommand(node);
			ctlThread.setSuspended(false);
		} else {
			stepRun(curLine, node, data);
		}
	}
	
	private void stepAfter(int curLine, SimpleNode node) {
		switch (step) {
		case STEP_OUT:
			stepOutAfter(curLine, node);
			break;
		case STEP_INTO:
			stepInto(curLine, node, null);
			break;
		case STEP_OVER:
			stepOverAfter(curLine, node);
			break;
		default:
		}
	}
	
	private void step(int curLine, SimpleNode node, Object data) {
		switch (step) {
		case STEP_OVER:
			stepOver(curLine, node, data);
			break;
		case STEP_OUT:
			stepOut(curLine, node, data);
			break;
		case STEP_INTO:
			stepInto(curLine, node, data);
			break;
		case STEP_RUN:
			stepRun(curLine, node, data);
			break;
		case STEP_SUSPEND:
			stepSuspend(curLine, node, data);
			break;
		default:
			throw new TransformLangExecutorRuntimeException("Undefined debugging state: " + step);
		}
	}
	
	private void stepOverAfter(int curLine, SimpleNode node) {
		if (stepTarget == debugStack.getCurrentFunctionCallIndex() + 1) {
			stepTarget = -1;
			ctlThread.setStepping(false);
			handleSuspension(node, CommandType.STEP_OVER);
			handleCommand(node);
			ctlThread.setSuspended(false);
		}
	}
	
	private void stepOutAfter(int curLine, SimpleNode node) {
		if (stepTarget == debugStack.getCurrentFunctionCallIndex()) {
			stepTarget = -1;
			ctlThread.setStepping(false);
			handleSuspension(node, CommandType.STEP_OUT);
			handleCommand(node);
			ctlThread.setSuspended(false);
		}
	}
	
	private void stepInto(final int curLine, SimpleNode node, Object data) {
		ctlThread.setStepping(false);
		handleSuspension(node, CommandType.STEP_IN);
		handleCommand(node);
		ctlThread.setSuspended(false);
	}
	
	private void stepSuspend(final int curLine, SimpleNode node, Object data) {
		handleSuspension(node, CommandType.SUSPEND);
		handleCommand(node);
		ctlThread.setSuspended(false);
	}
	
	private void registerCurrentThread() {
		ctlThread = new Thread();
		ctlThread.setJavaThread(java.lang.Thread.currentThread());
		boolean suspendIt = debugJMX.registerCTLThread(ctlThread, this);
		if (suspendIt) {
			suspend();
		}
	}
	
	private void unregisterCurrentThread() {
		if (ctlThread != null) {
			debugJMX.unregisterCTLThread(ctlThread);
			ctlThread = null;
		}
	}
	
	private void handleSuspension(SimpleNode node, CommandType cause) {
		ctlThread.setSuspended(true);
		DebugStatus status = new DebugStatus(node, cause);
		status.setSourceFilename(node.getSourceId());
		status.setThreadId(ctlThread.getId());
		debugJMX.notifySuspend(status);
	}
	
	private void handleResume(SimpleNode node, CommandType cause) {
		ctlThread.setSuspended(false);
		DebugStatus status = new DebugStatus(node, cause);
		status.setSourceFilename(node.getSourceId());
		status.setThreadId(ctlThread.getId());
		debugJMX.notifyResume(status);
	}
	
	private void setStack(DebugStack stack) {
		stack.setIdSequence(idSequence);
		this.stack = this.debugStack = stack;
	}

	private void handleCommand(SimpleNode node) {
		
		boolean runLoop = true;
		while(runLoop) {
			DebugCommand command = null;
			
			try {
				command = commandQueue.take();
			} catch (InterruptedException e) {
				logger.info("Debugging interrupted in " + ctlThread);
				throw new JetelRuntimeException("Interrupted while awaiting debug command.", e);
			}

			DebugStatus status = null;
			if (command != null) {
				try {
					switch (command.getType()) {
					case LIST_VARS:
						status = new DebugStatus(node, CommandType.LIST_VARS);
						ListVariableOptions options = (ListVariableOptions)command.getValue();
						status.setValue(listVariables(options.getFrameIndex(), options.isIncludeGlobal()));
						break;
					case RESUME:
						status = new DebugStatus(node, CommandType.RESUME);
						this.step = DebugStep.STEP_RUN;
						handleResume(node, CommandType.RESUME);
						runLoop = false;
						break;
					case STEP_IN:
						status = new DebugStatus(node, CommandType.STEP_IN);
						this.step = DebugStep.STEP_INTO;
						ctlThread.setStepping(true);
						handleResume(node, CommandType.STEP_IN);
						runLoop = false;
						break;
					case STEP_OVER:
						status = new DebugStatus(node, CommandType.STEP_OVER);
						this.step = DebugStep.STEP_OVER;
						stepTarget = debugStack.getCurrentFunctionCallIndex();
						ctlThread.setStepping(true);
						handleResume(node, CommandType.STEP_OVER);
						runLoop = false;
						break;
					case STEP_OUT:
						status = new DebugStatus(node, CommandType.STEP_OUT);
						this.step = DebugStep.STEP_OUT;
						stepTarget = debugStack.getPreviousFunctionCallIndex();
						ctlThread.setStepping(true);
						handleResume(node, CommandType.STEP_OUT);
						runLoop = false;
						break;
					case RUN_TO_LINE:
						status = new DebugStatus(node, CommandType.RUN_TO_LINE);
						runToMark = (RunToMark)command.getValue();
						this.step = DebugStep.STEP_RUN;
						ctlThread.setStepping(true);
						handleResume(node, CommandType.RUN_TO_LINE);
						runLoop = false;
						break;
					case GET_CALLSTACK:
						status = new DebugStatus(node, CommandType.GET_CALLSTACK);
						StackFrame callStack[] = getCallStack(node);
						status.setValue(callStack);
						break;
					case EVALUATE_EXPRESSION:
						status = new DebugStatus(node, CommandType.EVALUATE_EXPRESSION);
						CTLExpression expression = (CTLExpression)command.getValue();
						// CLO-9416
						Matcher matcher = KEYWORD_PATTERN.matcher(expression.getExpression());
						if (matcher.find()) {
							throw new JetelRuntimeException("Cannot evaluate statements containing keywords: ALL, OK, SKIP, STOP");
						}
						matcher = RETURN_PATTERN.matcher(expression.getExpression());
						expression.setExpression(matcher.replaceFirst(""));
						Object result = evaluateExpression(expression, node);
						status.setValue(result);
						break;
					default: {
						throw new JetelRuntimeException("Unknown command received by debug executor.");
					}
					}
				} catch (Exception e) {
					if (status == null) {
						status = new DebugStatus(node, null);
					}
					status.setException(e);
				}

				try {
					this.statusQueue.put(status);
				} catch (InterruptedException e) {
					logger.info("Debugging interrupted in " + ctlThread);
					throw new JetelRuntimeException("Interrupted while posting debug command result.", e);
				}
			}
		}
	}
	
	@Override
	protected Object getLocalVariableValue(CLVFIdentifier node) {
		try {
			return debugStack.getVariableChecked(node.getBlockOffset(), node.getVariableOffset());
		} catch (IllegalArgumentException e) {
			throw new TransformLangExecutorRuntimeException("\'" + node.getName() + "\' cannot be resolved to a variable");
		}
		
	}
	
	private Object evaluateExpression(CTLExpression expression, SimpleNode context) {
		DebugStack originalStack = this.debugStack;
		Object result;
		try {
			if (expression.getCallStackIndex() < this.debugStack.getCurrentFunctionCallIndex()) {
				FunctionCallFrame callFrame = this.debugStack.getFunctionCall(expression.getCallStackIndex() + 1);

				if (callFrame == null) {
					throw new JetelRuntimeException(String.format("Functional call for frame %d not exists.", expression.getCallStackIndex()));
				}

				context = callFrame.getFunctionCall();
				DebugStack frameStack = this.debugStack.createShallowCopyUpToFrame(callFrame);
				setStack(frameStack);
			}
			List<Node> compiled = CTLExpressionHelper.compileExpression(expression.getExpression(), this, context);
			Object value = executeExpressionOutsideDebug(compiled, TimeUnit.MILLISECONDS.toNanos(expression.getTimeout()));
			result = Variable.deepCopy(value);
		} catch (Exception e) {
			throw e;
		} finally {
			setStack(originalStack);
		}
		return result;
	}
	
	private String[] getArgumentTypeNames(CLVFFunctionDeclaration decl) {
		if (decl == null || decl.getFormalParameters() == null) {
			return null;
		}
		TLType paramTypes[] = decl.getFormalParameters();
		String result[] = new String[paramTypes.length];
		for (int i = 0; i < paramTypes.length; ++i) {
			result[i] = paramTypes[i].name();
		}
		return result;
	}
	
	private boolean isActiveBreakpoint(Breakpoint breakpoint, SimpleNode node) {
		if (!graph.getRuntimeContext().isCtlBreakingEnabled()) {
			return false;
		}
		
		for (Breakpoint bp : getCtlBreakpoints()) {
			if (bp.equals(breakpoint) && bp.isEnabled()) {
				final Condition condition = bp.getCondition();
				if (condition != null) {
					try {
						synchronized (condition) {
							condition.evaluate(this, node);
							return condition.isFulFilled();
						}
					} catch (Exception e) {
						debugJMX.notifyConditionError(bp, ExceptionUtils.getMessage(e));
					}
				}
				return true;
			}
		}
		return false;
	}
	
	@Override
	public Object visit(CLVFArrayAccessExpression node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFAssignment node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFBreakStatement node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFCaseStatement node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFComparison node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFConditionalExpression node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFConditionalFailExpression node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFContinueStatement node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFDictionaryNode node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFDivNode node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFDoStatement node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFEvalNode node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFFieldAccessExpression node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFForeachStatement node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFForStatement node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFFunctionCall node, Object data) {
		debug(node, data);
		if (node.getType().isVoid()) {
			stack.push(VOID_RESULT_MARKER);
		}
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFIfStatement node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFIIfNode node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFIsNullNode node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFLookupNode node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFMemberAccessExpression node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFNVL2Node node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFNVLNode node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFOr node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFAnd node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFPostfixExpression node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFPrintErrNode node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFPrintLogNode node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	@Deprecated
	public Object visit(CLVFPrintStackNode node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFRaiseErrorNode node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFReturnStatement node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFSequenceNode node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFStartExpression node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFSwitchStatement node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFUnaryExpression node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFUnaryNonStatement node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFUnaryStatement node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFVariableDeclaration node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
	
	@Override
	public Object visit(CLVFWhileStatement node, Object data) {
		debug(node, data);
		return super.visit(node, data);
	}
}
