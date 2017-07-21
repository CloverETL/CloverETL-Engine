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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.apache.commons.lang.ObjectUtils;
import org.jetel.component.Freeable;
import org.jetel.ctl.ASTnode.CLVFFunctionCall;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.ASTnode.CLVFImportSource;
import org.jetel.ctl.ASTnode.CLVFParameters;
import org.jetel.ctl.ASTnode.Node;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.debug.Breakpoint;
import org.jetel.ctl.debug.DebugCommand;
import org.jetel.ctl.debug.DebugCommand.CommandType;
import org.jetel.ctl.debug.DebugJMX;
import org.jetel.ctl.debug.DebugStack;
import org.jetel.ctl.debug.DebugStack.FunctionCallFrame;
import org.jetel.ctl.debug.DebugStatus;
import org.jetel.ctl.debug.RunToMark;
import org.jetel.ctl.debug.SerializedDataRecord;
import org.jetel.ctl.debug.StackFrame;
import org.jetel.ctl.debug.Thread;
import org.jetel.ctl.debug.Variable;
import org.jetel.ctl.debug.VariableID;
import org.jetel.ctl.debug.VariableRetrievalResult;
import org.jetel.data.DataRecord;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.string.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Nov 3, 2014
 */
public class DebugTransformLangExecutor extends TransformLangExecutor implements Freeable {
	
	public static final DebugStep INITIAL_DEBUG_STATE = DebugStep.STEP_RUN;
	
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
	private boolean initialized;
	private volatile boolean resumed = false;
	
	private long inputRecordIds[];
	private long outputRecordIds[];

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
		this.stack = new DebugStack();
	}
	
	public DebugTransformLangExecutor(TransformLangParser parser, TransformationGraph graph) {
		this(parser, graph, null);
	}
	
	public void suspend() {
		step = DebugStep.STEP_SUSPEND;
		resumed = false;
	}

	public void resume() {
		resumed = true;
	}

	@Override
	public final void debug(SimpleNode node, Object data) {
		if (!inExecution) {
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
	
	@Override
	public final boolean inDebugMode(){
		return graph.getRuntimeContext().isCtlDebug();
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
			getStack().enteredSyntheticBlock(synthCall);
			Object value = super.executeExpression(expression);
			getStack().exitedSyntheticBlock(synthCall);
			return value;
		} finally {
			afterExecute();
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
			getStack().enteredSyntheticBlock(synthCall);
			super.executeInternal(node);
			getStack().exitedSyntheticBlock(synthCall);
		} finally {
			afterExecute();
		}
	}
	
	@Override
	protected void executeFunction(CLVFFunctionCall node) {
		super.executeFunction(node);
		if (ctlThread.isStepping()) {
			prevLine = node.getLine();
			prevSourceId = node.getSourceId();
			stepAfter(node.getLine(), node);
		}
	}
	
	private long[] createRecordIds(DataRecord records[]) {
		long result[] = new long[records != null ? records.length : 0];
		for (int i = 0; i < result.length; ++i) {
			result[i] = getStack().nextVariableId();
		}
		return result;
	}
	
	private void beforeExecute() {
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
		ListIterator<FunctionCallFrame> iter = getStack().getFunctionCallStack();
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
	
	private void stepRun(final int curLine, SimpleNode node, Object data) {
		curpoint.setLine(curLine);
		curpoint.setSource(node.getSourceId());
		if (runToMark != null && runToMark.getTo().equals(curpoint)) {
			ctlThread.setStepping(false);
			runToMark = null;
			handleSuspension(node, CommandType.RUN_TO_LINE);
			handleCommand(node);
		} else if ((runToMark == null || !runToMark.isSkipBreakpoints()) && isActiveBreakpoint(this.curpoint)) {
			ctlThread.setStepping(false);
			runToMark = null;
			handleSuspension(node, null);
			handleCommand(node);
		}
	}
	
	private void stepOver(final int curLine, SimpleNode node, Object data) {
		if (stepTarget == getStack().getCurrentFunctionCallIndex()) {
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
		if (stepTarget == getStack().getCurrentFunctionCallIndex()) {
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
		if (stepTarget == getStack().getCurrentFunctionCallIndex() + 1) {
			stepTarget = -1;
			ctlThread.setStepping(false);
			handleSuspension(node, CommandType.STEP_OVER);
			handleCommand(node);
			ctlThread.setSuspended(false);
		}
	}
	
	private void stepOutAfter(int curLine, SimpleNode node) {
		if (stepTarget == getStack().getCurrentFunctionCallIndex()) {
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
		status.setSuspended(true);
		status.setSourceFilename(node.getSourceId());
		status.setThreadId(ctlThread.getId());
		debugJMX.notifySuspend(status);
	}
	
	private void handleResume(SimpleNode node, CommandType cause) {
		ctlThread.setSuspended(false);
		DebugStatus status = new DebugStatus(node, cause);
		status.setSuspended(false);
		status.setSourceFilename(node.getSourceId());
		status.setThreadId(ctlThread.getId());
		debugJMX.notifyResume(status);
	}

	private void handleCommand(SimpleNode node) {
		
		boolean runLoop = true;
		while(runLoop) {
			DebugCommand command = null;
			
			try {
				command = commandQueue.take();
			} catch (InterruptedException e) {
				logger.info("Debug interrupted in " + ctlThread);
				throw new JetelRuntimeException("Interrupted while awaiting debug command.");
			}

			DebugStatus status = null;
			if (command != null) {
				switch (command.getType()) {
				case LIST_VARS:
					List<Variable> global = new ArrayList<>();
					List<Variable> local = new ArrayList<>();
					
					final Object[] globalVariables = getStack().getGlobalVariables();
					for (int i = 0; i < globalVariables.length; i++) {
						if (globalVariables[i] instanceof Variable) {
							Variable variable = (Variable)globalVariables[i];
							global.add(variable.serializableCopy());
						}
					}
					int index = (int) command.getValue();
					final Object[] localVariables = getStack().getLocalVariables(index);
					for (int i = 0; i < localVariables.length; i++) {
						local.add(((Variable) localVariables[i]).serializableCopy());
					}
					VariableRetrievalResult result = new VariableRetrievalResult(global, local);
					if (inputRecords != null && inputRecords.length > 0) {
						List<SerializedDataRecord> records = new ArrayList<>(inputRecords.length);
						for (int i = 0; i < inputRecords.length; ++i) {
							SerializedDataRecord record = SerializedDataRecord.fromDataRecord(inputRecords[i]);
							record.setId(inputRecordIds[i]);
							records.add(record);
						}
						result.setInputRecords(records);
					}
					if (outputRecords != null && outputRecords.length > 0) {
						List<SerializedDataRecord> records = new ArrayList<>(outputRecords.length);
						for (int i = 0; i < outputRecordIds.length; ++i) {
							SerializedDataRecord record = SerializedDataRecord.fromDataRecord(outputRecords[i]);
							record.setId(outputRecordIds[i]);
							records.add(record);
						}
						result.setOutputRecords(records);
					}
					status = new DebugStatus(node, CommandType.LIST_VARS);
					status.setValue(result);
					break;
				case GET_VAR:
					VariableID varID = (VariableID) command.getValue();
					Variable var = null;
					
					final Object[] globalVars = getStack().getGlobalVariables();
					for (int i = 0; i < globalVars.length; i++) {
						if (((Variable) globalVars[i]).getName().equals(varID.getName())) {
							var = (Variable) globalVars[i];
							break;
						}
					}
					
					final Object[] vars = getStack().getLocalVariables(varID.getStackFrameDepth());
					for (int i = 0; i < vars.length; i++) {
						if (((Variable) vars[i]).getName().equals(varID.getName())) {
							var = (Variable) vars[i];
							break;
						}
					}
					
					status = new DebugStatus(node, CommandType.GET_VAR);
					if (var != null) {
						status.setValue(var.serializableCopy());
					} else {
						status.setError(true);
						status.setValue(command.getValue());
						status.setMessage("unknown variable name: " + varID.getName());
					}
					break;
				case SET_VAR:
					Variable var2set = (Variable) command.getValue();
					Variable variable = (Variable)getStack().getVariable(var2set.getName());
					status = new DebugStatus(node, CommandType.SET_VAR);
					if (variable != null) {
						if (variable.getType() != var2set.getType()){
							status.setError(true);
							status.setMessage("incompatible data types: "+var2set.getName());
							status.setValue(var2set);
						}else{
							variable.setValue(var2set.getValue());
							status.setError(false);
						}
					} else {
						status.setError(true);
						status.setMessage("unknown variable name: "+var2set.getName());
						status.setValue(var2set);
					}
					break;
				case GET_IN_RECORDS:
					status = new DebugStatus(node, CommandType.GET_IN_RECORDS);
					status.setValue(this.inputRecords);
					break;
				case GET_OUT_RECORDS:
					status = new DebugStatus(node, CommandType.GET_OUT_RECORDS);
					status.setValue(this.outputRecords);
					break;
				case RESUME:
					status = new DebugStatus(node, CommandType.RESUME);
					status.setSuspended(false);
					this.step = DebugStep.STEP_RUN;
					handleResume(node, CommandType.RESUME);
					runLoop = false;
					break;
				case SUSPEND:
					status = new DebugStatus(node, CommandType.SUSPEND);
					status.setSuspended(true);
					this.step = DebugStep.STEP_SUSPEND;
					break;
				case STEP_IN:
					status = new DebugStatus(node, CommandType.STEP_IN);
					status.setSuspended(false);
					this.step = DebugStep.STEP_INTO;
					ctlThread.setStepping(true);
					handleResume(node, CommandType.STEP_IN);
					runLoop = false;
					break;
				case STEP_OVER:
					status = new DebugStatus(node, CommandType.STEP_OVER);
					status.setSuspended(false);
					this.step = DebugStep.STEP_OVER;
					stepTarget = getStack().getCurrentFunctionCallIndex();
					ctlThread.setStepping(true);
					handleResume(node, CommandType.STEP_OVER);
					runLoop = false;
					break;
				case STEP_OUT:
					status = new DebugStatus(node, CommandType.STEP_OUT);
					status.setSuspended(false);
					this.step = DebugStep.STEP_OUT;
					stepTarget = getStack().getPreviousFunctionCallIndex();
					ctlThread.setStepping(true);
					handleResume(node, CommandType.STEP_OUT);
					runLoop = false;
					break;
				case RUN_TO_LINE:
					runToMark = (RunToMark)command.getValue();
					status = new DebugStatus(node, CommandType.RUN_TO_LINE);
					status.setSuspended(false);
					this.step = DebugStep.STEP_RUN;
					ctlThread.setStepping(true);
					handleResume(node, CommandType.RUN_TO_LINE);
					runLoop = false;
					break;
				case GET_AST: {
					StringWriter writer = new StringWriter();
					PrintWriter print = new PrintWriter(writer);
					this.ast.dump(print, "CTL");
					status = new DebugStatus(node, CommandType.GET_AST);
					status.setValue(writer.toString());
					}
					break;
				case GET_CALLSTACK:
					StackFrame callStack[] = getCallStack(node);
					status = new DebugStatus(node, CommandType.GET_CALLSTACK);
					status.setValue(callStack);
					break;
				case REMOVE_BREAKPOINT:{
					Breakpoint bpoint = (Breakpoint) command.getValue();
					status = new DebugStatus(node, CommandType.REMOVE_BREAKPOINT);
					if (!getCtlBreakpoints().remove(bpoint)){
						status.setError(true);
						status.setMessage("can not find breakpoint: "+bpoint);
						status.setValue(bpoint);
					}
					}
					break;
				case SET_BREAKPOINT: {
					Breakpoint bpoint = (Breakpoint) command.getValue();
					status = new DebugStatus(node, CommandType.SET_BREAKPOINT);
					if (verifyBreakpoint(bpoint)){
						if (!getCtlBreakpoints().add(bpoint)){
							status.setError(true);
							status.setMessage("breakpoint already set: "+bpoint);
							status.setValue(bpoint);
						}
					}else{
						status.setError(true);
						status.setMessage("invalid breakpoint definition: "+bpoint);
						status.setValue(bpoint);
					}
					}
					break;
				case LIST_BREAKPOINTS:
					status = new DebugStatus(node, CommandType.LIST_BREAKPOINTS);
					status.setValue(getCtlBreakpoints().toArray(new Breakpoint[0]));
					status.setError(false);
					break;
				case SET_BREAKPOINTS:{
					Breakpoint bpoints[] = (Breakpoint[]) command.getValue();
					status = new DebugStatus(node, CommandType.SET_BREAKPOINTS);
					for(Breakpoint bpoint: bpoints){
						if (!verifyBreakpoint(bpoint)){
							status.setError(true);
							status.setMessage("invalid breakpoint definition: "+bpoint);
							status.setValue(bpoint);
							break;
						}else{
							getCtlBreakpoints().add(bpoint);
						}
					}
					}
					break;
				case INFO: {
					StringWriter writer = new StringWriter();
					PrintWriter print = new PrintWriter(writer);
					print.print("** Reached breakpoint on line: ");
					print.print(node.getLine());
					print.print(" on node: ");
					print.println(node);
					status = new DebugStatus(node, CommandType.INFO);
					status.setValue(writer.toString());
				}
					break;
				default: {
					throw new JetelRuntimeException("Unknown command received by debug executor.");
				}
				}
				try {
					this.statusQueue.put(status);
				} catch (InterruptedException e) {
					logger.info("Debug interrupted in " + ctlThread);
					throw new JetelRuntimeException("Interrupted while putting debug command result.");
				}
			}
		}
	}
	
	@SuppressFBWarnings("BC_UNCONFIRMED_CAST")
	private DebugStack getStack() {
		return (DebugStack)stack;
	}
	
	private Node findBreakableNode(SimpleNode startNode,int onLine){
		if (startNode.getLine() == onLine && startNode.isBreakable()) return startNode;
		SimpleNode childNode;
		for(int i=0;i<startNode.jjtGetNumChildren();i++){
			childNode=(SimpleNode)startNode.jjtGetChild(i);	
			if (childNode.getLine()>onLine || (childNode instanceof CLVFImportSource)) continue; //speed up optimization
			if (findBreakableNode(childNode,onLine)!=null){
				return childNode;
			}
		}
		return null;
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
	
	private boolean isActiveBreakpoint(Breakpoint breakpoint) {
		if (!graph.getRuntimeContext().isCtlBreakingEnabled()) {
			return false;
		}
		for (Breakpoint bp : getCtlBreakpoints()) {
			if (bp.equals(breakpoint)) {
				return bp.isEnabled();
			}
		}
		return false;
	}
	
	private boolean verifyBreakpoint(Breakpoint bpoint){
		Node startImport;
		if (bpoint.getSource()==null)
			startImport=this.ast;
		else{
			startImport=this.imports.get(bpoint.getSource());
			if (startImport==null) return false;
		}
		SimpleNode foundNode;
		do{
			foundNode= (SimpleNode) findBreakableNode((SimpleNode)startImport, bpoint.getLine());
			if (foundNode==null) return false;
		}while(!StringUtils.equalsWithNulls(foundNode.sourceFilename,bpoint.getSource()));
		return true;
	}
}
