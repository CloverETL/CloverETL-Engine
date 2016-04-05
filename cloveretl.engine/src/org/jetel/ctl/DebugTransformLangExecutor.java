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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.jetel.ctl.ASTnode.CLVFFunctionCall;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.ASTnode.CLVFImportSource;
import org.jetel.ctl.ASTnode.CLVFStart;
import org.jetel.ctl.ASTnode.Node;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.ctl.debug.Breakpoint;
import org.jetel.ctl.debug.DebugCommand;
import org.jetel.ctl.debug.DebugCommand.CommandType;
import org.jetel.ctl.debug.DebugJMX;
import org.jetel.ctl.debug.DebugStack;
import org.jetel.ctl.debug.DebugStatus;
import org.jetel.ctl.debug.StackFrame;
import org.jetel.ctl.debug.Thread;
import org.jetel.ctl.debug.Variable;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.string.StringUtils;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Nov 3, 2014
 */
public class DebugTransformLangExecutor extends TransformLangExecutor {

	
	public static final DebugStep INITIAL_DEBUG_STATE = DebugStep.STEP_RUN;
	
	public enum DebugStep {
		STEP_SUSPEND,
		STEP_INTO,
		STEP_OVER,
		STEP_OUT,
		STEP_RUN;
	}
	
	private int prevLine = -1;
	private Set<Breakpoint> breakpoints;
	private volatile DebugStep step = INITIAL_DEBUG_STATE;
	private String prevSourceFilename = null;
	private ArrayBlockingQueue<DebugCommand> commandQueue;
	private ArrayBlockingQueue<DebugStatus> statusQueue;
	private PrintStream debug_print;
	private DebugJMX debugJMX;
	private Breakpoint curpoint;
	private Thread ctlThread;
	private boolean inExecution;

	public DebugTransformLangExecutor(TransformLangParser parser, TransformationGraph graph, Properties globalParameters) {
		super(parser, graph, globalParameters);
		this.breakpoints= new HashSet<Breakpoint>();
		List<Breakpoint> breakpoints = graph.getRuntimeContext().getCtlBreakpoints();
		if (breakpoints != null) {
			this.breakpoints.addAll(breakpoints);
		}
		this.curpoint=new Breakpoint(null, -1);
		this.stack= new DebugStack();
	}
	
	public DebugTransformLangExecutor(TransformLangParser parser, TransformationGraph graph) {
		this(parser, graph, null);
	}
	
	/**
	 * @param node
	 */
	protected void printSourceLine(SimpleNode node) {
		int curline = node.getLine();
		Scanner scanner=null;
		if (node.sourceFilename!=null){
			try {
				scanner = new Scanner(new File(node.getSourceFilename()));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else{
			scanner = new Scanner(this.parser.getSource());
		}
		int index=0;
		while(scanner.hasNextLine()){
			String line=scanner.nextLine();
			index++;
			if (index==curline){
				debug_print.println(line);
			}
		}
		scanner.close();
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

	protected void handleBreakpoint(SimpleNode node, Object data) {
		DebugStatus status = new DebugStatus(node, CommandType.SUSPEND);
		status.setSuspended(true);
		status.setSourceFilename(node.getSourceId());
		status.setThreadId(ctlThread.getId());
		debugJMX.notifySuspend(status);
	}
	
	protected void handleResume(SimpleNode node, Object data) {
		DebugStatus status = new DebugStatus(node, CommandType.RESUME);
		status.setSuspended(false);
		status.setThreadId(ctlThread.getId());
		debugJMX.notifyResumed(status);
	}

	protected void handleCommand(SimpleNode node) {
		DebugCommand command = null;
		boolean runloop = true;

		while (runloop) {
			try {
				command = commandQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			DebugStatus status = null;
			if (command != null) {
				switch (command.getType()) {
				case LIST_VARS:
					// list all variables
					ArrayList<Variable> vars = new ArrayList<Variable>();
					try {
						final Object[] globalVariables = stack.getGlobalVariables();
						for (int i = 0; i < globalVariables.length; i++) {
							vars.add((Variable) globalVariables[i]);
						}
						final Object[] localVariables = ((DebugStack) stack).getAllLocalVariables();
						
						for (int i = 0; i < localVariables.length; i++) {
							vars.add((Variable) localVariables[i]);
						}
					} catch (Exception ex) {
						// ignore for now
					}
					status = new DebugStatus(node, CommandType.LIST_VARS);
					status.setValue(vars.toArray(new Variable[0]));
					break;
				case GET_VAR:
					String varname = ((Variable)command.getValue()).getName();
					Variable var = (Variable) ((DebugStack) stack).getVariable(varname);
					status = new DebugStatus(node, CommandType.GET_VAR);
					if (var!=null){
						status.setValue(new Variable(var.getName(), var.getType(), false, var.getValue()));
					}else{
						status.setError(true);
						status.setValue(command.getValue());
						status.setMessage("unknown variable name: "+varname);
					}
					break;
				case SET_VAR:
					Variable var2set = (Variable) command.getValue();
					Variable variable = (Variable) ((DebugStack) stack).getVariable(var2set.getName());
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
					runloop = false;
					// is this correct place?
					handleResume(node, status);
					break;
				case STEP_IN:
					status = new DebugStatus(node, CommandType.STEP_IN);
					status.setSuspended(false);
					this.withinFunction = null;
					this.step = DebugStep.STEP_INTO;
					runloop = false;
					break;
				case STEP_OVER:
					status = new DebugStatus(node, CommandType.STEP_OVER);
					status.setSuspended(false);
					this.step = DebugStep.STEP_OVER;
					this.withinFunction = ((DebugStack) stack).getFunctionCallNode();
					runloop = false;
					break;
				case STEP_OUT:
					status = new DebugStatus(node, CommandType.STEP_OUT);
					status.setSuspended(false);
					this.step = DebugStep.STEP_OUT;
					this.withinFunction = ((DebugStack) stack).getPreviousFunctionCallNode();
					runloop = false;
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
					ArrayList<StackFrame> callStack = new ArrayList<StackFrame>();
					ListIterator<CLVFFunctionCall> iter = ((DebugStack) stack).getFunctionCallsStack();
					CLVFFunctionCall functionCall = null;
					int line = node.getLine();
					String sourceId = node.getSourceId();
					while (iter.hasPrevious()) {
						functionCall = iter.previous();
						StackFrame stackFrame = new StackFrame();
						stackFrame.setName(functionCall.getName());
						stackFrame.setLineNumber(line); 
						stackFrame.setFile(sourceId);
//						System.out.println(functionCall.getName() + ":" + line + ", " + sourceId);
						callStack.add(stackFrame);

						line = functionCall.getLine();
						sourceId = functionCall.getSourceId();
					}

					// add also first stackframe (generate, preExecute, etc.) which is not in stack
					Node parentNode = functionCall != null ? functionCall.jjtGetParent() : node.jjtGetParent();
					while (!(parentNode instanceof CLVFStart)) {
						if (parentNode instanceof CLVFFunctionDeclaration) {
							CLVFFunctionDeclaration declarationNode = (CLVFFunctionDeclaration) parentNode;
							StackFrame functionDeclarationFrame = new StackFrame();
							functionDeclarationFrame.setName(declarationNode.getName());
							functionDeclarationFrame.setLineNumber(line);
							functionDeclarationFrame.setFile(declarationNode.getSourceId());
							callStack.add(functionDeclarationFrame);
//							System.out.println(declarationNode.getName() + ":" + line + ", " + declarationNode.getSourceId());
							break;
						} else {
							parentNode = parentNode.jjtGetParent();
						}
					}
					status = new DebugStatus(node, CommandType.GET_CALLSTACK);
					status.setValue(callStack.toArray(new StackFrame[callStack.size()]));
					break;
				case REMOVE_BREAKPOINT:{
					Breakpoint bpoint = (Breakpoint) command.getValue();
					status = new DebugStatus(node, CommandType.REMOVE_BREAKPOINT);
					if (!this.breakpoints.remove(bpoint)){
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
						if (!this.breakpoints.add(bpoint)){
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
					status.setValue(this.breakpoints.toArray(new Breakpoint[0]));
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
							this.breakpoints.add(bpoint);
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
				}
				try {
					this.statusQueue.put(status);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				command = null;
			}
		}

	}

	@Override
	public final void debug(SimpleNode node, Object data) {
		if (!inExecution) {
			return;
		}
		
		final int curLine = node.getLine();
		if (curLine == prevLine && node.sourceFilename==prevSourceFilename){
			return;
		}
		prevLine = curLine;
		prevSourceFilename=node.sourceFilename;
		
		switch (step) {
		case STEP_OUT:
		case STEP_OVER:
			if (this.withinFunction == ((DebugStack) stack).getFunctionCallNode()) {
				ctlThread.setSuspended(true);
				handleBreakpoint(node, data);
				handleCommand(node);
				ctlThread.setSuspended(false);
			}
			break;
		case STEP_INTO:
			ctlThread.setSuspended(true);
			handleBreakpoint(node, data);
			handleCommand(node);
			ctlThread.setSuspended(false);
			break;
		case STEP_RUN:
			this.curpoint.setLine(curLine);
			this.curpoint.setSource(node.getSourceId());
			if (this.breakpoints.contains(this.curpoint)) {
				ctlThread.setSuspended(true);
				handleBreakpoint(node, data);
				handleCommand(node);
			}
			break;
		case STEP_SUSPEND:
			ctlThread.setSuspended(true);
			handleBreakpoint(node, data);
			handleCommand(node);
			ctlThread.setSuspended(false);
			break;
		default:
			throw new TransformLangExecutorRuntimeException("Undefined debugging state: " + step);
		}
		
	}
	
	@Override
	public void init() {
		super.init();
		initDebug();
	}
	
	@Override
	public void postExecute() {
		super.postExecute();
		commandQueue = null;
		statusQueue = null;
	}
	
	public BlockingQueue<DebugCommand> getCommandQueue() {
		return commandQueue;
	}
	
	public BlockingQueue<DebugStatus> getStatusQueue() {
		return statusQueue;
	}
	
	public synchronized void suspendExecution() {
			this.step=DebugStep.STEP_SUSPEND;
	}
	
	@Override
	public final boolean inDebugMode(){
		return graph.getRuntimeContext().isCtlDebug();
	}
	
	@Override
	protected void beforeExecute() {
		registerCurrentThread();
		inExecution = true;
	}
	
	@Override
	protected void afterExecute() {
		unregisterCurrentThread();
		inExecution = false;
	}
	
	private void initDebug() {
		this.commandQueue = new ArrayBlockingQueue<DebugCommand>(2, true);
		this.statusQueue = new ArrayBlockingQueue<DebugStatus>(1, true);
		debugJMX = graph.getDebugJMX();
		debugJMX.registerTransformLangExecutor(this);
	}
	
	private void registerCurrentThread() {
		this.ctlThread = createCurrentCTLThread();
		debugJMX.registerCTLThread(ctlThread, commandQueue, statusQueue);
	}
	
	private void unregisterCurrentThread() {
		if (ctlThread != null) {
			debugJMX.unregisterCTLDebugThread(ctlThread);
			ctlThread = null;
		}
	}
	
	private Thread createCurrentCTLThread() {
		java.lang.Thread javaThread = java.lang.Thread.currentThread();
		Thread ctlThread = new Thread();
		ctlThread.setId(javaThread.getId());
		ctlThread.setName(javaThread.getName());
		ctlThread.setStepping(false); // TODO
		ctlThread.setSuspended(false);
		return ctlThread;
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
}
