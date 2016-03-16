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
package org.jetel.ctl.debug;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jetel.ctl.DebugTransformLangExecutor;
import org.jetel.ctl.ASTnode.CLVFFunctionCall;
import org.jetel.ctl.debug.DebugCommand;
import org.jetel.ctl.debug.DebugCommand.CommandType;

/**
 * Example debugging client (communicating through OS named pipes)
 * 
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Nov 27, 2014
 */
public class DebugClient implements Runnable{

	public static final String DEBUG_IN_PATH = "/Users/dpavlis/tmp/debug_in";
	public static final String DEBUG_OUT_PATH = "/Users/dpavlis/tmp/debug_out";
	public static BufferedReader debug_in;
	public static BufferedWriter debug_out;
	ArrayBlockingQueue<DebugStatus> status_queue;
	ArrayBlockingQueue<DebugCommand> command_queue;
	
	
	public DebugClient(ArrayBlockingQueue<DebugCommand> commandQueue, ArrayBlockingQueue<DebugStatus> statusQueue){
		this.status_queue = statusQueue;
		this.command_queue = commandQueue;
	}
	
	
	public void main(String[] args) {
		System.out.println("CTL 2 Debugger started !");
		try {
			debug_in = new BufferedReader(new InputStreamReader(new FileInputStream(DEBUG_IN_PATH)));
			debug_out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(DEBUG_OUT_PATH)));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		PrintWriter debug_print = new PrintWriter(debug_out);
		DebugStatus status;
		

		debug_print.println("CTL 2 Debugger started !");
		
		
		try {
			do {
				String command="";
				//do{
					status = status_queue.poll(100, TimeUnit.MILLISECONDS);
					//if (status==null) break;
				//}while(status.getForCommand()==CommandType.SUSPEND);

				if (status!=null){
					debug_print.print(status.getForCommand());
					debug_print.print(" : ");
					debug_print.print(status.line);
					debug_print.print(" : ");
					debug_print.print(status.sourceFilename);
					debug_print.print(" : ");
					if (status.forCommand==CommandType.LIST_VARS){
						debug_print.println();
						Variable vars[] = (Variable[])status.getValue();
						for (Variable var:vars){
							debug_print.format("%s : %s : %s : %s",var.name,var.value.toString(),var.type,var.global ? "G": "L");
							debug_print.println();
						}
					}else if (status.forCommand == CommandType.LIST_BREAKPOINTS) {
						debug_print.println();
						Breakpoint breakpoints[] = (Breakpoint[])status.getValue();
						Arrays.sort(breakpoints);
						for (Breakpoint point:breakpoints){
							debug_print.print(point.getSource());
							debug_print.print(" : ");
							debug_print.print(point.getLine());
							debug_print.println();
						}
					}else if (status.forCommand == CommandType.GET_CALLSTACK) {
						debug_print.println();
						CLVFFunctionCall calls[] = (CLVFFunctionCall[])status.getValue();
						Arrays.sort(calls);
						for (CLVFFunctionCall point:calls){
							debug_print.print(point.getLine());
							debug_print.print(" : ");
							debug_print.print(point.getName());
							debug_print.println();
						}
					}
					else
					{
					debug_print.print(status.getValue());
					debug_print.print(" : ");
					debug_print.print(status.isError());
					debug_print.print(" : ");
					debug_print.print(status.getMessage());
					debug_print.println();
					}
					debug_print.flush();
				}
					try {
						if (debug_in.ready()){
							command = debug_in.readLine();
							System.out.println(command);
						}else{
							command="";
							try {
								java.lang.Thread.sleep(250);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (command.equalsIgnoreCase("help")){
						debug_print.println("info");
						debug_print.println("tree");
						debug_print.println("resume");
						debug_print.println("step");
						debug_print.println("stepin");
						debug_print.println("stepout");
						debug_print.println("list");
						debug_print.println("getvar");
						debug_print.println("setvar");
						debug_print.println("disable");
						debug_print.println("enable");
						debug_print.println("list breakpoints");
						debug_print.println("list callstack");
						debug_print.println("line");
						
					}
					if (command.equalsIgnoreCase("info")){
						command_queue.put(new DebugCommand(CommandType.INFO));
					}
					if(command.startsWith("getvar")){
							String[] varDef = command.split("\\s+");
							DebugCommand dcommand = new DebugCommand(CommandType.GET_VAR);
							dcommand.setValue(new Variable(varDef[1]));
							command_queue.put(dcommand);
					}
					if(command.startsWith("disable")){
						String[] breakdef = command.split("\\s+");
						int line = Integer.parseInt(breakdef[1]);
						String source =  breakdef.length>2 ? breakdef[2] : null;
						DebugCommand dcommand = new DebugCommand(CommandType.REMOVE_BREAKPOINT);
						dcommand.setValue(new Breakpoint(source, line));
						command_queue.put(dcommand);
					}
					if(command.startsWith("enable")){
						String[] breakdef = command.split("\\s+");
						int line = Integer.parseInt(breakdef[1]);
						String source =  breakdef.length>2 ? breakdef[2] : null;
						DebugCommand dcommand = new DebugCommand(CommandType.SET_BREAKPOINT);
						dcommand.setValue(new Breakpoint(source, line));
						command_queue.put(dcommand);
						
					}
					if (command.equalsIgnoreCase("tree")){
						DebugCommand dcommand = new DebugCommand(CommandType.GET_AST);
						command_queue.put(dcommand);
					}
					if (command.equalsIgnoreCase("resume")) {
						DebugCommand dcommand = new DebugCommand(CommandType.RESUME);
						command_queue.put(dcommand);
					}
					if (command.equalsIgnoreCase("step")) {
						DebugCommand dcommand = new DebugCommand(CommandType.STEP_OVER);
						command_queue.put(dcommand);
					}
					if (command.equalsIgnoreCase("stepout")) {
						DebugCommand dcommand = new DebugCommand(CommandType.STEP_OUT);
						command_queue.put(dcommand);
					}
					if (command.equalsIgnoreCase("stepin")) {
						DebugCommand dcommand = new DebugCommand(CommandType.STEP_IN);
						command_queue.put(dcommand);
					}
					if (command.equalsIgnoreCase("list")) {
						DebugCommand dcommand = new DebugCommand(CommandType.LIST_VARS);
						command_queue.put(dcommand);
					}
					if (command.equalsIgnoreCase("list breakpoints")) {
						DebugCommand dcommand = new DebugCommand(CommandType.LIST_BREAKPOINTS);
						command_queue.put(dcommand);
					}
					if (command.equalsIgnoreCase("list callstack")) {
						DebugCommand dcommand = new DebugCommand(CommandType.GET_CALLSTACK);
						command_queue.put(dcommand);
					}
					if (command.equalsIgnoreCase("line")) {
					}

			} while (true);
		} catch (InterruptedException e){
			// nothing
		}
		System.out.println("CTL 2 Debugger finished !");
	}

	@Override
	public void run() {
		main(new String[0]);
		
	}
}