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

import java.io.Serializable;

import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.ctl.debug.DebugCommand.CommandType;

public class DebugStatus implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	protected int line;
	protected String sourceFilename;
	protected Object value;
	protected CommandType forCommand;
	private long threadId;
	private Exception exception;

	public DebugStatus(SimpleNode node, CommandType forCommand) {
		this.line = node.getLine();
		this.sourceFilename = node.sourceFilename;
		value = null;
		this.forCommand=forCommand;
	}

	public CommandType getForCommand() {
		return forCommand;
	}

	public void setForCommand(CommandType forCommand) {
		this.forCommand = forCommand;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public int getLine() {
		return line;
	}

	public void setLine(int line) {
		this.line = line;
	}

	public String getSourceFilename() {
		return sourceFilename;
	}

	public void setSourceFilename(String sourceFilename) {
		this.sourceFilename = sourceFilename;
	}

	@Override
	public String toString(){
		return this.forCommand.toString();
	}

	public long getThreadId() {
		return threadId;
	}

	public void setThreadId(long threadId) {
		this.threadId = threadId;
	}

	public Exception getException() {
		return exception;
	}

	public void setException(Exception exception) {
		this.exception = exception;
	}
}