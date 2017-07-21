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
	protected boolean suspended;
	protected transient SimpleNode node;
	protected String message;
	protected Object value;
	protected CommandType forCommand;
	protected boolean error;
	private long threadId;

	public DebugStatus(SimpleNode node, CommandType forCommand) {
		this.line = node.getLine();
		this.node = node;
		this.sourceFilename = node.sourceFilename;
		suspended = true;
		value = null;
		this.forCommand=forCommand;
		this.error=false;
	}

	public boolean isError() {
		return error;
	}

	public void setError(boolean error) {
		this.error = error;
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

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
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

	public boolean isSuspended() {
		return suspended;
	}

	public void setSuspended(boolean suspended) {
		this.suspended = suspended;
	}

	public SimpleNode getNode() {
		return node;
	}

	public void setNode(SimpleNode node) {
		this.node = node;
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
}