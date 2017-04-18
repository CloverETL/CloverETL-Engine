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

/**
 * This unit represents a single frame on the CTL interpreter function callstack.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14.3.2016
 */
public class StackFrame implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private int lineNumber;
	private String file;
	private String name;
	private String paramTypes[];
	private boolean synthetic;
	private boolean generated;
	private long callId;
	
	public int getLineNumber() {
		return lineNumber;
	}

	public void setLineNumber(int lineNumber) {
		this.lineNumber = lineNumber;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String[] getParamTypes() {
		return paramTypes;
	}

	public void setParamTypes(String[] paramTypes) {
		this.paramTypes = paramTypes;
	}

	public boolean isSynthetic() {
		return synthetic;
	}

	public void setSynthetic(boolean synthetic) {
		this.synthetic = synthetic;
	}

	public boolean isGenerated() {
		return generated;
	}

	public void setGenerated(boolean generated) {
		this.generated = generated;
	}

	public long getCallId() {
		return callId;
	}

	public void setCallId(long callId) {
		this.callId = callId;
	}	
}
