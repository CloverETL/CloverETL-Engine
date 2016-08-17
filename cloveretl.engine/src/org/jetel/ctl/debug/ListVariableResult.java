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
import java.util.Collections;
import java.util.List;

/**
 * @author Jiri Musil (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created May 11, 2016
 */
public class ListVariableResult implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private List<Variable> globalVariables;
	private List<Variable> localVariables;
	private List<SerializedDataRecord> inputRecords;
	private List<SerializedDataRecord> outputRecords;
	
	public ListVariableResult() {}
	
	public ListVariableResult(List<Variable> globalVariables, List<Variable> localVariables) {
		this.globalVariables = globalVariables;
		this.localVariables = localVariables;	
	}

	public List<Variable> getGlobalVariables() {
		if (globalVariables == null) {
			return Collections.emptyList();
		}
		return globalVariables;
	}

	public void setGlobalVariables(List<Variable> globalVariables) {
		this.globalVariables = globalVariables;
	}

	public List<Variable> getLocalVariables() {
		if (localVariables == null) {
			return Collections.emptyList();
		}
		return localVariables;
	}

	public void setLocalVariables(List<Variable> localVariables) {
		this.localVariables = localVariables;
	}

	public List<SerializedDataRecord> getInputRecords() {
		if (inputRecords == null) {
			return Collections.emptyList();
		}
		return inputRecords;
	}

	public void setInputRecords(List<SerializedDataRecord> inputRecords) {
		this.inputRecords = inputRecords;
	}

	public List<SerializedDataRecord> getOutputRecords() {
		if (outputRecords == null) {
			return Collections.emptyList();
		}
		return outputRecords;
	}

	public void setOutputRecords(List<SerializedDataRecord> outputRecords) {
		this.outputRecords = outputRecords;
	}
}
