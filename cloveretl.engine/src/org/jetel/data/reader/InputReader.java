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
package org.jetel.data.reader;

import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;

/**
 * @author kuglerm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.10.2012
 */
public abstract class InputReader implements IInputReader {
	
	protected static final int CURRENT = 0;
	protected static final int NEXT = 1;
	
	protected int recCounter;
	protected RecordKey key;
	protected DataRecord[] rec = new DataRecord[2];
	
	protected static InputOrdering updateOrdering(int comparison, InputOrdering inputOrdering) {
		if (comparison > 0){
			if (inputOrdering!=InputOrdering.DESCENDING)
				inputOrdering = (inputOrdering==InputOrdering.UNDEFINED ? InputOrdering.DESCENDING : InputOrdering.UNSORTED ) ;
		} 
		if (comparison < 0){
			if (inputOrdering!=InputOrdering.ASCENDING)
				inputOrdering = (inputOrdering==InputOrdering.UNDEFINED ? InputOrdering.ASCENDING : InputOrdering.UNSORTED ) ;
		}
		return inputOrdering;
	}
	
	@Override
	abstract public InputOrdering getOrdering();

	@Override
	abstract public boolean loadNextRun() throws InterruptedException, IOException;

	@Override
	abstract public void free();

	@Override
	abstract public void reset() throws ComponentNotReadyException;;

	@Override
	abstract public DataRecord next() throws IOException, InterruptedException;

	@Override
	abstract public void rewindRun();

	@Override
	abstract public DataRecord getSample();

	@Override
	abstract public RecordKey getKey();

	@Override
	abstract public int compare(IInputReader other);

	@Override
	abstract public boolean hasData();

	@Override
	public String getInfo() {
		StringBuilder sb = new StringBuilder();
		sb.append("Record #");
		sb.append(recCounter).append(": Key field");
		if (key.getKeyFields().length > 1) {
			sb.append("s");
		}
		sb.append("=\"");
		for (String index : key.getKeyFieldNames()) {
			sb.append(index).append(":"); 
		}
		sb.replace(sb.length()-1, sb.length(), "\"").append(". ");
		if (rec[NEXT] == null) {
			sb.append("Current=null; ");
		} else {
			sb.append("Current=\"");
			for (String index : key.getKeyFieldNames()) {
				sb.append(index).append(":").append(rec[NEXT].getField(index)).append(" "); 
			}
			sb.replace(sb.length()-1, sb.length(), "\"").append("; ");
		}
		if (rec[CURRENT] == null) {
			sb.append("Previous=null.");
		} else {
			sb.append("Previous=\"");
			for (String index : key.getKeyFieldNames()) {
				sb.append(index).append(":").append(rec[CURRENT].getField(index)).append(" "); 
			}
			sb.replace(sb.length()-1, sb.length(), "\"").append(".");
		}
		return sb.toString();
	}

}
