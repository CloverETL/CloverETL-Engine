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
package org.jetel.component.tree.reader;

import org.jetel.data.DataRecord;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;

/**
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 20, 2012
 */
public class FieldFillingException extends RuntimeException {

	private static final long serialVersionUID = 8128676699553841441L;

    private int port;
	private DataFieldMetadata fieldMetadata;
	private DataRecord incompleteRecord;
	
	// maybe MappingContext?
	
	public FieldFillingException(BadDataFormatException cause) {
		super(cause);
	}

	public void setPortIndex(int port) {
		this.port = port;
	}
	
	public int getPortIndex() {
		return port;
	}
	
	public void setIncompleteRecord(DataRecord rec) {
		incompleteRecord = rec.duplicate();
	}
	
	public DataRecord getIncompleteRecord() {
		return incompleteRecord;
	}

	public DataFieldMetadata getFieldMetadata() {
		return fieldMetadata;
	}

	public void setFieldMetadata(DataFieldMetadata fieldMetadata) {
		this.fieldMetadata = fieldMetadata;
	}

	@Override
	public BadDataFormatException getCause() {
		return (BadDataFormatException) super.getCause();
	}
}
