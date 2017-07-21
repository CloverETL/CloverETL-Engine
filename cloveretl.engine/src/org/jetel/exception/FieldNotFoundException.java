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
package org.jetel.exception;

import org.jetel.data.DataRecord;

/**
 * @author jakub (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.9.2012
 */
public class FieldNotFoundException extends RuntimeException {
	
	private static final long serialVersionUID = 1L;
	final String metadataName;
	final String fieldName;

	public FieldNotFoundException(final DataRecord dataRecord, final String fieldName) {
		this.metadataName = dataRecord.getMetadata().getName();
		this.fieldName = fieldName;
	}

	@Override
	public String getMessage() {
		return "Field named '" + this.fieldName + "' was not found in '" + this.metadataName + "' metadata.";
	}
	
	
}
