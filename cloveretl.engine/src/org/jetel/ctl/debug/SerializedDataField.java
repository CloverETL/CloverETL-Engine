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

import org.jetel.data.DataField;
import org.jetel.data.DataFieldFactory;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.8.2016
 */
public class SerializedDataField implements Serializable {

	private static final long serialVersionUID = 1L;

	private DataFieldMetadata metadata;
	private byte serializedField[];
	private transient DataField dataField;
	
	public static SerializedDataField fromDataField(DataField field) {
		SerializedDataField result = new SerializedDataField();
		result.metadata = field.getMetadata();
		CloverBuffer buffer = CloverBuffer.allocate(Defaults.Record.FIELD_INITIAL_SIZE, false);
		field.serialize(buffer);
		byte content[] = new byte[buffer.position()];
		buffer.flip().get(content);
		result.serializedField = content;
		return result;
	}
	
	public DataField getDataField() {
		if (dataField == null) {
			DataField field = DataFieldFactory.createDataField(metadata, true);
			CloverBuffer buffer = CloverBuffer.wrap(serializedField);
			field.deserialize(buffer);
			dataField = field;
		}
		return dataField;
	}
}
