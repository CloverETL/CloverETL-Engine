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
package org.jetel.component.tree.writer.util;

import org.jetel.metadata.DataFieldMetadata;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4 Feb 2011
 */
public class DataFieldMetadataWrapper {
	final int port;
	final int fieldIndex;
	final DataFieldMetadata dataFieldMetadata;
	final String namespace;
	
	public DataFieldMetadataWrapper(int port, int fieldIndex, DataFieldMetadata dataFieldMetadata, String namespace) {
		this.port = port;
		this.fieldIndex = fieldIndex;
		this.dataFieldMetadata = dataFieldMetadata;
		this.namespace = namespace;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dataFieldMetadata == null) ? 0 : dataFieldMetadata.hashCode());
		result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
		result = prime * result + port;
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof DataFieldMetadataWrapper)) {
			return false;
		}
		
		DataFieldMetadataWrapper other = (DataFieldMetadataWrapper) obj;
		if (dataFieldMetadata != other.dataFieldMetadata) {
			return false;
		}
		if (namespace == null) {
			if (other.namespace != null) {
				return false;
			}
		} else if (!namespace.equals(other.namespace)) {
			return false;
		}
		if (port != other.port) {
			return false;
		}
		return true;
	}

	public int getPort() {
		return port;
	}

	public int getFieldIndex() {
		return fieldIndex;
	}

	public DataFieldMetadata getDataFieldMetadata() {
		return dataFieldMetadata;
	}

	public String getNamespace() {
		return namespace;
	}
	
}
