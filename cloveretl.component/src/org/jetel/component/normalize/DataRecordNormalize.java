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
package org.jetel.component.normalize;

import java.util.Properties;

import org.jetel.component.AbstractDataTransform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CloverPublicAPI;

/**
 * Base class for various normalization implementations.
 * 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/21/06
 * @see org.jetel.component.Denormalizer
 */
@CloverPublicAPI
public abstract class DataRecordNormalize extends AbstractDataTransform implements RecordNormalize {

	protected Properties parameters;
	protected DataRecordMetadata sourceMetadata;
	protected DataRecordMetadata targetMetadata;

	@Override
	public boolean init(Properties parameters, DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		this.parameters = parameters;
		this.sourceMetadata = sourceMetadata;
		this.targetMetadata = targetMetadata;

		return init();
	}
	
	/**
	 * Override this method to provide user-desired initialization.
	 * 
	 * @throws ComponentNotReadyException if the initialization fails for any reason
	 */
	protected boolean init() throws ComponentNotReadyException {
		return true;
	}

	@Override
	public int countOnError(Exception exception, DataRecord source) throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Normalization failed!", exception);
	}

	@Override
	public int transformOnError(Exception exception, DataRecord source, DataRecord target, int idx)
			throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Normalization failed!", exception);
	}

	@Override
	public void clean() {
		// do nothing by default
	}

}
