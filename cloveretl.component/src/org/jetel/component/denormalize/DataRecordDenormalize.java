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
package org.jetel.component.denormalize;

import java.util.Properties;

import org.jetel.component.AbstractDataTransform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CloverPublicAPI;

/**
 * Base class for various denormalization implementations.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/21/06  
 * @see org.jetel.component.Denormalizer
 */
@CloverPublicAPI
public abstract class DataRecordDenormalize extends AbstractDataTransform implements RecordDenormalize {

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

	@Deprecated
	@Override
	public int append(DataRecord inRecord) throws TransformException {
		throw new JetelRuntimeException("Abstract method 'append' is not implemented.");
	}

	@Override
	public int append(DataRecord inRecord, DataRecord outRecord) throws TransformException {
		return append(inRecord);
	}
	
	@Deprecated
	@Override
	public int appendOnError(Exception exception, DataRecord inRecord) throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Denormalization failed!", exception);
	}

	@Override
	public int appendOnError(Exception exception, DataRecord inRecord, DataRecord outRecord) throws TransformException {
		return appendOnError(exception, inRecord);
	}

	@Deprecated
	@Override
	public int transform(DataRecord outRecord) throws TransformException {
		throw new JetelRuntimeException("Abstract method 'transform' is not implemented.");
	}

	@Override
	public int transform(DataRecord inRecord, DataRecord outRecord) throws TransformException {
		return transform(outRecord);
	}
	
	@Override
	public int transformOnError(Exception exception, DataRecord outRecord) throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Denormalization failed!", exception);
	}

	@Override
	public int transformOnError(Exception exception, DataRecord inRecord, DataRecord outRecord) throws TransformException {
		return transformOnError(exception, outRecord);
	}
	
	@Override
	public void clean(){
		// do nothing by default
	}

}
