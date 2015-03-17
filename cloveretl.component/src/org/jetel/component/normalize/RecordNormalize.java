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

import org.jetel.component.Transform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CloverPublicAPI;

/**
 * Interface to be implemented by classes implementing normalization, i.e. decomposition of one input record to several
 * output records.
 *
 * @author Jan Hadrava, Javlin a.s. &lt;jan.hadrava@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 17th June 2010
 * @created 21st November 2006
 *
 * @see org.jetel.component.Normalizer
 */
@CloverPublicAPI
public interface RecordNormalize extends Transform {
	
	/** the return value of the transform() method specifying that the record will be sent to output port */
	public static final int OK = 0;
	/** the return value of the transform() method specifying that the record will be skipped */
	public static final int SKIP = -1;
	
	/**
	 *  Initializes normalize class/function. This method is called only once at the
	 * beginning of normalization process. Any object allocation/initialization should
	 * happen here.
	 *
	 *@param  parameters	   Global graph parameters and parameters defined specially for the
	 * component which calls this transformation class
	 *@param  sourceMetadata  Metadata describing source data records
	 *@param  targetMetadata   Metadata describing target data record
	 *@return                  True if OK, otherwise False
	 */
	public boolean init(Properties parameters, DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException;

	/**
	 * @param source Input record
	 * @return Number of output records which will be create from specified input record 
	 */
	public int count(DataRecord source) throws TransformException;

	/**
	 * Called only if {@link #count(DataRecord)} throws an exception.
	 *
	 * @param exception an exception that caused {@link #count(DataRecord)} to fail
	 * @param source Input record
	 *
	 * @return Number of output records which will be created from specified input record 
	 */
	public int countOnError(Exception exception, DataRecord source) throws TransformException;

	/**
	 * @param source Input record
	 * @param target Output records
	 * @param idx Sequential number of output record (starting from 0)
	 * @return < -1 -- fatal error / user defined
	 *           -1 -- error / skip record
	 *         >= 0 -- OK
	 * @throws TransformException
	 */
	public int transform(DataRecord source, DataRecord target, int idx) throws TransformException;

	/**
	 * Called only if {@link #transform(DataRecord, DataRecord, int)} throws an exception.
	 *
	 * @param exception an exception that caused {@link #transform(DataRecord, DataRecord, int)} to fail
	 * @param source Input record
	 * @param target Output records
	 * @param idx Sequential number of output record (starting from 0)
	 *
	 * @return < -1 -- fatal error / user defined
	 *           -1 -- error / skip record
	 *         >= 0 -- OK
	 *
	 * @throws TransformException
	 */
	public int transformOnError(Exception exception, DataRecord source, DataRecord target, int idx)
			throws TransformException;

	/**
	 * Finalize current round/clean after current round - called after the last transform method was called for the input record
	 */
	public void clean();

}
