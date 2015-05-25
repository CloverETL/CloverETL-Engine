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

import org.jetel.component.Transform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CloverPublicAPI;

/**
 * Interface to be implemented by classes implementing denormalization, i.e. composition of one output record from
 * several input records.
 *
 * @author Jan Hadrava, Javlin a.s. &lt;jan.hadrava@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 17th June 2010
 * @created 21st November 2006
 *
 * @see org.jetel.component.Denormalizer
 */
@CloverPublicAPI
public interface RecordDenormalize extends Transform {

	/** the return value of the transform() method specifying that the record will be sent to output port */
	public static final int OK = 0;
	/** the return value of the transform() method specifying that the record will be skipped */
	public static final int SKIP = -1;

	/**
	 * Initializes normalize class/function. This method is called only once at the beginning of normalization process.
	 * Any object allocation/initialization should happen here.
	 *
	 * @param parameters Global graph parameters and parameters defined specially for the component which calls this
	 * transformation class
	 * @param sourceMetadata Metadata describing source data records
	 * @param targetMetadata Metadata describing target data record
	 * @return true if OK, otherwise false
	 */
	public boolean init(Properties parameters, DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException;

	/**
	 * Passes one input record to the composing class.
	 * 
	 * @param inRecord
	 * @return < -1 -- fatal error / user defined<br/>
	 * -1 -- error / skip record<br/>
	 * >= 0 -- OK
	 * @throws TransformException
	 * @Deprecated invoke {@link #append(DataRecord, DataRecord)} instead
	 */
	@Deprecated
	public int append(DataRecord inRecord) throws TransformException;

	/**
	 * Passes one input record to the composing class.
	 * 
	 * @param inRecord
	 * @param outRecord final group transformation can be performed already in append method
	 * @return < -1 -- fatal error / user defined<br/>
	 * -1 -- error / skip record<br/>
	 * >= 0 -- OK
	 * @throws TransformException
	 */
	public int append(DataRecord inRecord, DataRecord outRecord) throws TransformException;

	/**
	 * Passes one input record to the composing class. Called only if {@link #append(DataRecord)} throws an exception.
	 * 
	 * @param exception an exception that caused {@link #append(DataRecord)} to fail
	 * @param inRecord
	 *
	 * @return < -1 -- fatal error / user defined<br/>
	 * -1 -- error / skip record<br/>
	 * >= 0 -- OK
	 *
	 * @throws TransformException
	 * @Deprecated invoke {@link #appendOnError(Exception, DataRecord, DataRecord)} instead
	 */
	@Deprecated
	public int appendOnError(Exception exception, DataRecord inRecord) throws TransformException;

	/**
	 * Passes one input record to the composing class. Called only if {@link #append(DataRecord)} throws an exception.
	 * 
	 * @param exception an exception that caused {@link #append(DataRecord)} to fail
	 * @param inRecord
	 *
	 * @return < -1 -- fatal error / user defined<br/>
	 * -1 -- error / skip record<br/>
	 * >= 0 -- OK
	 *
	 * @throws TransformException
	 * 
	 */
	public int appendOnError(Exception exception, DataRecord inRecord, DataRecord outRecord) throws TransformException;

	/**
	 * Retrieves composed output record.
	 * 
	 * @param outRecord
	 * @return < -1 -- fatal error / user defined<br/>
	 * -1 -- error / skip record<br/>
	 * >= 0 -- OK
	 * @throws TransformException
	 * @Deprecated invoke {@link #transform(DataRecord, DataRecord)} instead
	 */
	@Deprecated
	public int transform(DataRecord outRecord) throws TransformException;

	/**
	 * Retrieves composed output record.
	 * 
	 * @param inRecord last input record from the group
	 * @param outRecord
	 * @return < -1 -- fatal error / user defined<br/>
	 * -1 -- error / skip record<br/>
	 * >= 0 -- OK
	 * @throws TransformException
	 */
	public int transform(DataRecord inRecord, DataRecord outRecord) throws TransformException;

	/**
	 * Retrieves composed output record. Called only if {@link #transform(DataRecord)} throws an exception.
	 * 
	 * @param exception an exception that caused {@link #transform(DataRecord)} to fail
	 * @param outRecord
	 *
	 * @return < -1 -- fatal error / user defined<br/>
	 * -1 -- error / skip record<br/>
	 * >= 0 -- OK
	 *
	 * @throws TransformException
	 * @Deprecated invoke {@link #transformOnError(Exception, DataRecord, DataRecord)} instead
	 */
	@Deprecated
	public int transformOnError(Exception exception, DataRecord outRecord) throws TransformException;

	/**
	 * Retrieves composed output record. Called only if {@link #transform(DataRecord)} throws an exception.
	 * 
	 * @param exception an exception that caused {@link #transform(DataRecord)} to fail
	 * @param inRecord last input record from the group
	 * @param outRecord
	 *
	 * @return < -1 -- fatal error / user defined<br/>
	 * -1 -- error / skip record<br/>
	 * >= 0 -- OK
	 *
	 * @throws TransformException
	 */
	public int transformOnError(Exception exception, DataRecord inRecord, DataRecord outRecord) throws TransformException;

	/**
	 * Finalize current round/clean after current round - called after the transform method was called for the input
	 * record
	 */
	public void clean();

}
