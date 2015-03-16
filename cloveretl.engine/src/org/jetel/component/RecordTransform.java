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
package org.jetel.component;

import java.util.Properties;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CloverPublicAPI;

/**
 * Interface used by all components performing some sort of reformat operation - Reformat, Join, etc.<br>
 * For most transformations, it is better to start with subclassing DataRecordTransform class which provides default
 * implementation for most methods prescribed by this interface.<br>
 * <h4>Order of execution/methods call</h4>
 * <ol>
 * <li>setGraph()</li>
 * <li>init()</li>
 * <li>transform() <i>for each input&amp;output records pair</i></li>
 * <li><i>optionally</i> getMessage() <i>or</i> signal() <i>or</i> getSemiResult()</li>
 * <li>finished()
 * </ol>
 * 
 * @author David Pavlis, Javlin a.s. &lt;david.pavlis@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 17th June 2010
 * @created 4th February 2003
 */
@CloverPublicAPI
public interface RecordTransform extends Transform {

	/** the return value of the transform() method specifying that the record will be sent to all the output ports */
	public static final int ALL = Integer.MAX_VALUE;
	/** the return value of the transform() method specifying that the record will be sent to the first output port */
	public static final int OK = 0;
	/** the return value of the transform() method specifying that the record will be skipped */
	public static final int SKIP = -1;
	/** the return value of the transform() method, all values lesser or equal this value are considered as errors */
	public static final int STOP = -2;
	/** 
	 * the return value of the transform() method, all values lesser or equal this value are considered as errors 
	 * @deprecated use STOP constant instead
	 */
	@Deprecated
	public static final int ERROR = -2;

	/**
	 * Initializes reformat class/function. This method is called only once at the beginning of transformation process.
	 * Any object allocation/initialization should happen here.
	 * 
	 *@param parameters component attributes defined for the component which calls this transformation class
	 *@param sourcesMetadata Metadata describing source data records [array]
	 *@param targetMetadata Metadata describing target data record
	 *@return True if OK, otherwise False
	 */
	public boolean init(Properties parameters, DataRecordMetadata[] sourcesMetadata, DataRecordMetadata[] targetMetadata)
			throws ComponentNotReadyException;

	/**
	 * Performs reformat of source records to target records. This method is called as one step in transforming flow of
	 * records.<br>
	 * For example in simple reformat situation, for one input record, one output record has to be generated. Thus for
	 * each incoming record, this method is called by Reformat component.<br>
	 * The number of source records (sources[]) and target records (target[]) depends on particular component
	 * configuration. Can be 1:1 , N:1 or N:M.
	 * 
	 * @param sources Source DataRecords
	 * @param target Target DataRecord
	 * 
	 * @return RecordTransform.ALL -- send the data record(s) to all the output ports<br>
	 *         RecordTransform.SKIP -- skip the data record(s)<br>
	 *         >= 0 -- send the data record(s) to a specified output port<br>
	 *         < -1 -- fatal error / user defined
	 */
	public int transform(DataRecord[] sources, DataRecord[] target) throws TransformException;

	/**
	 * Performs reformat of source records to target records. This method is called as one step in transforming flow of
	 * records.<br>
	 * For example in simple reformat situation, for one input record, one output record has to be generated. Thus for
	 * each incoming record, this method is called by Reformat component.<br>
	 * The number of source records (sources[]) and target records (target[]) depends on particular component
	 * configuration. Can be 1:1 , N:1 or N:M.<br>
	 * Called only if {@link #transform(DataRecord[], DataRecord[])} throws an exception.
	 * 
	 * @param exception an exception that caused {@link #transform(DataRecord[], DataRecord[])} to fail
	 * @param sources Source DataRecords
	 * @param target Target DataRecord
	 * 
	 * @return RecordTransform.ALL -- send the data record(s) to all the output ports<br>
	 *         RecordTransform.SKIP -- skip the data record(s)<br>
	 *         >= 0 -- send the data record(s) to a specified output port<br>
	 *         < -1 -- fatal error / user defined
	 */
	public int transformOnError(Exception exception, DataRecord[] sources, DataRecord[] target) throws TransformException;
	
	/**
	 * Method which can be used for signalling into transformation that something outside happened.<br>
	 * For example in aggregation component key changed.
	 * 
	 * @param signalObject particular data object - depends on concrete implementation
	 * @deprecated shouldn't be used at all; this method is never invoked and will be removed in short future
	 */
	@Deprecated
	public void signal(Object signalObject);

	/**
	 * Method which can be used for getting intermediate results out of transformation. May or may not be implemented.
	 * 
	 * @return
	 * @deprecated shouldn't be used at all and will be removed in short future  
	 */
	@Deprecated
	public Object getSemiResult();

}
