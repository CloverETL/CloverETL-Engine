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

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Interface to be implemented by classes implementing normalization, ie decomposition of
 * one input record to several output records.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/21/06  
 * @see org.jetel.component.Normalizer
 *
 */
public interface RecordNormalize {
	
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
	public boolean init(Properties parameters,
			DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
	throws ComponentNotReadyException;

	/**
	 * @param source Input record
	 * @return Number of output records which will be create from specified input record 
	 */
	public int count(DataRecord source) throws TransformException;
	
	/**
	 * 
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
	 * Finalize current round/clean after current round - called after the last transform method was called for the input record
	 */
	public void clean();
	
	/**
	 * Releases used resources.
	 */
	public void finished();

	/**
	 *  Returns description of error if one of the methods failed. Can be
	 * also used to get any message produced by transformation.
	 */
	public String getMessage();

	/**
	 * Resets normalizer for next graph execution. 
	 */	
	public void reset();

	/**
	 *  Passes instance of transformation graph to normalize transformation
	 */
	public void setGraph(TransformationGraph graph);
	
	/**
	 *  Returns the instance of graph actually set for normalize transformation
	 */
	public TransformationGraph getGraph();


}
