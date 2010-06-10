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

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Interface to be implemented by classes implementing denormalization, ie composition of
 * one output record from several input records.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/21/06  
 * @see org.jetel.component.Denormalizer
 *
 */
public interface RecordDenormalize {

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
     * This is also initialization method, which is invoked before each separate graph run.
     * Contrary the init() procedure here should be allocated only resources for this graph run.
     * All here allocated resources should be released in #postExecute() method.
     * 
     * @throws ComponentNotReadyException some of the required resource is not available or other
     * precondition is not accomplish
     */
    public void preExecute() throws ComponentNotReadyException; 

	/**
	 * Releases used resources.
     * 
     * @param transactionMethod type of transaction finalize method; was the graph/phase run successful?
     * @throws ComponentNotReadyException
     */
    public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException;

	/**
	 * Passes one input record to the composing class.
	 * @param inRecord
	 * @return < -1 -- fatal error / user defined
	 *           -1 -- error / skip record
	 *         >= 0 -- OK
	 * @throws TransformException
	 */
	public int append(DataRecord inRecord) throws TransformException;

	/**
	 * Retrieves composed output record.
	 * @param outRecord
	 * @return < -1 -- fatal error / user defined
	 *           -1 -- error / skip record
	 *         >= 0 -- OK
	 * @throws TransformException
	 */
	public int transform(DataRecord outRecord) throws TransformException;


	/**
	 * Finalize current round/clean after current round - called after the transform method was called for the input record
	 */
	public void clean();

	/**
	 * Use postExecuste method.
	 */
	@Deprecated
	public void finished();

	/**
	 *  Returns description of error if one of the methods failed. Can be
	 * also used to get any message produced by transformation.
	 *
	 */
	public String getMessage();

	/**
	 * Use preExecute method.
	 */
	@Deprecated
	public void reset();
	
	/**
	 *  Passes instance of transformation graph to denormalize transformation
	 */
	public void setGraph(TransformationGraph graph);
	
	/**
	 *  Returns the instance of graph actually set for denormalize transformation
	 */
	public TransformationGraph getGraph();

}
