/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.component;

import java.util.Properties;
import org.jetel.data.DataRecord;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Interface used by all components performing some sort of reformat operation -
 * Reformat, Join, etc.<br>
 * For most transformations, it is better to start with subclassing DataRecordTransform
 * class which provides default implementation for most methods prescribed by
 * this interface.
 *
 *@author      dpavlis
 *@created     February 4, 2003
 *@since       April 18, 2002
 *@revision    $Revision$
 */
public interface RecordTransform {

	/**
	 *  Initializes reformat class/function. This method is called only once at the
	 * beginning of transformation process. Any object allocation/initialization should
	 * happen here.
	 *
	 *@param  parameters	   Global graph parameters and parameters defined specially for the
	 * component which calls this transformation class
	 *@param  sourcesMetadata  Metadata describing source data records [array]
	 *@param  targetMetadata   Metadata describing target data record
	 *@return                  True if OK, otherwise False
	 */
	public boolean init(Properties parameters,DataRecordMetadata[] sourcesMetadata, DataRecordMetadata[] targetMetadata);


	/**
	 *  Performs reformat of source records to target records.
	 *  This method is called as one step in transforming flow of
	 * records.<br>
	 * For example in simple reformat situation, for one input record, one
	 * output record has to be generated. Thus for each incoming record, this
	 * method is called by Reformat component.<br>
	 * The number of source records (sources[]) and target records (target[])
	 * depends on particular component configuration. Can be 1:1 , N:1 or N:M.
	 *
	 *@param  sources  Source DataRecords
	 *@param  target   Target DataRecord
	 *@return          True if OK, otherwise False
	 */
	public boolean transform(DataRecord[] sources, DataRecord[] target);


	/**
	 *  Returns description of error if one of the methods failed. Can be
	 * also used to get any message produced by transformation.
	 *
	 *@return    Error message
	 *@since     April 18, 2002
	 */
	public String getMessage();

	
	
	/**
	 * Method which can be used for signalling into transformation
	 * that something outside happened.<br>
	 * For example in aggregation component key changed.
	 * 
	 * @param signalObject	particular data object - depends on concrete implementation
	 */
	public void signal(Object signalObject);
	
	
	/**
	 * Method which can be used for getting intermediate results out
	 * of transformation. May or may not be implemented.
	 * 
	 * @return
	 */
	public Object getSemiResult();
	
	/**
	 * Method called at the end of transformation process. No more
	 * records will be processed. The implementing class should release
	 * any resource reserved during init() or runtime at this point. 
	 */
	public void finished();
    
    /**
     * Method sets graph instance for this transformation implementation.
     * This variable provides all elements of runnig graph.
     * @param graph
     */
    public void setGraph(TransformationGraph graph);
	
}

