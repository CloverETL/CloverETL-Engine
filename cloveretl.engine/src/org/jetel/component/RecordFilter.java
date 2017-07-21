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

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;

/**
 * Interface for record-filtering transformations based on boolean condition.
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 * @since  24.4.2009
 */

public interface RecordFilter {
	    
	    /**
	     * @param record data 
	     * @return true if valid record, false otherwise
	     */
	    boolean isValid(DataRecord record) throws TransformException;

	    /**
	     * Called before partition function is first used (getOutputPort is used).
	     * @param numPartitions how many partitions we have
	     * @param recordKey set of fields composing key based on which should the
	     * partition be determined
	     */
	    void init() throws ComponentNotReadyException;
	    
		/**
		 * Graph instance for descendant
		 * 
		 * @param graph
		 */
		public void setGraph(TransformationGraph graph);
		
}
