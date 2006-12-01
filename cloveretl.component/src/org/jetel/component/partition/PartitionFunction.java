/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-05  David Pavlis <david_pavlis@hotmail.com> and others.
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
 * Created on 24.4.2005
 *
 */
package org.jetel.component.partition;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;

/**
 * Simple interface for partition functions.
 * 
 * @author david.pavlis
 * @since  1.3.2005
 */

public interface PartitionFunction {

	    
	    /**
	     * @param record data record which should be used for determining partition�
	     * number
	     * @return port number which should be used for sending
	     * data out.
	     */
	    int getOutputPort(DataRecord record);
	    
	    /**
	     * Called befor partiton function is first used (getOutputPort is used).
	     * @param numPartitions how many partitions we have
	     * @param recordKey set of fields composing key based on which should the
	     * partition be determined
	     */
	    
	    void init(int numPartitions,RecordKey partitionKey) throws ComponentNotReadyException;
	    
}
