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
package org.jetel.data.lookup;

import java.util.Iterator;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;

/**
 * This interface serves as a provider to lookup table data. Whenever user wants to access to lookup table data
 * associated with a key, he has to create this 'proxy' object via createLookup() method invoked on a lookup table.
 * The given instance provides all appropriate records through the Iterator<DataRecord> interface.
 * The owner of this object can whenever change query to the lookup table and consequently call seek() method
 * to restart iterator on a different location of lookup table data. 
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 29.10.2008
 */
public interface Lookup extends Iterator<DataRecord> {

	public LookupTable getLookupTable();
	
	public RecordKey getKey();
	
	public void seek();
	
	public void seek(DataRecord keyRecord);
	
	public int getNumFound();
	
}
