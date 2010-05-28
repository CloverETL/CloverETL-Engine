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
package org.jetel.ctl;

import java.util.List;

import org.jetel.data.lookup.LookupTable;
import org.jetel.data.sequence.Sequence;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.metadata.DataRecordMetadata;


/**
 * Interface for the CTL compiler providing access to the external resources of a transformation graph  
 * than can be referenced from the CTL code such as metadata, sequences or lookup tables.
 * 
 * This is introduced since in some situation the transformation graph may not exist but the required
 * information about external resources in already known.
 * 
 * @author mtomcanyi
 *
 */
public interface TLContextProvider {

	public List<DataRecordMetadata> getDataRecordMetadata();
	
	public List<Sequence> getSequences();
	
	public List<LookupTable> getLookupTables();
	
	public Dictionary getDictionary();
	
}
