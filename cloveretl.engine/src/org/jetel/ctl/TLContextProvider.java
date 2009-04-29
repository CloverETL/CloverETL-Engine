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
