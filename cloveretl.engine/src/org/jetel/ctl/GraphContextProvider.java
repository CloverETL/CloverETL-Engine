package org.jetel.ctl;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.jetel.data.lookup.LookupTable;
import org.jetel.data.sequence.Sequence;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Implementation of the context provider for transformation graph.
 * The provider delegates its methods to the underlying {@link TransformationGraph}
 * so any changes to the graph will be reflected by the provider.
 *  
 * @author mtomcanyi
 *
 */
public class GraphContextProvider implements TLContextProvider {

	private final TransformationGraph graph;
	
	public GraphContextProvider(TransformationGraph graph) {
		this.graph = graph;
	}
	
	public List<DataRecordMetadata> getDataRecordMetadata() {
		final List<DataRecordMetadata> meta = new LinkedList<DataRecordMetadata>();
		final Iterator<String> mi = graph.getDataRecordMetadata();
		while (mi.hasNext()) {
			meta.add(graph.getDataRecordMetadata(mi.next()));
		}
		
		return meta;
	}
	
	public Dictionary getDictionary() {
		return graph.getDictionary();
	}
	
	public List<LookupTable> getLookupTables() {
		final List<LookupTable> lookups = new LinkedList<LookupTable>();
		final Iterator<String> li = graph.getLookupTables();
		while (li.hasNext()) {
			lookups.add(graph.getLookupTable(li.next()));
		}
		
		return lookups;
	}
	
	public List<Sequence> getSequences() {
		final Iterator<String> si = graph.getSequences();
		final List<Sequence> sequences = new LinkedList<Sequence>();
		while (si.hasNext()) {
			sequences.add(graph.getSequence(si.next()));
		}
		
		return sequences;
	}
	
}
