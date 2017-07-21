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
	
	@Override
	public List<DataRecordMetadata> getDataRecordMetadata() {
		final List<DataRecordMetadata> meta = new LinkedList<DataRecordMetadata>();
		final Iterator<String> mi = graph.getDataRecordMetadata();
		while (mi.hasNext()) {
			meta.add(graph.getDataRecordMetadata(mi.next()));
		}
		
		return meta;
	}
	
	@Override
	public Dictionary getDictionary() {
		return graph.getDictionary();
	}
	
	@Override
	public List<LookupTable> getLookupTables() {
		final List<LookupTable> lookups = new LinkedList<LookupTable>();
		final Iterator<String> li = graph.getLookupTables();
		while (li.hasNext()) {
			lookups.add(graph.getLookupTable(li.next()));
		}
		
		return lookups;
	}
	
	@Override
	public List<Sequence> getSequences() {
		final Iterator<String> si = graph.getSequences();
		final List<Sequence> sequences = new LinkedList<Sequence>();
		while (si.hasNext()) {
			sequences.add(graph.getSequence(si.next()));
		}
		
		return sequences;
	}
	
}
