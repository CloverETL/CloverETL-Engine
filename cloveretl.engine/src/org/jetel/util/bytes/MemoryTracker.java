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
package org.jetel.util.bytes;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;

/**
 * This class collect information about amount of allocated memory
 * by each particular node or graph. This tracker is closely
 * tight with {@link TransformationGraph}.
 * 
 * For now only tracked memory is memory occupied by {@link CloverBuffer}s.
 * So collected information is not accurate. 
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14 Nov 2011
 * 
 * @see TransformationGraph#getMemoryTracker()
 * @see CloverBuffer
 */
public class MemoryTracker {

    private final static Log logger = LogFactory.getLog(MemoryTracker.class);
	
	private long usedMemoryByGraph;
	
	private Map<Node, Integer> usedMemoryPerNode = new HashMap<Node, Integer>();
	
	/**
	 * Listener for a memory allocation event.
	 * Should be called whenever a significant amount of memory is allocated. 
	 */
	public synchronized void memoryAllocated(Node node, int memorySize) {
		if (node != null) {
			Integer usedMemoryByNode = usedMemoryPerNode.get(node);
			int newUsedMemoryByNode = memorySize;
			if (usedMemoryByNode != null) {
				newUsedMemoryByNode += usedMemoryByNode.intValue();
			}
			usedMemoryPerNode.put(node, Integer.valueOf(newUsedMemoryByNode));
		}
		
		usedMemoryByGraph += memorySize;
	}
	
	/**
	 * Listener for a memory de-allocation event.
	 * Should be called whenever a significant amount of memory is de-allocated. 
	 */
	public synchronized void memoryDeallocated(Node node, int memorySize) {
		if (node != null) {
			Integer usedMemoryByNode = usedMemoryPerNode.get(node);
			if (usedMemoryByNode != null) {
				usedMemoryPerNode.put(node, Integer.valueOf(usedMemoryByNode.intValue() - memorySize));
			} else {
				logger.warn("Unexpected issue in memory tracker. Deallocation performed without foregoing allocation.");
				return;
			}
		}
		
		usedMemoryByGraph -= memorySize;
	}
	
	/**
	 * @return number of bytes allocated in memory by the given node
	 */
	public synchronized int getUsedMemory(Node node) {
		Integer usedMemory = usedMemoryPerNode.get(node);
		return usedMemory != null ? usedMemory.intValue() : 0;
	}
	
	/**
	 * @return number of bytes allocated in memory by the graph
	 */
	public synchronized long getUsedMemory() {
		return usedMemoryByGraph;
	}
	
}
