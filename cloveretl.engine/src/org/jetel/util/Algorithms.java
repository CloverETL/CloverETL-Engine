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
package org.jetel.util;

import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Oct 11, 2013
 */
public class Algorithms {

	public static interface AdjacencyFunction <T> {

		/**
		 * Returns a collection of vertices adjacent to the given vertex.
		 * 
		 * @param current the current vertex
		 * @return the neighbors of the given vertex
		 */
		public Collection<T> getAdjacentVertices(T current);
	}
	
	private static interface QueueManager<T> {
		public void add(Deque<T> collection, T element);
	}
	
	/**
	 * Pushes the element onto the stack represented by the {@link Deque}.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @param <T>
	 * @created 30. 1. 2014
	 */
	private static class DFSManager<T> implements QueueManager<T> {

		@Override
		public void add(Deque<T> collection, T element) {
			collection.push(element);
		}
		
	}
	
	/**
	 * Adds the element to the tail of the queue represented by the {@link Deque}.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @param <T>
	 * @created 30. 1. 2014
	 */
	private static class BFSManager<T> implements QueueManager<T> {

		@Override
		public void add(Deque<T> collection, T element) {
			collection.add(element);
		}
		
	}
	
	/**
	 * Performs BFS from the root using the provided adjacency function implementation.
	 * Returns the set of visited vertices, including the root.
	 * 
	 * @param root the starting vertex
	 * @param generator adjacency function implementation
	 * @return set of visited vertices
	 */
	public static <T> Set<T> breadthFirstSearch(T root, AdjacencyFunction<T> generator) {
		return search(root, generator, new BFSManager<T>());
	}
	
	/**
	 * Performs DFS from the root using the provided adjacency function implementation.
	 * Returns the set of visited vertices, including the root.
	 * 
	 * @param root the starting vertex
	 * @param generator adjacency function implementation
	 * @return set of visited vertices
	 */
	public static <T> Set<T> depthFirstSearch(T root, AdjacencyFunction<T> generator) {
		return search(root, generator, new DFSManager<T>());
	}

	private static <T> Set<T> search(T root, AdjacencyFunction<T> generator, QueueManager<T> queueManager) {
		Deque<T> queue = new LinkedList<T>();
		Set<T> visited = new HashSet<T>();
		queue.add(root);
		while (!queue.isEmpty()) {
			T current = queue.poll();
			visited.add(current);
			for (T neighbor: generator.getAdjacentVertices(current)) {
				if (!visited.contains(neighbor)) {
					queueManager.add(queue, neighbor);
				}
			}
		}
		return visited;
	}
	
}
