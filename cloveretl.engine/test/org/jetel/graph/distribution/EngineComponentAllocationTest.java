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
package org.jetel.graph.distribution;

import java.util.Arrays;

import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.distribution.EngineComponentAllocation.AllClusterNodesEngineComponentAllocation;
import org.jetel.graph.distribution.EngineComponentAllocation.ClusterNodesEngineComponentAllocation;
import org.jetel.graph.distribution.EngineComponentAllocation.ComponentEngineComponentAllocation;
import org.jetel.graph.distribution.EngineComponentAllocation.NeighboursEngineComponentAllocation;
import org.jetel.graph.distribution.EngineComponentAllocation.NumberEngineComponentAllocation;
import org.jetel.graph.distribution.EngineComponentAllocation.SandboxEngineComponentAllocation;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7. 4. 2014
 */
public class EngineComponentAllocationTest extends CloverTestCase {
	
	public void testFromString() {
		EngineComponentAllocation allocation;
		
		allocation = EngineComponentAllocation.fromString("allClusterNodes:");
		assertTrue(allocation.isAllClusterNodesAllocation());
		assertFalse(allocation.isClusterNodesAllocation());
		assertFalse(allocation.isComponentAllocation());
		assertFalse(allocation.isNeighboursAllocation());
		assertFalse(allocation.isNumberAllocation());
		assertFalse(allocation.isSandboxAllocation());
		assertEquals("allClusterNodes:", allocation.toString());
		assertTrue(allocation.toAllClusterNodesAllocation() instanceof AllClusterNodesEngineComponentAllocation);

		allocation = EngineComponentAllocation.fromString("clusterNodes:node1");
		assertFalse(allocation.isAllClusterNodesAllocation());
		assertTrue(allocation.isClusterNodesAllocation());
		assertFalse(allocation.isComponentAllocation());
		assertFalse(allocation.isNeighboursAllocation());
		assertFalse(allocation.isNumberAllocation());
		assertFalse(allocation.isSandboxAllocation());
		assertEquals("clusterNodes:node1", allocation.toString());
		assertTrue(allocation.toClusterNodesAllocation() instanceof ClusterNodesEngineComponentAllocation);
		assertEquals(Arrays.asList("node1"), allocation.toClusterNodesAllocation().getClusterNodes());

		allocation = EngineComponentAllocation.fromString("clusterNodes:node1;n2;n3");
		assertFalse(allocation.isAllClusterNodesAllocation());
		assertTrue(allocation.isClusterNodesAllocation());
		assertFalse(allocation.isComponentAllocation());
		assertFalse(allocation.isNeighboursAllocation());
		assertFalse(allocation.isNumberAllocation());
		assertFalse(allocation.isSandboxAllocation());
		assertEquals("clusterNodes:node1;n2;n3", allocation.toString());
		assertTrue(allocation.toClusterNodesAllocation() instanceof ClusterNodesEngineComponentAllocation);
		assertEquals(Arrays.asList("node1", "n2", "n3"), allocation.toClusterNodesAllocation().getClusterNodes());

		allocation = EngineComponentAllocation.fromString("component:abc");
		assertFalse(allocation.isAllClusterNodesAllocation());
		assertFalse(allocation.isClusterNodesAllocation());
		assertTrue(allocation.isComponentAllocation());
		assertFalse(allocation.isNeighboursAllocation());
		assertFalse(allocation.isNumberAllocation());
		assertFalse(allocation.isSandboxAllocation());
		assertEquals("component:abc", allocation.toString());
		assertTrue(allocation.toComponentAllocation() instanceof ComponentEngineComponentAllocation);
		assertEquals("abc", allocation.toComponentAllocation().getComponentId());

		allocation = EngineComponentAllocation.fromString("neighbours:");
		assertFalse(allocation.isAllClusterNodesAllocation());
		assertFalse(allocation.isClusterNodesAllocation());
		assertFalse(allocation.isComponentAllocation());
		assertTrue(allocation.isNeighboursAllocation());
		assertFalse(allocation.isNumberAllocation());
		assertFalse(allocation.isSandboxAllocation());
		assertEquals("neighbours:", allocation.toString());
		assertTrue(allocation.toNeighboursAllocation() instanceof NeighboursEngineComponentAllocation);

		allocation = EngineComponentAllocation.fromString("number:123");
		assertFalse(allocation.isAllClusterNodesAllocation());
		assertFalse(allocation.isClusterNodesAllocation());
		assertFalse(allocation.isComponentAllocation());
		assertFalse(allocation.isNeighboursAllocation());
		assertTrue(allocation.isNumberAllocation());
		assertFalse(allocation.isSandboxAllocation());
		assertEquals("number:123", allocation.toString());
		assertTrue(allocation.toNumberAllocation() instanceof NumberEngineComponentAllocation);
		assertEquals(123, allocation.toNumberAllocation().getNumber());

		allocation = EngineComponentAllocation.fromString("sandbox:abc123");
		assertFalse(allocation.isAllClusterNodesAllocation());
		assertFalse(allocation.isClusterNodesAllocation());
		assertFalse(allocation.isComponentAllocation());
		assertFalse(allocation.isNeighboursAllocation());
		assertFalse(allocation.isNumberAllocation());
		assertTrue(allocation.isSandboxAllocation());
		assertEquals("sandbox:abc123", allocation.toString());
		assertTrue(allocation.toSandboxAllocation() instanceof SandboxEngineComponentAllocation);
		assertEquals("abc123", allocation.toSandboxAllocation().getSandboxId());

		try {
			allocation = EngineComponentAllocation.fromString(null);
			assertTrue(false);
		} catch (JetelRuntimeException e) {
			//OK
		}

		try {
			allocation = EngineComponentAllocation.fromString("");
			assertTrue(false);
		} catch (JetelRuntimeException e) {
			//OK
		}

		try {
			allocation = EngineComponentAllocation.fromString("neighbours");
			assertTrue(false);
		} catch (JetelRuntimeException e) {
			//OK
		}

		try {
			allocation = EngineComponentAllocation.fromString("neighbours:neco");
			assertTrue(false);
		} catch (JetelRuntimeException e) {
			//OK
		}

		try {
			allocation = EngineComponentAllocation.fromString("clusterNodes:");
			assertTrue(false);
		} catch (JetelRuntimeException e) {
			//OK
		}

		try {
			allocation = EngineComponentAllocation.fromString("component:");
			assertTrue(false);
		} catch (JetelRuntimeException e) {
			//OK
		}

		try {
			allocation = EngineComponentAllocation.fromString("number");
			assertTrue(false);
		} catch (JetelRuntimeException e) {
			//OK
		}

		try {
			allocation = EngineComponentAllocation.fromString("number:abc");
			assertTrue(false);
		} catch (JetelRuntimeException e) {
			//OK
		}

		try {
			allocation = EngineComponentAllocation.fromString("sandbox");
			assertTrue(false);
		} catch (JetelRuntimeException e) {
			//OK
		}
	}
	
	public void testCreateAllClusterNodesAllocation() {
		EngineComponentAllocation allocation;
		
		allocation = EngineComponentAllocation.createAllClusterNodesAllocation();
		assertTrue(allocation.isAllClusterNodesAllocation());
		assertFalse(allocation.isClusterNodesAllocation());
		assertFalse(allocation.isComponentAllocation());
		assertFalse(allocation.isNeighboursAllocation());
		assertFalse(allocation.isNumberAllocation());
		assertFalse(allocation.isSandboxAllocation());
		assertEquals("allClusterNodes:", allocation.toString());
		assertTrue(allocation.toAllClusterNodesAllocation() instanceof AllClusterNodesEngineComponentAllocation);

		allocation = EngineComponentAllocation.createClusterNodesAllocation(Arrays.asList("node1"));
		assertFalse(allocation.isAllClusterNodesAllocation());
		assertTrue(allocation.isClusterNodesAllocation());
		assertFalse(allocation.isComponentAllocation());
		assertFalse(allocation.isNeighboursAllocation());
		assertFalse(allocation.isNumberAllocation());
		assertFalse(allocation.isSandboxAllocation());
		assertEquals("clusterNodes:node1", allocation.toString());
		assertTrue(allocation.toClusterNodesAllocation() instanceof ClusterNodesEngineComponentAllocation);
		assertEquals(Arrays.asList("node1"), allocation.toClusterNodesAllocation().getClusterNodes());

		allocation = EngineComponentAllocation.createClusterNodesAllocation(Arrays.asList("node1", "n2", "n3"));
		assertFalse(allocation.isAllClusterNodesAllocation());
		assertTrue(allocation.isClusterNodesAllocation());
		assertFalse(allocation.isComponentAllocation());
		assertFalse(allocation.isNeighboursAllocation());
		assertFalse(allocation.isNumberAllocation());
		assertFalse(allocation.isSandboxAllocation());
		assertEquals("clusterNodes:node1;n2;n3", allocation.toString());
		assertTrue(allocation.toClusterNodesAllocation() instanceof ClusterNodesEngineComponentAllocation);
		assertEquals(Arrays.asList("node1", "n2", "n3"), allocation.toClusterNodesAllocation().getClusterNodes());

		allocation = EngineComponentAllocation.createComponentAllocation("abc");
		assertFalse(allocation.isAllClusterNodesAllocation());
		assertFalse(allocation.isClusterNodesAllocation());
		assertTrue(allocation.isComponentAllocation());
		assertFalse(allocation.isNeighboursAllocation());
		assertFalse(allocation.isNumberAllocation());
		assertFalse(allocation.isSandboxAllocation());
		assertEquals("component:abc", allocation.toString());
		assertTrue(allocation.toComponentAllocation() instanceof ComponentEngineComponentAllocation);
		assertEquals("abc", allocation.toComponentAllocation().getComponentId());

		allocation = EngineComponentAllocation.createNeighboursAllocation();
		assertFalse(allocation.isAllClusterNodesAllocation());
		assertFalse(allocation.isClusterNodesAllocation());
		assertFalse(allocation.isComponentAllocation());
		assertTrue(allocation.isNeighboursAllocation());
		assertFalse(allocation.isNumberAllocation());
		assertFalse(allocation.isSandboxAllocation());
		assertEquals("neighbours:", allocation.toString());
		assertTrue(allocation.toNeighboursAllocation() instanceof NeighboursEngineComponentAllocation);

		allocation = EngineComponentAllocation.createNumberAllocation(123);
		assertFalse(allocation.isAllClusterNodesAllocation());
		assertFalse(allocation.isClusterNodesAllocation());
		assertFalse(allocation.isComponentAllocation());
		assertFalse(allocation.isNeighboursAllocation());
		assertTrue(allocation.isNumberAllocation());
		assertFalse(allocation.isSandboxAllocation());
		assertEquals("number:123", allocation.toString());
		assertTrue(allocation.toNumberAllocation() instanceof NumberEngineComponentAllocation);
		assertEquals(123, allocation.toNumberAllocation().getNumber());

		allocation = EngineComponentAllocation.createSandboxAllocation("abc123");
		assertFalse(allocation.isAllClusterNodesAllocation());
		assertFalse(allocation.isClusterNodesAllocation());
		assertFalse(allocation.isComponentAllocation());
		assertFalse(allocation.isNeighboursAllocation());
		assertFalse(allocation.isNumberAllocation());
		assertTrue(allocation.isSandboxAllocation());
		assertEquals("sandbox:abc123", allocation.toString());
		assertTrue(allocation.toSandboxAllocation() instanceof SandboxEngineComponentAllocation);
		assertEquals("abc123", allocation.toSandboxAllocation().getSandboxId());
	}

}
