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
import java.util.Iterator;
import java.util.List;

import org.jetel.data.Defaults;
import org.jetel.exception.JetelException;
import org.jetel.util.string.StringUtils;


/**
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 11.1.2010
 */
public class EngineComponentAllocation {

	public static final String SANDBOX_PREFIX = "sandbox:";

	public static final String CLUSTER_NODES_PREFIX = "clusterNodes:";
	
	public static final String COMPONENT_PREFIX = "component:";
	
	private static final EngineComponentAllocation INFERED_FROM_NEIGHBOURS;
	
	static {
		INFERED_FROM_NEIGHBOURS = new EngineComponentAllocation();
		INFERED_FROM_NEIGHBOURS.setNeighbours(true);
	}
	
	private boolean neighbours;

	private String sandboxId;
	
	private String componentId;
	
	//is not supported yet
	private List<String> clusterNodes;
	
	public static EngineComponentAllocation createBasedOnNeighbours() {
		return INFERED_FROM_NEIGHBOURS;
	}

	public static EngineComponentAllocation createBasedOnSandbox(String sandboxId) {
		if (StringUtils.isEmpty(sandboxId)) {
			throw new IllegalArgumentException("Sandbox id cannot be empty.");
		}
		EngineComponentAllocation componentAllocation = new EngineComponentAllocation();
		componentAllocation.setSandboxId(sandboxId);
		return componentAllocation;
	}

	public static EngineComponentAllocation createBasedOnComponentId(String componentId) {
		if (StringUtils.isEmpty(componentId)) {
			throw new IllegalArgumentException("Component id cannot be empty.");
		}
		EngineComponentAllocation componentAllocation = new EngineComponentAllocation();
		componentAllocation.setComponentId(componentId);
		return componentAllocation;
	}

	public static EngineComponentAllocation createBasedOnClusterNodes(List<String> clusterNodes) {
		if (clusterNodes == null || clusterNodes.size() == 0) {
			throw new IllegalArgumentException("Cluster nodes cannot be empty array.");
		}
		EngineComponentAllocation componentAllocation = new EngineComponentAllocation();
		componentAllocation.setClusterNodes(clusterNodes);
		return componentAllocation;
	}

	public static EngineComponentAllocation fromString(String rawAllocation) throws JetelException {
		if (rawAllocation.startsWith(SANDBOX_PREFIX)) {
			String sandboxId = rawAllocation.substring(SANDBOX_PREFIX.length());
			if (StringUtils.isEmpty(sandboxId)) {
				throw new JetelException("Ivalid component allocation format: " + rawAllocation + ".");
			}
			return EngineComponentAllocation.createBasedOnSandbox(sandboxId);
		} else if (rawAllocation.startsWith(COMPONENT_PREFIX)) {
			String componentId = rawAllocation.substring(COMPONENT_PREFIX.length());
			if (StringUtils.isEmpty(componentId)) {
				throw new JetelException("Ivalid component allocation format: " + rawAllocation + ".");
			}
			return EngineComponentAllocation.createBasedOnComponentId(componentId);
		} else if (rawAllocation.startsWith(CLUSTER_NODES_PREFIX)) {
			String clusterNodeIds = rawAllocation.substring(CLUSTER_NODES_PREFIX.length());
			if (StringUtils.isEmpty(clusterNodeIds)) {
				throw new JetelException("Ivalid component allocation format: " + rawAllocation + ".");
			}
			return EngineComponentAllocation.createBasedOnClusterNodes(Arrays.asList(clusterNodeIds.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX)));
		}
		throw new JetelException("Ivalid component allocation format: " + rawAllocation);
	}
	
	/**
	 * Only private constructor. 
	 */
	private EngineComponentAllocation() {
		//DO NOTHING
	}
	
	public boolean isInferedFromNeighbours() {
		return neighbours;
	}
	
	public boolean isInferedFromSandbox() {
		return sandboxId != null;
	}
	
	public boolean isInferedFromComponent() {
		return componentId != null;
	}
	
	public boolean isInferedFromClusterNodes() {
		return clusterNodes != null;
	}

	//GETTERS & SETTERS
	
	private void setNeighbours(boolean neighbours) {
		this.neighbours = neighbours;
	}

	public String getSandboxId() {
		return sandboxId;
	}

	private void setSandboxId(String sandboxId) {
		this.sandboxId = sandboxId;
	}

	public String getComponentId() {
		return componentId;
	}

	private void setComponentId(String componentId) {
		this.componentId = componentId;
	}

	public List<String> getClusterNodes() {
		return clusterNodes;
	}

	private void setClusterNodes(List<String> clusterNodes) {
		this.clusterNodes = clusterNodes;
	}
	
	@Override
	public String toString() {
		
		if (isInferedFromSandbox()) {
			return SANDBOX_PREFIX + sandboxId;
		}
		if (isInferedFromClusterNodes()) {
			StringBuilder sb = new StringBuilder();
			sb.append(CLUSTER_NODES_PREFIX);
			if (clusterNodes != null) {
				for (Iterator<String> i = clusterNodes.iterator(); i.hasNext();) {
					sb.append(i.next());
					if (i.hasNext()) {
						sb.append(Defaults.Component.KEY_FIELDS_DELIMITER);
					}
				}
			}
			return sb.toString();
		}
		if (isInferedFromComponent()) {
			return COMPONENT_PREFIX + componentId;
		}
		return super.toString();
	}
}
