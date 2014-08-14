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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.jetel.data.Defaults;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.string.StringUtils;

/**
 * This class represents type of allocation of a single component.
 * Currently supported allocations:
 * 		- derived from neigbours
 * 		- derived from a component
 * 		- derived from a partitioned sandbox
 * 		- derived from a number requested partitions
 * 		- allocation on all available cluster nodes
 * 		- allocation on list of specified cluster nodes
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4. 4. 2014
 */
public abstract class EngineComponentAllocation {
	
	/** List of allocation parsers. See {@link #fromString(String)} */
	private static List<AllocationParser> allocationParsers = new ArrayList<>();
	
	/**
	 * Adds new factory which creates allocation from a string representation.
	 * @param allocationParser
	 */
	public static void addAllocationParser(AllocationParser allocationParser) {
		allocationParsers.add(allocationParser);
	}

	/**
	 * Creates an allocation from given string representation.
	 * @param rawAllocation string representation of requested allocation
	 * @return component allocation from the given string representation
	 */
	public static EngineComponentAllocation fromString(String rawAllocation) {
		if (!StringUtils.isEmpty(rawAllocation)) {
			for (AllocationParser allocationParser : allocationParsers) {
				EngineComponentAllocation engineComponentAllocation = allocationParser.parse(rawAllocation);
				if (engineComponentAllocation != null) {
					return engineComponentAllocation;
				}
			}
			throw new JetelRuntimeException("Ivalid component allocation format: " + rawAllocation);
		} else {
			throw new JetelRuntimeException("Empty component allocation.");
		}
	}

	private EngineComponentAllocation() {
	}

	/**
	 * Returns string representation of this component allocation.
	 */
	@Override
	public String toString() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * This is factory which creates a component allocation from a given string allocation representation.
	 * @see EngineComponentAllocation#fromString(String)
	 */
	protected static abstract class AllocationParser {
		public abstract EngineComponentAllocation parse(String rawAllocation);
	}
	
	////////////////
	//all allocation
	public static class AllClusterNodesEngineComponentAllocation extends EngineComponentAllocation {
		public final static String PREFIX = "allClusterNodes:";
		
		public static AllClusterNodesEngineComponentAllocation INSTANCE = new AllClusterNodesEngineComponentAllocation(); 

		private AllClusterNodesEngineComponentAllocation() {
		}
		
		@Override
		public String toString() {
			return PREFIX;
		}
	}

	static {
		addAllocationParser(new AllocationParser() {
			@Override
			public EngineComponentAllocation parse(String rawAllocation) {
				return rawAllocation.equals(AllClusterNodesEngineComponentAllocation.PREFIX) ? AllClusterNodesEngineComponentAllocation.INSTANCE : null;
			}
		});
	}

	/**
	 * @return component allocation instance which is spread on all available cluster nodes
	 */
	public static AllClusterNodesEngineComponentAllocation createAllClusterNodesAllocation() {
		return AllClusterNodesEngineComponentAllocation.INSTANCE;
	}
	
	/**
	 * Converts this allocation to {@link AllClusterNodesEngineComponentAllocation}.
	 * Can be called only if {@link #isAllClusterNodesAllocation()}.
	 */
	public AllClusterNodesEngineComponentAllocation toAllClusterNodesAllocation() {
		return (AllClusterNodesEngineComponentAllocation) this;
	}

	/**
	 * @return true if this allocation is {@link AllClusterNodesEngineComponentAllocation}; false otherwise
	 */
	public boolean isAllClusterNodesAllocation() {
		return this instanceof AllClusterNodesEngineComponentAllocation;
	}

	//////////////////////
	//neighbour allocation
	public static class NeighboursEngineComponentAllocation extends EngineComponentAllocation {
		public final static String PREFIX = "neighbours:";

		public static NeighboursEngineComponentAllocation INSTANCE = new NeighboursEngineComponentAllocation(); 

		private NeighboursEngineComponentAllocation() {
		}

		@Override
		public String toString() {
			return PREFIX;
		}
	}

	static {
		addAllocationParser(new AllocationParser() {
			@Override
			public EngineComponentAllocation parse(String rawAllocation) {
				return rawAllocation.equals(NeighboursEngineComponentAllocation.PREFIX) ? NeighboursEngineComponentAllocation.INSTANCE : null;
			}
		});
	}

	/**
	 * @return component allocation instance which inherits allocation from neighbouring components
	 */
	public static NeighboursEngineComponentAllocation createNeighboursAllocation() {
		return NeighboursEngineComponentAllocation.INSTANCE;
	}
	
	/**
	 * Converts this allocation to {@link NeighboursEngineComponentAllocation}.
	 * Can be called only if {@link #isNeighboursAllocation()}.
	 */
	public NeighboursEngineComponentAllocation toNeighboursAllocation() {
		return (NeighboursEngineComponentAllocation) this;
	}

	/**
	 * @return true if this allocation is {@link NeighboursEngineComponentAllocation}; false otherwise
	 */
	public boolean isNeighboursAllocation() {
		return this instanceof NeighboursEngineComponentAllocation;
	}

	///////////////////
	//number allocation
	public static class NumberEngineComponentAllocation extends EngineComponentAllocation {
		public final static String PREFIX = "number:";

		private int number;
		
		private NumberEngineComponentAllocation(int number) {
			this.number = number;
		}
		
		public int getNumber() {
			return number;
		}

		@Override
		public String toString() {
			return PREFIX + number;
		}
	}

	static {
		addAllocationParser(new AllocationParser() {
			@Override
			public EngineComponentAllocation parse(String rawAllocation) {
				if (rawAllocation.startsWith(NumberEngineComponentAllocation.PREFIX)) {
					String numberStr = rawAllocation.substring(NumberEngineComponentAllocation.PREFIX.length());
					try {
						if (numberStr.startsWith("${") && numberStr.endsWith("}")) {
							return EngineComponentAllocation.createParameterNumberAllocation(numberStr);
						}
						int number = Integer.valueOf(numberStr);
						return EngineComponentAllocation.createNumberAllocation(number);
					} catch (NumberFormatException e) {
						throw new JetelRuntimeException("Ivalid component allocation format: " + rawAllocation + ".");
					}
				} else {
					return null;
				}
			}
		});
	}

	/**
	 * @return component allocation instance which sits on specified number of cluster nodes
	 */
	public static NumberEngineComponentAllocation createNumberAllocation(int number) {
		return new NumberEngineComponentAllocation(number);
	}
	
	/**
	 * Converts this allocation to {@link NumberEngineComponentAllocation}.
	 * Can be called only if {@link #isNumberAllocation()}.
	 */
	public NumberEngineComponentAllocation toNumberAllocation() {
		return (NumberEngineComponentAllocation) this;
	}

	/**
	 * @return true if this allocation is {@link NumberEngineComponentAllocation}; false otherwise
	 */
	public boolean isNumberAllocation() {
		return this instanceof NumberEngineComponentAllocation;
	}
	
	///////////////////
	//number in parameter allocation
	public static class ParameterNumberEngineComponentAllocation extends EngineComponentAllocation {
		private String paramString;

		private ParameterNumberEngineComponentAllocation(String paramString) {
			this.paramString = paramString;
		}

		public String getParamString() {
			return paramString;
		}

		@Override
		public String toString() {
			return NumberEngineComponentAllocation.PREFIX + paramString;
		}
	}

	/**
	 * @return component allocation instance which sits on specified number of cluster nodes, number specified by parameter
	 */
	public static ParameterNumberEngineComponentAllocation createParameterNumberAllocation(String paramString) {
		return new ParameterNumberEngineComponentAllocation(paramString);
	}
	
	/**
	 * Converts this allocation to {@link ParameterNumberEngineComponentAllocation}.
	 * Can be called only if {@link #isParameterNumberAllocation()}.
	 */
	public ParameterNumberEngineComponentAllocation toParameterNumberAllocation() {
		return (ParameterNumberEngineComponentAllocation) this;
	}

	/**
	 * @return true if this allocation is {@link ParameterNumberEngineComponentAllocation}; false otherwise
	 */
	public boolean isParameterNumberAllocation() {
		return this instanceof ParameterNumberEngineComponentAllocation;
	}

	////////////////////
	//sandbox allocation
	public static class SandboxEngineComponentAllocation extends EngineComponentAllocation {
		public final static String PREFIX = "sandbox:";

		private String sandboxId;
		
		private SandboxEngineComponentAllocation(String sandboxId) {
			this.sandboxId = sandboxId;
		}
		
		public String getSandboxId() {
			return sandboxId;
		}

		@Override
		public String toString() {
			return PREFIX + sandboxId;
		}
	}

	static {
		addAllocationParser(new AllocationParser() {
			@Override
			public EngineComponentAllocation parse(String rawAllocation) {
				if (rawAllocation.startsWith(SandboxEngineComponentAllocation.PREFIX)) {
					String sandboxId = rawAllocation.substring(SandboxEngineComponentAllocation.PREFIX.length());
					if (StringUtils.isEmpty(sandboxId)) {
						throw new JetelRuntimeException("Ivalid component allocation format: " + rawAllocation + ".");
					}
					return EngineComponentAllocation.createSandboxAllocation(sandboxId);
				} else {
					return null;
				}
			}
		});
	}

	/**
	 * @return component allocation instance which is inherited from locations of a partitioned sandbox
	 */
	public static SandboxEngineComponentAllocation createSandboxAllocation(String sandbox) {
		return new SandboxEngineComponentAllocation(sandbox);
	}
	
	/**
	 * Converts this allocation to {@link SandboxEngineComponentAllocation}.
	 * Can be called only if {@link #isSandboxAllocation()}.
	 */
	public SandboxEngineComponentAllocation toSandboxAllocation() {
		return (SandboxEngineComponentAllocation) this;
	}

	/**
	 * @return true if this allocation is {@link SandboxEngineComponentAllocation}; false otherwise
	 */
	public boolean isSandboxAllocation() {
		return this instanceof SandboxEngineComponentAllocation;
	}

	//////////////////////////
	//cluster nodes allocation
	public static class ClusterNodesEngineComponentAllocation extends EngineComponentAllocation {
		public final static String PREFIX = "clusterNodes:";

		private List<String> clusterNodes;
		
		private ClusterNodesEngineComponentAllocation(List<String> clusterNodes) {
			this.clusterNodes = clusterNodes;
		}
		
		public List<String> getClusterNodes() {
			return clusterNodes;
		}
	
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(PREFIX);
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
	}

	static {
		addAllocationParser(new AllocationParser() {
			@Override
			public EngineComponentAllocation parse(String rawAllocation) {
				if (rawAllocation.startsWith(ClusterNodesEngineComponentAllocation.PREFIX)) {
					String clusterNodeIds = rawAllocation.substring(ClusterNodesEngineComponentAllocation.PREFIX.length());
					if (StringUtils.isEmpty(clusterNodeIds)) {
						throw new JetelRuntimeException("Ivalid component allocation format: " + rawAllocation + ".");
					}
					return EngineComponentAllocation.createClusterNodesAllocation(Arrays.asList(clusterNodeIds.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX)));
				} else {
					return null;
				}
			}
		});
	}

	/**
	 * @return component allocation instance which sits on specified cluster nodes
	 */
	public static ClusterNodesEngineComponentAllocation createClusterNodesAllocation(List<String> clusterNodes) {
		return new ClusterNodesEngineComponentAllocation(clusterNodes);
	}
	
	/**
	 * Converts this allocation to {@link ClusterNodesEngineComponentAllocation}.
	 * Can be called only if {@link #isClusterNodesAllocation()}.
	 */
	public ClusterNodesEngineComponentAllocation toClusterNodesAllocation() {
		return (ClusterNodesEngineComponentAllocation) this;
	}

	/**
	 * @return true if this allocation is {@link ClusterNodesEngineComponentAllocation}; false otherwise
	 */
	public boolean isClusterNodesAllocation() {
		return this instanceof ClusterNodesEngineComponentAllocation;
	}

	//////////////////////
	//component allocation
	public static class ComponentEngineComponentAllocation extends EngineComponentAllocation {
		public final static String PREFIX = "component:";

		private String componentId;
		
		private ComponentEngineComponentAllocation(String componentId) {
			this.componentId = componentId;
		}
		
		public String getComponentId() {
			return componentId;
		}

		@Override
		public String toString() {
			return PREFIX + componentId;
		}
	}

	static {
		addAllocationParser(new AllocationParser() {
			@Override
			public EngineComponentAllocation parse(String rawAllocation) {
				if (rawAllocation.startsWith(ComponentEngineComponentAllocation.PREFIX)) {
					String componentId = rawAllocation.substring(ComponentEngineComponentAllocation.PREFIX.length());
					if (StringUtils.isEmpty(componentId)) {
						throw new JetelRuntimeException("Ivalid component allocation format: " + rawAllocation + ".");
					}
					return EngineComponentAllocation.createComponentAllocation(componentId);
				} else {
					return null;
				}
			}
		});
	}

	/**
	 * @return component allocation instance which is derived from allocation of a specified component
	 */
	public static ComponentEngineComponentAllocation createComponentAllocation(String component) {
		return new ComponentEngineComponentAllocation(component);
	}
	
	/**
	 * Converts this allocation to {@link ComponentEngineComponentAllocation}.
	 * Can be called only if {@link #isComponentAllocation()}.
	 */
	public ComponentEngineComponentAllocation toComponentAllocation() {
		return (ComponentEngineComponentAllocation) this;
	}

	/**
	 * @return true if this allocation is {@link ComponentEngineComponentAllocation}; false otherwise
	 */
	public boolean isComponentAllocation() {
		return this instanceof ComponentEngineComponentAllocation;
	}

}
