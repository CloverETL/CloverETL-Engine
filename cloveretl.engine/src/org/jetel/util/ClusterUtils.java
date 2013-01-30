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

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26.11.2012
 */
public class ClusterUtils {

	public static final String CLUSTER_PARTITION_TYPE = "CLUSTER_PARTITION";
	public static final String CLUSTER_LOAD_BALANCING_PARTITION_TYPE = "CLUSTER_LOAD_BALANCING_PARTITION";
	
	public static final String CLUSTER_SIMPLE_GATHER_TYPE = "CLUSTER_SIMPLE_GATHER";
	public static final String CLUSTER_MERGE_TYPE = "CLUSTER_MERGE";

	public static final String CLUSTER_REPARTITION_TYPE = "CLUSTER_REPARTITION";
	public static final String CLUSTER_REGATHER_TYPE = "CLUSTER_REGATHER";

	public static final String REMOTE_EDGE_DATA_TRANSMITTER_TYPE = "REMOTE_EDGE_DATA_TRANSMITTER";
	public static final String REMOTE_EDGE_DATA_RECEIVER_TYPE = "REMOTE_EDGE_DATA_RECEIVER";
	
	/**
	 * Check whether the given component type is one of cluster components.
	 * @param componentType
	 * @return true if the given component is a cluster component
	 */
	public static boolean isClusterComponent(String componentType) {
		return CLUSTER_PARTITION_TYPE.equals(componentType) ||
				CLUSTER_LOAD_BALANCING_PARTITION_TYPE.equals(componentType) ||
				CLUSTER_SIMPLE_GATHER_TYPE.equals(componentType) ||
				CLUSTER_MERGE_TYPE.equals(componentType) ||
				CLUSTER_REPARTITION_TYPE.equals(componentType) ||
				CLUSTER_REGATHER_TYPE.equals(componentType) ||
				REMOTE_EDGE_DATA_TRANSMITTER_TYPE.equals(componentType) ||
				REMOTE_EDGE_DATA_RECEIVER_TYPE.equals(componentType);
	}
		
	/**
	 * Check whether the given component type is one of cluster gathers.
	 * @param componentType
	 * @return true if the given component is a cluster gather
	 */
	public static boolean isClusterGather(String componentType) {
		return componentType.equals(CLUSTER_SIMPLE_GATHER_TYPE) ||
				componentType.equals(CLUSTER_MERGE_TYPE);
	}
	
	/**
	 * Check whether the given component type is one of cluster partitioners.
	 * @param componentType
	 * @return true if the given component is a cluster partitioner
	 */
	public static boolean isClusterPartition(String componentType) {
		return componentType.equals(CLUSTER_PARTITION_TYPE) ||
				componentType.equals(CLUSTER_LOAD_BALANCING_PARTITION_TYPE);
	}

	/**
	 * Check whether the given component type matches with cluster partitioner component.
	 * @param componentType
	 * @return true if the given component is a cluster partitioner
	 */
	public static boolean isClusterRepartition(String componentType) {
		return componentType.equals(CLUSTER_REPARTITION_TYPE);
	}

	/**
	 * Check whether the given component type matches cluster regather component.
	 * @param componentType
	 * @return true if the given component is a cluster gather
	 */
	public static boolean isClusterRegather(String componentType) {
		return componentType.equals(CLUSTER_REGATHER_TYPE);
	}

	/**
	 * Check whether the given component type is one of the artificial components
	 * dedicated to be opposite site of remote edge. For example, these components
	 * are omitted in JMX tracking. 
	 * @param componentType
	 * @return true if the given component is 'remote edge' component 
	 */
	public static boolean isRemoteEdgeComponent(String componentType) {
		return componentType.equals(REMOTE_EDGE_DATA_TRANSMITTER_TYPE) ||
				componentType.equals(REMOTE_EDGE_DATA_RECEIVER_TYPE);
	}
	
}
