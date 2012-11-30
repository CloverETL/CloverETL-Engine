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
		return componentType.equals(CLUSTER_PARTITION_TYPE);
	}

}
