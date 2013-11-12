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
package org.jetel.graph;

import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataStub;

/**
 * This factory class is intended to be used for factorisation of graph edges.
 * For now only two types of edges are distinguished - regular and jobflow edges.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2 May 2012
 */
public class EdgeFactory {

	/**
	 * Returns appropriate edge implementation for given parameters.
	 * Either regular {@link Edge} or {@link JobflowEdge} is returned based on {@link ContextProvider#getJobType()}.
	 */
	public static Edge newEdge(String id, DataRecordMetadata metadata, boolean debugMode) {
    	if (ContextProvider.getJobType() == JobType.JOBFLOW) {
			return new JobflowEdge(id, metadata, debugMode);
		} else {
			return new Edge(id, metadata, debugMode);
		}
	}
	
	/**
	 * Returns appropriate edge implementation for given parameters.
	 * Either regular {@link Edge} or {@link JobflowEdge} is returned based on {@link ContextProvider#getJobType()}.
	 */
	public static Edge newEdge(String id, DataRecordMetadata metadata) {
		return newEdge(id, metadata, false);
    }
    
	/**
	 * Creates new edge (see {@link #newEdge(String, DataRecordMetadata)}) derived
	 * from the given edge, with same setting (metadata, debug mode, ...).
	 */
	public static Edge newEdge(String id, Edge templateEdge) {
		Edge result = newEdge(id, templateEdge.getMetadata());
		result.copySettingsFrom(templateEdge);
		return result;
	}
	
	/**
	 * Returns appropriate edge implementation for given parameters.
	 * Either regular {@link Edge} or {@link JobflowEdge} is returned based on {@link ContextProvider#getJobType()}.
	 */
	public static Edge newEdge(String id, DataRecordMetadataStub metadataStub) {
    	if (ContextProvider.getJobType() == JobType.JOBFLOW) {
			return new JobflowEdge(id, metadataStub);
		} else {
			return new Edge(id, metadataStub);
		}
	}

}
