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

import org.jetel.util.FileType;
import org.jetel.util.string.StringUtils;

/**
 * This enumeration represents various kind of jobs from clover engine family.
 * For now only ETL graph and jobflow types are considered.
 * Each graph has specified its jobType, {@link TransformationGraph#getJobType()}.
 * Job type is defined in *.grf file as an XML attribute of top level 'Graph' element.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2 May 2012
 */
public enum JobType {

	/** This type represents regular ETL graphs */
	ETL_GRAPH("etlGraph", FileType.ETL_GRAPH),
	/** This type represents jobflows */
	JOBFLOW("jobflow", FileType.JOBFLOW);

	/** This type is used in case the type is not specified in different way. */
	public static JobType DEFAULT = ETL_GRAPH;

	private String id;
	
	/** Associated file type. */
	private FileType fileType;
	
	private JobType(String id, FileType fileType) {
		this.id = id;
		this.fileType = fileType;
		
	}
	
	@Override
	public String toString() {
		return id;
	}
	
	/**
	 * @return the fileType
	 */
	public FileType getFileType() {
		return fileType;
	}

	/**
	 * @return job type based on given type identifier
	 */
	public static JobType fromString(String jobTypeId) {
		if (StringUtils.isEmpty(jobTypeId)) {
			return DEFAULT;
		}
		for (JobType jobType : values()) {
			if (jobTypeId.equals(jobType.id)) {
				return jobType;
			}
		}
		throw new IllegalArgumentException("unknown job type " + jobTypeId);
	}
	
	/**
	 * Detection of job type based on file name of graph or jobflow.
	 */
	public static JobType fromFileName(String fileName) {
		if (StringUtils.isEmpty(fileName)) {
			return DEFAULT;
		}
		fileName = fileName.toLowerCase();
		for (JobType jobType: values()) {
			if (fileName.endsWith("." + jobType.fileType.getExtension())) {
				return jobType;
			}
		}
		throw new IllegalArgumentException("unknown job type associated with file name " + fileName);
	}
	
}
