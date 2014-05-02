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
	JOBFLOW("jobflow", FileType.JOBFLOW),
	PROFILER_JOB("profilerJob", FileType.PROFILER_JOB),
	SUBGRAPH("subgraph", FileType.SUBGRAPH, ETL_GRAPH),
	SUBJOBFLOW("subjobflow", FileType.SUBJOBFLOW, JOBFLOW);

	/** This type is used in case the type is not specified in different way. */
	public static final JobType DEFAULT = ETL_GRAPH;

	private String id;
	
	/** Associated file type. */
	private FileType fileType;
	
	private JobType parent;
	
	private JobType(String id, FileType fileType) {
		this.id = id;
		this.fileType = fileType;
	}

	public String getId() {
		return id;
	}
	
	/** 
	 * Getter which returns response of name() method. 
	 * This getter is useful when the JobType is used as placeholder 
	 * and we must explicitly specify that it should return name() not toString() 
	 * - e.g. JSF may sometimes call toString() in some cases instead of name(). */ 
	public String getName() {
		return this.name();
	}
	
	private JobType(String id, FileType fileType, JobType parent) {
		this(id, fileType);
		this.parent = parent;
	}
	
	@Override
	public String toString() {
		return name();
	}
	
	/**
	 * @return the fileType
	 */
	public FileType getFileType() {
		return fileType;
	}
	
	/**
	 * Returns <code>true</code> if the current job type
	 * is a sub-type of the given job type (or the type itself).
	 * 
	 * @param parentType
	 * @return
	 */
	public boolean isSubTypeOf(JobType parentType) {
		return (this == parentType) || ((parent != null) && parent.isSubTypeOf(parentType));
	}
	
	/**
	 * Returns <code>true</code> if the current job type
	 * is {@link #ETL_GRAPH} or {@link #SUBGRAPH}.
	 * 
	 * @return <code>true</code> for {@link #ETL_GRAPH} or {@link #SUBGRAPH}
	 */
	public boolean isGraph() {
		return this.isSubTypeOf(ETL_GRAPH);
	}
	
	/**
	 * Returns <code>true</code> if the current job type
	 * is {@link #JOBFLOW} or {@link #SUBJOBFLOW}.
	 * 
	 * @return <code>true</code> for {@link #JOBFLOW} or {@link #SUBJOBFLOW}
	 */
	public boolean isJobflow() {
		return this.isSubTypeOf(JOBFLOW);
	}
	
	/**
	 * Returns <code>true</code> if the current job type
	 * is {@link #PROFILER_JOB}.
	 * 
	 * @return <code>true</code> for {@link #PROFILER_JOB}
	 */
	public boolean isProfilerJob() {
		return this.isSubTypeOf(PROFILER_JOB);
	}
	
	public JobType getBaseType() {
		return (parent == null) ? this : parent.getBaseType();
	}

	/**
	 * @return job type based on given type identifier
	 */
	public static JobType fromString(String jobTypeId) {
		if (StringUtils.isEmpty(jobTypeId)) {
			return DEFAULT;
		}
		for (JobType jobType : values()) {
			if (jobTypeId.equals(jobType.id) || jobTypeId.equals(jobType.name())) {
				return jobType;
			}
		}
		throw new IllegalArgumentException("unknown job type " + jobTypeId);
	}
	
	/**
	 * Detection of job type based on file name.
	 * For example, for *.grf is returned {@link #ETL_GRAPH}
	 * and for *.sgrf is returned {@link #SUBGRAPH}.
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

	/**
	 * This is identical with {@link #fromFileName(String)#getBaseType()}.
	 * For example, for both *.grf and *.sgrf is returned {@link #ETL_GRAPH}.
	 * 
	 * @param fileName
	 * @return base job type of job type derived from file name
	 */
	public static JobType baseTypeFromFileName(String fileName) {
		return fromFileName(fileName).getBaseType();
	}
	
	/**
	 * Detection of job type based on file name extension.
	 */
	public static JobType fromFileExtension(String fileExtension) {
		if (StringUtils.isEmpty(fileExtension)) {
			return DEFAULT;
		}
		for (JobType jobType : values()) {
			if (fileExtension.equalsIgnoreCase(jobType.fileType.getExtension())) {
				return jobType;
			}
		}
		throw new IllegalArgumentException("unknown job type associated with file extension " + fileExtension);
	}

	/**
	 * @return true for {@link JobType#SUBGRAPH} and {@link JobType#SUBJOBFLOW}; false otherwise
	 */
	public boolean isSubJob() {
		return parent != null;
	}

}
