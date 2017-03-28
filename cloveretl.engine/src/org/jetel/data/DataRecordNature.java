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
package org.jetel.data;

import org.jetel.graph.JobType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * This enumeration represents various kind of data records.
 * For now only Data Record and Token natures are considered.
 * Token nature is required for jobflows.
 * Each graph metadata has specified its nature, {@link DataRecordMetadata#getNature()}.
 * Metadata nature can be defined in *.fmt (or in *.grf for internal metadata) file as an XML attribute of 'Record' element
 * or is derived from job type, see {@link DataRecordMetadata#getNature()}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17.5.2012
 */
public enum DataRecordNature {

	/** This nature represents regular data record, which is dedicated for ETL graphs */
	DATA_RECORD("dataRecord", JobType.ETL_GRAPH),
	/** This nature represents tokens, which are dedicate for jobflows */
	TOKEN("token", JobType.JOBFLOW);

	/** This nature is used in case the nature is not specified in different way. */
	public static DataRecordNature DEFAULT = DATA_RECORD;

	private String id;
	private JobType respectiveJobType;
	private DataRecordNature(String id, JobType respectiveJobType) {
		this.id = id;
		this.respectiveJobType = respectiveJobType;
	}
	
	@Override
	public String toString() {
		return id;
	}
	
	/**
	 * @return data record nature based on given nature identifier
	 */
	public static DataRecordNature fromString(String dataRecordNatureId) {
		if (StringUtils.isEmpty(dataRecordNatureId)) {
			return DEFAULT;
		}
		for (DataRecordNature dataRecordNature : values()) {
			if (dataRecordNatureId.equals(dataRecordNature.id)) {
				return dataRecordNature;
			}
		}
		throw new IllegalArgumentException("unknown data record nature " + dataRecordNatureId);
	}
	
	public static DataRecordNature fromJobType(JobType jobType) {
		if (jobType == null) {
			return DEFAULT;
		}
		for (DataRecordNature dataRecordNature : values()) {
			if (jobType.isSubTypeOf(dataRecordNature.respectiveJobType)) {
				return dataRecordNature;
			}
		}
		throw new IllegalArgumentException("unexpected job type " + jobType);
	}
	
}
