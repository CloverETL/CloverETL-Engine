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
 * This class is used for file type registration.
 * It can be used to obtain the default file type description for GUI.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Oct 15, 2012
 */
public enum FileType {

	ETL_GRAPH("grf"), //$NON-NLS-1$
	JOBFLOW("jbf"), //$NON-NLS-1$
	PROFILER_JOB("cpj"), //$NON-NLS-1$
	METADATA("fmt"), //$NON-NLS-1$
	CTL_TRANSFORMATION("ctl"), //$NON-NLS-1$
	SUB_GRAPH("subgrf"), //$NON-NLS-1$
	SUB_JOBFLOW("subjbf"); //$NON-NLS-1$
	
	public final String extension;
	
	private FileType(String extension) {
		this.extension = extension;
	}

	/**
	 * @return the extension
	 */
	public String getExtension() {
		return extension;
	}
	
	/**
	 * Returns the {@link FileType} associated with
	 * the given extension or <code>null</code> if
	 * no such association is found. 
	 * The extension should not contain the leading dot.
	 * 
	 * @param extension the file extension (without a dot)
	 * @return {@link FileType} associated with the <code>extension</code> or <code>null</code>
	 */
	public static FileType fromExtension(String extension) {
		if (extension == null) {
			return null;
		}
		extension = extension.toLowerCase();
		for (FileType t: values()) {
			if (t.extension.equals(extension)) {
				return t;
			}
		}
		return null;
	}
	
	/**
	 * 
	 * @param fileName
	 * @return
	 */
	public static FileType fromFileName(String fileName) {
		if (fileName == null) {
			return null;
		}
		fileName = fileName.toLowerCase();
		for (FileType jobType: values()) {
			if (fileName.endsWith("." + jobType.extension)) {
				return jobType;
			}
		}
		return null;
	}

	/**
	 * MVa: what is this for? Does it work?
	 * TODO 
	 * 
	 * Returns the description of the file type (GUI).
	 * 
	 * @return the description
	 */
	public String getName() {
		return UtilMessages.getString("FileType." + this.toString()); //$NON-NLS-1$
	}

}
