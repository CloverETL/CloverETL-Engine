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
package org.jetel.graph.runtime;

import java.io.File;

/**
 * <p>
 * TempSpace is a directory where temp files are stored.
 * </p>
 * <p>
 * Two TempSpaces are equal if they reside in the same directory
 * </p>
 * 
 * @author "Michal Oprendek" (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 22.3.2012
 */
public class TempSpace {

	public static final TempSpace ENGINE_DEFAULT;

	static {
		File defaultTempDir = new File(System.getProperty("java.io.tmpdir"));
		ENGINE_DEFAULT = new TempSpace(defaultTempDir, "${java.io.tmpdir}");
	}

	private final File directory;
	private final String path;
	private Long id;

	/**
	 * Creates new TempSpace in given directory
	 * 
	 * @param directory
	 *            directory where temp files shall be stored
	 */
	public TempSpace(File directory, String path) {
		if (directory == null) {
			throw new IllegalArgumentException("param. directory can not be null");
		}
		this.directory = directory;
		this.path = path;
	}

	/**
	 * Returns the directory where temp files are stored
	 * 
	 * @return the directory where temp files are stored
	 */
	public File getDirectory() {
		return directory;
	}
	
	/**
	 * @return the path
	 */
	public String getPath() {
		return path;
	}
	
	/**
	 * @return the id
	 */
	public Long getId() {
		return id;
	}
	
	/**
	 * @param id the id to set
	 */
	public void setId(Long id) {
		this.id = id;
	}

	@Override
	public int hashCode() {
		return directory.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof TempSpace)) {
			return false;
		}
		TempSpace other = (TempSpace) obj;
		return directory.equals(other.directory);
	}

	@Override
	public String toString() {
		return "TempSpace in directory: " + directory;
	}

}
