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
package org.jetel.component.fileoperation;

import java.io.File;
import java.net.URI;
import java.util.Date;

public class FileInfo implements Info {
	
	private final File file;
	
	public FileInfo(File file) {
		File tmp = file;
		try {
			tmp = file.getCanonicalFile();
		} catch (Exception ex) {}
		this.file = tmp;
	}

	@Override
	public String getName() {
		return file.getName();
	}

	@Override
	public URI getURI() {
		return file.toURI();
	}

	@Override
	public boolean isDirectory() {
		return file.isDirectory();
	}

	@Override
	public Date getLastModified() {
		return new Date(file.lastModified());
	}

	@Override
	public Long getSize() {
		return file.length();
	}

	@Override
	public URI getParentDir() {
		File parentDir = file.getParentFile();
		return parentDir != null ? parentDir.toURI() : null;
	}

	@Override
	public boolean isFile() {
		return file.isFile();
	}
	
	@Override
	public Boolean isLink() {
		return null;
	}

	@Override
	public Type getType() {
		if (isFile()) {
			return Type.FILE;
		} else if (isDirectory()) {
			return Type.DIR;
		} else {
			return Type.OTHER;
		}
	}

	@Override
	public Date getCreated() {
		return null;
	}

	@Override
	public Date getLastAccessed() {
		return null;
	}
	
	@Override
	public String toString() {
		return getURI().toString();
	}

	@Override
	public Boolean isHidden() {
		return file.isHidden();
	}

	@Override
	public Boolean canRead() {
		return file.canRead();
	}

	@Override
	public Boolean canWrite() {
		return file.canWrite();
	}

	@Override
	public Boolean canExecute() {
		return file.canExecute();
	}

}