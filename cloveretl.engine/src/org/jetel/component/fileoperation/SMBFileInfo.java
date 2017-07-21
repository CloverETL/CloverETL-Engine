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

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;

import org.jetel.exception.JetelRuntimeException;

// TODO what to do with those SmbExceptions

public class SMBFileInfo implements Info {
	
	private final SmbFile file;
	
	public SMBFileInfo(SmbFile file) {
		SmbFile tmp = file;
		try {
			tmp = new SmbFile(file.getCanonicalPath());
		} catch (MalformedURLException ex) {
			throw new JetelRuntimeException(ex); // cannot happen if jcifs is correct
		}
		this.file = tmp;
	}

	@Override
	public String getName() {
		return getName(file);
	}
	
	/**
	 * Similar to SmbFile.getName(), but removes trailing slash (in case of directories) and decodes URL "%xy" escape sequences.
	 */
	public static String getName(SmbFile file) {
		String name = file.getName();
		if (name.endsWith("/")) {
			name = name.substring(0, name.length() - 1);
		}
		return URIUtils.urlDecode(name);
	}

	@Override
	public URI getURI() {
		return SMBOperationHandler.toURI(file);
	}

	@Override
	public boolean isDirectory() {
		try {
			return file.isDirectory();
		} catch (SmbException e) {
			throw new JetelRuntimeException(e);
		}
	}

	@Override
	public Date getLastModified() {
		try {
		return new Date(file.lastModified());
		} catch (SmbException e) {
			throw new JetelRuntimeException(e);
		}
	}

	@Override
	public Long getSize() {
		try {
			return file.length();
		} catch (SmbException e) {
			throw new JetelRuntimeException(e);
		}
	}

	@Override
	public URI getParentDir() {
		try {
			return new URI(file.getParent()); // TODO should null be returned sometimes?
		} catch (URISyntaxException e) {
			throw new JetelRuntimeException(e);
		}
	}

	@Override
	public boolean isFile() {
		try {
			return file.isFile();
		} catch (SmbException e) {
			throw new JetelRuntimeException(e);
		}
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
		try {
			return new Date(file.createTime());
		} catch (SmbException e) {
			throw new JetelRuntimeException(e);
		}
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
		try {
			return file.isHidden();
		} catch (SmbException e) {
			throw new JetelRuntimeException(e);
		}
	}

	@Override
	public Boolean canRead() {
		try {
			return file.canRead();
		} catch (SmbException e) {
			throw new JetelRuntimeException(e);
		}
	}

	@Override
	public Boolean canWrite() {
		try {
			return file.canWrite();
		} catch (SmbException e) {
			throw new JetelRuntimeException(e);
		}
	}

	@Override
	public Boolean canExecute() {
		return null;
	}
	
}