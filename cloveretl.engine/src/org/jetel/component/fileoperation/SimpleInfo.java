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

import java.io.Serializable;
import java.net.URI;
import java.util.Date;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6.3.2012
 */
public class SimpleInfo implements Info, Serializable {
	
	private static final long serialVersionUID = 1L;

	private final String name;
	
	private final URI uri;
	
	private URI parentDir = null;

	private Type type = null; 
	
	private Long size = 0L;
	
	private Date lastModified = null;
	
	private Date created = null;
	
	private Date lastAccessed = null;
	
	private Boolean canRead = null;
	
	private Boolean canWrite = null;
	
	private Boolean canExecute = null;
	
	private Boolean hidden = null;
	
	public SimpleInfo(String name, URI uri) {
		if (name == null) {
			throw new NullPointerException("name"); //$NON-NLS-1$
		}
		if (uri == null) {
			throw new NullPointerException("uri"); //$NON-NLS-1$
		}
		this.name = name;
		this.uri = uri;
	}
	
	public SimpleInfo(Info info) {
		this(info.getName(), info.getURI());
		this.canExecute = info.canExecute();
		this.canRead = info.canRead();
		this.canWrite = info.canWrite();
		this.created = info.getCreated();
		this.hidden = info.isHidden();
		this.lastAccessed = info.getLastAccessed();
		this.lastModified = info.getLastModified();
		this.parentDir = info.getParentDir();
		this.size = info.getSize();
		this.type = info.getType();
	}
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public URI getURI() {
		return uri;
	}
	
	@Override
	public URI getParentDir() {
		return parentDir;
	}
	
	@Override
	public boolean isDirectory() {
		return type == Type.DIR;
	}

	@Override
	public Long getSize() {
		return size;
	}

	@Override
	public Date getLastModified() {
		if (lastModified != null) {
			return new Date(lastModified.getTime());
		}
		return null;
	}

	/**
	 * @param directory the directory to set
	 */
	public SimpleInfo setType(Type type) {
		this.type = type;
		return this;
	}

	/**
	 * @param size the size to set
	 */
	public SimpleInfo setSize(long size) {
		this.size = size;
		return this;
	}

	/**
	 * @param lastModified the lastModified to set
	 */
	public SimpleInfo setLastModified(Date lastModified) {
		this.lastModified = lastModified;
		return this;
	}
	
	public SimpleInfo setParentDir(URI parentDir) {
		this.parentDir = parentDir;
		return this;
	}

	@Override
	public boolean isFile() {
		return type == Type.FILE;
	}
	
	@Override
	public Boolean isLink() {
		return type == Type.LINK;
	}

	@Override
	public Type getType() {
		return null;
	}

	@Override
	public Date getCreated() {
		return created;
	}

	@Override
	public Date getLastAccessed() {
		return lastAccessed;
	}

	@Override
	public String toString() {
		return uri.toString();
	}

	@Override
	public Boolean isHidden() {
		return hidden;
	}

	@Override
	public Boolean canRead() {
		return canRead;
	}

	@Override
	public Boolean canWrite() {
		return canWrite;
	}

	@Override
	public Boolean canExecute() {
		return canExecute;
	}

	public SimpleInfo setCanRead(boolean canRead) {
		this.canRead = canRead;
		return this;
	}

	public SimpleInfo setCanWrite(boolean canWrite) {
		this.canWrite = canWrite;
		return this;
	}

	public SimpleInfo setCanExecute(boolean canExecute) {
		this.canExecute = canExecute;
		return this;
	}

	public SimpleInfo setHidden(boolean hidden) {
		this.hidden = hidden;
		return this;
	}
	
	

}
