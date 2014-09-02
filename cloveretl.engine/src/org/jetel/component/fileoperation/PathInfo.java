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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;

/**
 * An implementation of {@link Info}
 * used primarily for listing local files.
 * <p>
 * File metadata are cached at the time the object is created,
 * may be outdated at the time they are requested.
 * </p>
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2. 9. 2014
 */
public class PathInfo implements Info {
	
	private final Path path;
	private final BasicFileAttributes attributes;
	
	public PathInfo(Path path) throws IOException {
		this(path, Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS));
	}

	/**
	 * Package-private constructor,
	 * uses precomputed {@link BasicFileAttributes}.
	 * 
	 * @param path			target {@link Path}
	 * @param attributes	precomputed file attributes
	 */
	PathInfo(Path path, BasicFileAttributes attributes) {
		this.path = path;
		this.attributes = attributes;
	}

	@Override
	public String getName() {
		if (path.getNameCount() == 0) {
			return "";
		}
		return path.getFileName().toString();
	}

	@Override
	public URI getURI() {
		return path.toUri();
	}

	@Override
	public URI getParentDir() {
		Path parent = path.getParent();
		return parent != null ? parent.toUri() : null;
	}

	@Override
	public boolean isDirectory() {
		return attributes.isDirectory();
	}

	@Override
	public boolean isFile() {
		return attributes.isRegularFile();
	}
	
	@Override
	public Boolean isLink() {
		return attributes.isSymbolicLink();
	}

	@Override
	public Boolean isHidden() {
		if (Files.exists(path)) {
			try {
				return Files.isHidden(path);
			} catch (IOException ioe) {}
		}
		return null;
	}

	@Override
	public Boolean canRead() {
		return Files.isReadable(path);
	}

	@Override
	public Boolean canWrite() {
		return Files.isWritable(path);
	}

	@Override
	public Boolean canExecute() {
		return Files.isExecutable(path);
	}

	@Override
	public Type getType() {
		if (attributes.isDirectory()) {
			return Type.DIR;
		} else if (attributes.isRegularFile()) {
			return Type.FILE;
		} else if (attributes.isSymbolicLink()) {
			return Type.LINK;
		}
		return Type.OTHER;
	}

	@Override
	public Date getLastModified() {
		return new Date(attributes.lastModifiedTime().toMillis());
	}

	@Override
	public Date getCreated() {
		return new Date(attributes.creationTime().toMillis());
	}

	@Override
	public Date getLastAccessed() {
		return new Date(attributes.lastAccessTime().toMillis());
	}

	@Override
	public Long getSize() {
		return attributes.size();
	}

	@Override
	public String toString() {
		return path.toString();
	}
	
}