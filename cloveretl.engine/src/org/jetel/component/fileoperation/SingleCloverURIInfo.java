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

import java.net.URI;
import java.util.Date;

/**
 * CLO-6675: SingleCloverURI extension that also implements {@link Info}
 * as an optimization for listing directories with wildcards.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26. 6. 2015
 */
public class SingleCloverURIInfo extends SingleCloverURI implements Info {
	
	private final Info info;

	public SingleCloverURIInfo(Info info) {
		super(info.getURI());
		this.info = info;
	}

	@Override
	public String getName() {
		return info.getName();
	}

	@Override
	public URI getURI() {
		return info.getURI();
	}

	@Override
	public URI getParentDir() {
		return info.getParentDir();
	}

	@Override
	public boolean isDirectory() {
		return info.isDirectory();
	}

	@Override
	public boolean isFile() {
		return info.isFile();
	}

	@Override
	public Boolean isLink() {
		return info.isLink();
	}

	@Override
	public Boolean isHidden() {
		return info.isHidden();
	}

	@Override
	public Boolean canRead() {
		return info.canRead();
	}

	@Override
	public Boolean canWrite() {
		return info.canWrite();
	}

	@Override
	public Boolean canExecute() {
		return info.canExecute();
	}

	@Override
	public Type getType() {
		return info.getType();
	}

	@Override
	public Date getLastModified() {
		return info.getLastModified();
	}

	@Override
	public Date getCreated() {
		return info.getCreated();
	}

	@Override
	public Date getLastAccessed() {
		return info.getLastAccessed();
	}

	@Override
	public Long getSize() {
		return info.getSize();
	}

}
