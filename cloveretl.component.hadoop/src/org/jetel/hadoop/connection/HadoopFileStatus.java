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

package org.jetel.hadoop.connection;

import java.net.URI;



/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @see org.apache.hadoop.fs.FileStatus
 */
public class HadoopFileStatus {

	private URI file;
	private long size;
	private boolean isDir;
	private long modified;
	
	
	public HadoopFileStatus(URI file, long size, boolean isDir,long modified) {
		this.file = file;
		this.size = size;
		this.isDir = isDir;
		this.modified=modified;
	}

	public URI getFile() {
		return file;
	}


	public long getSize() {
		return size;
	}

	public boolean isDir() {
		return isDir;
	}
	
	public long getModificationTime(){
		return modified;
	}

	
}
