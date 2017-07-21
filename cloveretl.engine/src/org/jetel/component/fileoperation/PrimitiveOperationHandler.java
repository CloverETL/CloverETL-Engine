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
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Date;
import java.util.List;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27.3.2012
 */
public interface PrimitiveOperationHandler {
	
	public boolean createFile(URI target) throws IOException;
	
	public boolean setLastModified(URI target, Date date) throws IOException;
	
	public boolean makeDir(URI target) throws IOException;
	
	public boolean deleteFile(URI target) throws IOException;
	
	public boolean removeDir(URI target) throws IOException;
	
	public URI moveFile(URI source, URI target) throws IOException;
	
	public URI copyFile(URI source, URI target) throws IOException;
	
	public URI renameTo(URI source, URI target) throws IOException;
	
	public ReadableByteChannel read(URI source) throws IOException;

	public WritableByteChannel write(URI target) throws IOException;

	public WritableByteChannel append(URI target) throws IOException;
	
	public Info info(URI target) throws IOException;
	
	public List<URI> list(URI target) throws IOException;
}
