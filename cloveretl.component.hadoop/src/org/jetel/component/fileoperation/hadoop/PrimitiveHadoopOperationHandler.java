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
package org.jetel.component.fileoperation.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.jetel.component.fileoperation.Info;
import org.jetel.component.fileoperation.PrimitiveOperationHandler;
import org.jetel.component.fileoperation.URIUtils;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.TransformationGraph;
import org.jetel.hadoop.connection.HadoopConnection;
import org.jetel.hadoop.connection.HadoopFileStatus;
import org.jetel.hadoop.connection.IHadoopConnection;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Nov 26, 2012
 */
public class PrimitiveHadoopOperationHandler implements PrimitiveOperationHandler {
	
	private IHadoopConnection getConnection(URI uri) throws IOException {
		TransformationGraph graph = ContextProvider.getGraph();
		String connectionId = uri.getAuthority();
		IConnection connection = graph.getConnection(connectionId);
		if (!(connection instanceof HadoopConnection)) {
			throw new IOException(MessageFormat.format("Not a Hadoop connection: {0}", connectionId));
		}
		if (!connection.isInitialized()) {
			try {
				connection.init();
			} catch (ComponentNotReadyException e) {
				throw new IOException(e);
			}
		}
		IHadoopConnection result = ((HadoopConnection) connection).getConnection();
		return result;
	}
	
	private URI getPath(URI uri) {
		// Cloudera 3u4 does not deal with "." and ".." in paths => normalize()
		StringBuilder sb = new StringBuilder();
		String path = uri.getRawPath();
		if (path != null) {
			sb.append(path);
		}
		String query = uri.getRawQuery();
		if (query != null) {
			sb.append('?').append(query);
		}
		return URI.create(sb.toString()).normalize();
	}

	public boolean createFile(URI target, boolean makeParents) throws IOException {
		URI path = getPath(target);
		IHadoopConnection connection = getConnection(target);
		if (!makeParents) {
			URI parent = URIUtils.getParentURI(path);
			if ((parent != null) && !connection.exists(parent)) {
				throw new IOException(parent.toString());
			}
		}
		return connection.createNewFile(path);
	}

	@Override
	public boolean createFile(URI target) throws IOException {
		return createFile(target, false);
	}

	@Override
	public boolean setLastModified(URI target, Date date) throws IOException {
		IHadoopConnection connection = getConnection(target);
		URI path = getPath(target);
		if (!connection.getStatus(path).isDir()) { // not supported for directories
			connection.setLastModified(path, date.getTime());
		}
		return true;
	}

	public boolean makeDir(URI target, boolean makeParents) throws IOException {
		URI path = getPath(target);
		IHadoopConnection connection = getConnection(target);
		if (!makeParents) {
			URI parent = URIUtils.getParentURI(path);
			if ((parent != null) && !connection.exists(parent)) {
				throw new IOException(parent.toString());
			}
		}
		return connection.mkdir(path);
	}

	@Override
	public boolean makeDir(URI target) throws IOException {
		return makeDir(target, false);
	}

	@Override
	public boolean deleteFile(URI target) throws IOException {
		return getConnection(target).delete(getPath(target), false);
	}

	@Override
	public boolean removeDir(URI target) throws IOException {
		return getConnection(target).delete(getPath(target), false);
	}

	public boolean removeDirRecursively(URI target) throws IOException {
		return getConnection(target).delete(getPath(target), true);
	}

	@Override
	public URI moveFile(URI source, URI target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public URI copyFile(URI source, URI target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public URI renameTo(URI source, URI target) throws IOException {
		return getConnection(source).rename(getPath(source), getPath(target)) ? target : null;
	}

	@Override
	public ReadableByteChannel read(URI source) throws IOException {
		return Channels.newChannel(getConnection(source).open(getPath(source)).getDataInputStream());
	}

	@Override
	public WritableByteChannel write(URI target) throws IOException {
		return Channels.newChannel(getConnection(target).create(getPath(target), true).getDataOutputStream());
	}

	@Override
	public WritableByteChannel append(URI target) throws IOException {
		return Channels.newChannel(getConnection(target).append(getPath(target)).getDataOutputStream());
	}

	@Override
	public Info info(URI target) throws IOException {
		IHadoopConnection connection = getConnection(target);
		URI path = getPath(target);
		if (connection.exists(path)) {
			return new HadoopInfo(connection.getStatus(path), target.getAuthority()); 
		}
		
		return null;
	}

	@Override
	public List<URI> list(URI target) throws IOException {
		HadoopFileStatus[] files = getConnection(target).listStatus(getPath(target));
		List<URI> result = new ArrayList<URI>(files.length);
		for (HadoopFileStatus file: files) {
			result.add(new HadoopInfo(file, target.getAuthority()).getURI());
		}
		return result;
	}
	
	// FIXME not used
	public List<URI> resolve(URI target) throws IOException {
		URI normalized = target.normalize();
		StringBuilder glob = new StringBuilder();
		String path = normalized.getPath(); // decode escape sequences
		if (path != null) {
			glob.append(path);
		}
		String query = normalized.getQuery(); // decode escape sequences
		if (query != null) {
			glob.append('?').append(query);
		}
		HadoopFileStatus[] files = getConnection(target).globStatus(glob.toString());
		if (files == null) {
			return Arrays.asList(target); // no wildcard
		}
		List<URI> result = new ArrayList<URI>(files.length);
		boolean directory = target.toString().endsWith(URIUtils.PATH_SEPARATOR);
		String connectionId = target.getHost();
		if (directory) {
			for (HadoopFileStatus file: files) {
				if (file.isDir()) {
					result.add(new HadoopInfo(file, connectionId).getURI());
				}
			}
		} else {
			for (HadoopFileStatus file: files) {
				result.add(new HadoopInfo(file, connectionId).getURI());
			}
		}
		return result;
	}
	
	private static class HadoopInfo implements Info {
		
		private final HadoopFileStatus file;
		private final URI uri;

		public HadoopInfo(HadoopFileStatus file, String connectionId) throws IOException {
			this.file = file;
			
			URI tmp = file.getFile();
			try {
				this.uri = new URI(tmp.getScheme(), connectionId, tmp.getPath(), null);
			} catch (URISyntaxException e) {
				throw new IOException(e);
			}
		}

		@Override
		public String getName() {
			return URIUtils.urlDecode(URIUtils.getFileName(uri));
		}

		@Override
		public URI getURI() {
			return uri;
		}

		@Override
		public URI getParentDir() {
			return URIUtils.getParentURI(uri);
		}

		@Override
		public boolean isDirectory() {
			return file.isDir();
		}

		@Override
		public boolean isFile() {
			return !file.isDir();
		}

		@Override
		public Boolean isLink() {
			return null;
		}

		@Override
		public Boolean isHidden() {
			return null;
		}

		@Override
		public Boolean canRead() {
			return null;
		}

		@Override
		public Boolean canWrite() {
			return null;
		}

		@Override
		public Boolean canExecute() {
			return null;
		}

		@Override
		public Type getType() {
			return file.isDir() ? Type.DIR : Type.FILE;
		}

		@Override
		public Date getLastModified() {
			return new Date(file.getModificationTime());
		}

		@Override
		public Date getCreated() {
			return null;
		}

		@Override
		public Date getLastAccessed() {
			return null; // TODO
		}

		@Override
		public Long getSize() {
			return file.getSize();
		}

		@Override
		public String toString() {
			return getURI().toString();
		}
		
	}

}
