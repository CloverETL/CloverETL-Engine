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
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.jetel.component.fileoperation.SimpleParameters.ReadParameters;
import org.jetel.component.fileoperation.SimpleParameters.WriteParameters;
import org.jetel.util.protocols.amazon.S3InputStream;
import org.jetel.util.protocols.amazon.S3OutputStream;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jun 19, 2012
 */
public class HttpS3OperationHandler extends BaseOperationHandler {

	static final String HTTP_SCHEME = "http"; //$NON-NLS-1$
	static final String HTTPS_SCHEME = "https"; //$NON-NLS-1$
	
// 	FIXME
//	public static final int PRIORITY = WebdavOperationHandler.PRIORITY + 1;
	public static final int PRIORITY = 1;
	
	private FileManager manager = FileManager.getInstance();
	
	private static class S3Content implements Content {
		
		private final URL url;
		
		public S3Content(URL url) {
			this.url = url;
		}

		@Override
		public ReadableByteChannel read() throws IOException {
			return Channels.newChannel(new S3InputStream(url));
		}
		
		@Override
		public WritableByteChannel write() throws IOException {
			return Channels.newChannel(new S3OutputStream(url));
		}

		@Override
		public WritableByteChannel append() throws IOException {
			throw new UnsupportedOperationException();
		}

	}

	@Override
	public ReadableContent getInput(SingleCloverURI source, ReadParameters params) throws IOException {
		if (S3InputStream.isS3File(source.getPath())) {
			return new S3Content(source.toURI().toURL());
		} else {
			IOperationHandler nextHandler = manager.findNextHandler(Operation.read(source.getScheme()), this);
			if (nextHandler != null) {
				return nextHandler.getInput(source, params);
			} else {
				throw new UnsupportedOperationException();
			}
		}
	}

	@Override
	public WritableContent getOutput(SingleCloverURI target, WriteParameters params) throws IOException {
		if (S3InputStream.isS3File(target.getPath())) {
			return new S3Content(target.toURI().toURL());
		} else {
			IOperationHandler nextHandler = manager.findNextHandler(Operation.write(target.getScheme()), this);
			if (nextHandler != null) {
				return nextHandler.getOutput(target, params);
			} else {
				throw new UnsupportedOperationException();
			}
		}
	}

	@Override
	public int getPriority(Operation operation) {
		return PRIORITY;
	}

	@Override
	public boolean canPerform(Operation operation) {
		switch (operation.kind) {
			case READ:
			case WRITE:
				return operation.scheme().equalsIgnoreCase(HTTP_SCHEME) || operation.scheme().equalsIgnoreCase(HTTPS_SCHEME);
			default: 
				return false;
		}
	}

	@Override
	public String toString() {
		return "S3OperationHandler"; //$NON-NLS-1$
	}

}
