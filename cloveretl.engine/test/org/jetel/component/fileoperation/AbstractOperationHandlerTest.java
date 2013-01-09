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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.jetel.component.fileoperation.SimpleParameters.CopyParameters;
import org.jetel.component.fileoperation.SimpleParameters.MoveParameters;
import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;
import org.jetel.util.file.FileUtils;

public class AbstractOperationHandlerTest extends LocalOperationHandlerTest {
	
	private TestAbstractOperationHandler handler = null;
	
	private static class TestAbstractOperationHandler extends AbstractOperationHandler {
		
		private final FileManager manager = FileManager.getInstance();

		public TestAbstractOperationHandler() {
			super(new PrimitiveFileOperationHandler());
		}
		
		PrimitiveFileOperationHandler getPrimitiveHandler() {
			return (PrimitiveFileOperationHandler) simpleHandler;
		}

		@Override
		public int getPriority(Operation operation) {
			return 0;
		}

		@Override
		public boolean canPerform(Operation operation) {
			if (operation.scheme().equals(LocalOperationHandler.FILE_SCHEME)) {
				return true;
			}
			return false;
		}

		@Override
		public List<SingleCloverURI> resolve(SingleCloverURI uri, ResolveParameters params) throws IOException {
			return manager.defaultResolve(uri);
		}

		@Override
		public SingleCloverURI copy(SingleCloverURI sourceUri, SingleCloverURI targetUri, CopyParameters params)
				throws IOException {
			File sourceFile = new File(sourceUri.toURI());
			File targetFile = new File(targetUri.toURI());
			try {
				sourceFile = sourceFile.getCanonicalFile();
			} catch (IOException ex) {}
			try {
				targetFile = targetFile.getCanonicalFile();
			} catch (IOException ex) {}
			if (sourceFile.equals(targetFile)) {
				throw new IOException(String.format("%s and %s are the same file", sourceFile, targetFile));
			}
			return super.copy(sourceUri, targetUri, params);
		}

		@Override
		public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params)
				throws IOException {
			File sourceFile = new File(source.toURI());
			File targetFile = new File(target.toURI());
			try {
				sourceFile = sourceFile.getCanonicalFile();
			} catch (IOException ex) {}
			try {
				targetFile = targetFile.getCanonicalFile();
			} catch (IOException ex) {}
			if (sourceFile.equals(targetFile)) {
				throw new IOException(String.format("%s and %s are the same file", sourceFile, targetFile));
			}
			return super.move(source, target, params);
		}
		
		
		
	}
	
	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new TestAbstractOperationHandler();
	}
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		handler = null;
	}

	@Override
	public void testGetPriority() {
	}

	@Override
	public void testCanPerform() {
//		assertTrue(handler.canPerform(Operation.copy(LocalOperationHandler.FILE_SCHEME, LocalOperationHandler.FILE_SCHEME)));
//		assertTrue(handler.canPerform(Operation.move(LocalOperationHandler.FILE_SCHEME, LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.delete(LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.create(LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.resolve(LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.info(LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.list(LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.read(LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.write(LocalOperationHandler.FILE_SCHEME)));
	}
	
	public void testMoveWithoutRename() throws Exception {
		handler.getPrimitiveHandler().setUseRename(false);
		super.testMove(); // once again without renaming
		handler.getPrimitiveHandler().setUseRename(true);
	}

	private static class PrimitiveFileOperationHandler implements PrimitiveOperationHandler {
		
		private boolean useRename = true;
		
		public void setUseRename(boolean rename) {
			this.useRename = rename;
		}

		@Override
		public boolean createFile(URI target) throws IOException {
			return new File(target).createNewFile();
		}

		@Override
		public boolean setLastModified(URI target, Date date) throws IOException {
			return new File(target).setLastModified(date.getTime());
		}

		@Override
		public boolean makeDir(URI target) throws IOException {
			return new File(target).mkdir();
		}

		@Override
		public boolean deleteFile(URI target) throws IOException {
			File file = new File(target);
			return file.isFile() && file.delete();
		}

		@Override
		public boolean removeDir(URI target) throws IOException {
			File file = new File(target);
			return file.isDirectory() && file.delete();
		}

		@Override
		public URI moveFile(URI source, URI target) throws IOException {
			throw new UnsupportedOperationException();
		}
		
		private boolean copyFile(File source, File target) throws IOException {
			return FileUtils.copyFile(source, target);
		}

		@Override
		public URI copyFile(URI source, URI target) throws IOException {
			File sourceFile = new File(source);
			File targetFile = new File(target);
			return copyFile(sourceFile, targetFile) ? target : null;
		}

		@Override
		public URI renameTo(URI source, URI target) throws IOException {
			if (useRename) {
				File sourceFile = new File(source);
				File targetFile = new File(target);
				return sourceFile.renameTo(targetFile) ? targetFile.toURI() : null;
			} else {
				return null;
			}
		}

		@Override
		public ReadableByteChannel read(URI source) throws IOException {
			return new FileInputStream(new File(source)).getChannel();
		}

		@Override
		public WritableByteChannel write(URI target) throws IOException {
			return new FileOutputStream(new File(target)).getChannel();
		}

		@Override
		public WritableByteChannel append(URI target) throws IOException {
			return new FileOutputStream(new File(target), true).getChannel();
		}

		@Override
		public Info info(URI target) throws IOException {
			File file = new File(target);
			return file.exists() ? new FileInfo(file) : null;
		}

		@Override
		public List<URI> list(URI target) throws IOException {
			List<URI> result = new ArrayList<URI>();
			File parent = new File(target);
			for (File child: parent.listFiles()) {
				result.add(child.toURI());
			}
			return result;
		}

		@Override
		public String toString() {
			return "PrimitiveFileOperationHandler";
		}

	}
}
