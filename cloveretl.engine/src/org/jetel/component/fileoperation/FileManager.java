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

import static java.text.MessageFormat.format;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jetel.component.fileoperation.SimpleParameters.CopyParameters;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.SimpleParameters.FileParameters;
import org.jetel.component.fileoperation.SimpleParameters.InfoParameters;
import org.jetel.component.fileoperation.SimpleParameters.ListParameters;
import org.jetel.component.fileoperation.SimpleParameters.MoveParameters;
import org.jetel.component.fileoperation.SimpleParameters.ReadParameters;
import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;
import org.jetel.component.fileoperation.SimpleParameters.WriteParameters;
import org.jetel.component.fileoperation.result.CopyResult;
import org.jetel.component.fileoperation.result.CreateResult;
import org.jetel.component.fileoperation.result.DeleteResult;
import org.jetel.component.fileoperation.result.InfoResult;
import org.jetel.component.fileoperation.result.ListResult;
import org.jetel.component.fileoperation.result.MoveResult;
import org.jetel.component.fileoperation.result.ResolveResult;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.PrimitiveAuthorityProxy;

/**
 * The FileManager and related classes
 * are considered internal and may change in the future.
 */
public class FileManager {
	
	/**
	 * @see <a href="http://en.wikipedia.org/wiki/Initialization_on_demand_holder_idiom#Example_Java_Implementation">http://en.wikipedia.org/wiki/Initialization_on_demand_holder_idiom#Example_Java_Implementation</a>
	 * 
	 * @created Oct 3, 2012
	 */
	private static class SingletonHolder {
        private static final FileManager INSTANCE = new FileManager();
	}
	
	/**
	 * @return the instance
	 */
	public static FileManager getInstance() { // no synchronization needed
		return SingletonHolder.INSTANCE;
	}
	
	private final Collection<IOperationHandler> handlers = new ArrayList<IOperationHandler>();
	
	@SuppressWarnings("serial")
	private class MRUCache<K, V> extends LinkedHashMap<K, V> {
		
		private final int maxSize;
		
		public MRUCache(int maxSize) {
			super(16, 0.75f, true);
			this.maxSize = maxSize;
		}

		/* (non-Javadoc)
		 * @see java.util.LinkedHashMap#removeEldestEntry(java.util.Map.Entry)
		 */
		@Override
		protected boolean removeEldestEntry(Entry<K, V> eldest) {
			boolean doIt = size() > maxSize;
//			if (doIt) {
//				System.err.println("Cache overflow");
//			}
			return doIt;
		}
		
	}
	
	private static final int MAX_CACHE_SIZE = 50;
	
	// the map must be synchronized, even map.get() causes a structural modification
	private final Map<Operation, List<IOperationHandler>> cachedHandlers = Collections.synchronizedMap(new MRUCache<Operation, List<IOperationHandler>>(MAX_CACHE_SIZE));
	
	private URI currentWorkingDir = null;
	
	private FileManager() {
	}
	
	private static class HandlerComparator implements Comparator<IOperationHandler> {
		
		private final Operation operation;
		
		public HandlerComparator(Operation operation) {
			this.operation = operation;
		}

		@Override
		public int compare(IOperationHandler o1, IOperationHandler o2) {
			int s1 = o1.getPriority(operation);
			int s2 = o2.getPriority(operation);
			return (s1>s2 ? -1 : (s1==s2 ? 0 : 1));
		}
		
	}
	
	public boolean canPerform(Operation operation) {
		return findHandler(operation) != null;
	}
	
	/*
	 * This method is synchronized to prevent 
	 * unpredictable behavior of file operations
	 * after the list of handlers has been modified 
	 * from a different thread than the one performing the operation. 
	 */
	public synchronized List<IOperationHandler> listHandlers(Operation operation) {
		ArrayList<IOperationHandler> candidates = new ArrayList<IOperationHandler>();
		for (IOperationHandler handler: handlers) {
			if (handler.canPerform(operation)) {
				candidates.add(handler);
			}
		}
		candidates.trimToSize(); // the list will never grow, minimize the consumed memory

		if (!candidates.isEmpty()) {
			Collections.sort(candidates, new HandlerComparator(operation));
		}
		
		return candidates;
	}
	
	public IOperationHandler findNextHandler(Operation operation, IOperationHandler previousHandler) {
		
		/*
		 * no synchronization needed here
		 * - if two threads compute the list of handlers concurrently
		 * and store it, the result will still be correct 
		 */
		List<IOperationHandler> handlers = cachedHandlers.get(operation);
		if (handlers == null) {
			handlers = listHandlers(operation);
			cachedHandlers.put(operation, handlers);
		}

		if (!handlers.isEmpty()) {
			if (previousHandler == null) {
				return handlers.get(0);
			} else {
				HandlerComparator comparator = new HandlerComparator(operation);
				int prevHandlerIndex = Collections.binarySearch(handlers, previousHandler, comparator);
				if ((prevHandlerIndex >= 0) && (prevHandlerIndex < handlers.size() - 1)) {
					return handlers.get(prevHandlerIndex + 1);
				}
			}
		}
		
		return null;
	}
	
	public IOperationHandler findHandler(Operation operation) {
		return findNextHandler(operation, null);
	}
	
	public static Operation getOperation(OperationKind kind, SingleCloverURI... CloverURIs) {
		String[] schemes = new String[CloverURIs.length];
		for (int i = 0; i < CloverURIs.length; i++) {
			schemes[i] = CloverURIs[i].getScheme();
		}
		Operation operation = new Operation(kind, schemes);
		return operation;
	}

	public synchronized void registerHandler(IOperationHandler handler) {
		cachedHandlers.clear();
		handlers.add(handler);
	}
	
	/**
	 * Registers the default handlers.
	 * 
	 * Should be called just once!
	 */
	public static void init() {
		FileManager manager = FileManager.getInstance();
		synchronized (manager) {
			manager.registerHandler(new LocalOperationHandler());
			manager.registerHandler(new FTPOperationHandler());
			manager.registerHandler(new URLOperationHandler());
			manager.registerHandler(new DefaultOperationHandler());
			manager.registerHandler(new WebdavOperationHandler());
			manager.registerHandler(new S3OperationHandler());
			manager.registerHandler(new SFTPOperationHandler());
		}
	}
	
	synchronized void clear() {
		cachedHandlers.clear();
		handlers.clear();
	}
	
	public URI getContextURI() {
		TransformationGraph graph = ContextProvider.getGraph();
		if (graph != null) {
			GraphRuntimeContext runtimeContext = graph.getRuntimeContext();
			if (runtimeContext != null) {
				URL url = runtimeContext.getContextURL();
				if (url != null) {
					try {
						return url.toURI();
					} catch (URISyntaxException ex) {
						throw new IllegalStateException(FileOperationMessages.getString("FileManager.context_URI_not_available"), ex); //$NON-NLS-1$
					}
				} else {
					if (graph.getAuthorityProxy() instanceof PrimitiveAuthorityProxy) {
						// locally running graph
						if (currentWorkingDir == null) {
							// the current working dir cannot change at runtime
							currentWorkingDir = new File(URIUtils.CURRENT_DIR_NAME).toURI();
						}
						return currentWorkingDir;
					}
				}
			}
		}
		throw new IllegalStateException(FileOperationMessages.getString("FileManager.context_URI_not_available")); //$NON-NLS-1$
	}
	
	public CopyResult copy(String source, String target, CopyParameters params) {
		if (source == null) {
			return new CopyResult().setException(new NullPointerException(FileOperationMessages.getString("FileManager.copy_source_is_null"))); //$NON-NLS-1$
		}
		if (target == null) {
			return new CopyResult().setException(new NullPointerException(FileOperationMessages.getString("FileManager.copy_target_is_null"))); //$NON-NLS-1$
		}
		CloverURI sourceCloverUri = CloverURI.createURI(source);
		CloverURI targetCloverUri = CloverURI.createURI(target);
		return copy(sourceCloverUri, targetCloverUri, params);
	}
	
	public CopyResult copy(CloverURI sourceList, CloverURI targetExpression, CopyParameters params) {
		CopyResult result = new CopyResult();
		if (!targetExpression.isSingle()) {
			return result.setException(new IllegalArgumentException(format(FileOperationMessages.getString("FileManager.single_target_URI_permitted"), targetExpression))); //$NON-NLS-1$
		}
		sourceList = sourceList.getAbsoluteURI();
		targetExpression = targetExpression.getAbsoluteURI();
		List<SingleCloverURI> resolvedTarget = resolve(targetExpression).getResult();
		if (resolvedTarget.size() > 1) {
			return result.setException(new IllegalArgumentException(format(FileOperationMessages.getString("FileManager.single_target_URI_permitted"), resolvedTarget))); //$NON-NLS-1$
		} else if (resolvedTarget.size() == 1) {
			targetExpression = resolvedTarget.get(0);
		}
		SingleCloverURI target = targetExpression.getSingleURI();
		List<SingleCloverURI> sources = sourceList.split();
		List<IOperationHandler> handlers = new ArrayList<IOperationHandler>(sources.size());
		for (SingleCloverURI sourceExpression: sources) {
			Operation operation = getOperation(OperationKind.COPY, sourceExpression, target);
			IOperationHandler handler = findHandler(operation);
			if (handler == null) {
				return result.setException(new UnsupportedOperationException(operation.toString()));
			}
			handlers.add(handler);
		}
		int count = 0;
		List<List<SingleCloverURI>> resolvedSources = new ArrayList<List<SingleCloverURI>>();
		for (SingleCloverURI sourceExpression: sources) {
			ResolveResult rr = resolve(sourceExpression);
			if (!rr.success()) {
				return result.setException(rr.getException());
			}
			List<SingleCloverURI> tmp = rr.getResult();
			count += tmp.size();
			resolvedSources.add(tmp);
		}
		if (count > 1) {
			if (resolvedTarget.size() < 1 || !info(resolvedTarget.get(0)).isDirectory()) {
				if (info(resolvedTarget.get(0)).exists() || !Boolean.TRUE.equals(params.isMakeParents()) || !resolvedTarget.get(0).getPath().endsWith(URIUtils.PATH_SEPARATOR)) {
					return result.setException(new IOException(format(FileOperationMessages.getString("FileManager.not_a_directory"), targetExpression))); //$NON-NLS-1$
				}
			}
		}
		
//		if (Boolean.TRUE.equals(params.isMakeParents())) {
//			URI parent = URIUtils.trimToLastSlash(target.toURI());
//			if (parent != null) {
//				Operation createOperation = Operation.create(parent.getScheme());
//				IOperationHandler createHandler = findHandler(createOperation);
//				if (createHandler != null) {
//					try {
//						createHandler.create(CloverURI.createSingleURI(parent), new CreateParameters().setDirectory(true).setMakeParents(true)); 
//					} catch (Exception ex) {
//						// ignore, try to continue and see if it works
//						// FIXME the operation could succeed, but create the file in a wrong directory
//					}
//				}
//			}
//		}
		
		Iterator<IOperationHandler> h = handlers.iterator();
		
		for (List<SingleCloverURI> resolvedSource: resolvedSources) {
			IOperationHandler handler = h.next();
			for (SingleCloverURI source: resolvedSource) {
				try {
					SingleCloverURI copied = handler.copy(source, target, params); 
					if (copied != null) {
						result.add(source, target, copied);
					} else {
						result.addError(source, target, FileOperationMessages.getString("FileManager.copy_failed")); //$NON-NLS-1$
					}
				} catch (FileNotFoundException fnfe) {
					result.addError(source, target, format(FileOperationMessages.getString("FileManager.file_not_found"), fnfe.getMessage())); //$NON-NLS-1$
				} catch (Exception ex) {
					result.addError(source, target, ex.getMessage());
				}
			}
		}
		
		return result;
	}
	
	public MoveResult move(String source, String target, MoveParameters params) {
		if (source == null) {
			return new MoveResult().setException(new NullPointerException(FileOperationMessages.getString("FileManager.move_source_is_null"))); //$NON-NLS-1$
		}
		if (target == null) {
			return new MoveResult().setException(new NullPointerException(FileOperationMessages.getString("FileManager.move_target_is_null"))); //$NON-NLS-1$
		}
		CloverURI sourceCloverUri = CloverURI.createURI(source);
		CloverURI targetCloverUri = CloverURI.createURI(target);
		return move(sourceCloverUri, targetCloverUri, params);
	}
	
	public MoveResult move(CloverURI sourceList, CloverURI targetExpression, MoveParameters params) {
		MoveResult result = new MoveResult();
		if (!targetExpression.isSingle()) {
			return result.setException(new IllegalArgumentException(format(FileOperationMessages.getString("FileManager.single_target_URI_permitted"), targetExpression))); //$NON-NLS-1$
		}
		sourceList = sourceList.getAbsoluteURI();
		SingleCloverURI target = targetExpression.getSingleURI().getAbsoluteURI();
		List<SingleCloverURI> resolvedTarget = resolve(targetExpression).getResult();
		if (resolvedTarget.size() > 1) {
			return result.setException(new IllegalArgumentException(format(FileOperationMessages.getString("FileManager.single_target_URI_permitted"), resolvedTarget))); //$NON-NLS-1$
		} else if (resolvedTarget.size() == 1) {
			target = resolvedTarget.get(0);
		}
		List<SingleCloverURI> sources = sourceList.split();
		List<IOperationHandler> handlers = new ArrayList<IOperationHandler>(sources.size());
		
		for (SingleCloverURI sourceExpression: sources) {
			Operation operation = getOperation(OperationKind.MOVE, sourceExpression, target);
			IOperationHandler handler = findHandler(operation);
			if (handler == null) {
				return result.setException(new UnsupportedOperationException(operation.toString()));
			}
			handlers.add(handler);
		}

		int count = 0;
		List<List<SingleCloverURI>> resolvedSources = new ArrayList<List<SingleCloverURI>>();
		for (SingleCloverURI sourceExpression: sources) {
			ResolveResult rr = resolve(sourceExpression);
			if (!rr.success()) {
				return result.setException(rr.getException());
			}
			List<SingleCloverURI> tmp = rr.getResult();
			count += tmp.size();
			resolvedSources.add(tmp);
		}
		if (count > 1) {
			if (resolvedTarget.size() < 1 || !info(resolvedTarget.get(0)).isDirectory()) {
				if (info(resolvedTarget.get(0)).exists() || !Boolean.TRUE.equals(params.isMakeParents()) || !resolvedTarget.get(0).getPath().endsWith(URIUtils.PATH_SEPARATOR)) {
					return result.setException(new IOException(format(FileOperationMessages.getString("FileManager.not_a_directory"), targetExpression))); //$NON-NLS-1$
				}
			}
		}
		
//		if (Boolean.TRUE.equals(params.isMakeParents())) {
//			URI parent = URIUtils.trimToLastSlash(target.toURI());
//			if (parent != null) {
//				Operation createOperation = Operation.create(parent.getScheme());
//				IOperationHandler createHandler = findHandler(createOperation);
//				if (createHandler != null) {
//					try {
//						createHandler.create(CloverURI.createSingleURI(parent), new CreateParameters().setDirectory(true).setMakeParents(true)); 
//					} catch (Exception ex) {
//						// ignore, try to continue and see if it works
//						// FIXME the operation could succeed, but create the file in a wrong directory
//					}
//				}
//			}
//		}
		
		Iterator<IOperationHandler> h = handlers.iterator();
		for (List<SingleCloverURI> resolvedSource: resolvedSources) {
			IOperationHandler handler = h.next();
			for (SingleCloverURI source: resolvedSource) {
				try {
					SingleCloverURI moved = handler.move(source, target, params); 
					if (moved != null) {
						result.add(source, target, moved);
					} else {
						result.addError(source, target, FileOperationMessages.getString("FileManager.move_failed")); //$NON-NLS-1$
					}
				} catch (FileNotFoundException fnfe) {
					result.addError(source, target, format(FileOperationMessages.getString("FileManager.file_not_found"), fnfe.getMessage())); //$NON-NLS-1$
				} catch (Exception ex) {
					result.addError(source, target, ex.getMessage());
				}
			}
		}

		return result;
	}
	
	public static class AbstractContentProvider<T> {
		
		protected final List<T> contents = new ArrayList<T>();
		protected final List<URI> uris = new ArrayList<URI>();
		protected final Map<URI, T> map = new HashMap<URI, T>();

		public URI getURI() {
			return uris.get(0);
		}

		public URI getURI(int i) {
			return uris.get(i);
		}

		public void add(URI uri, T content) {
			this.uris.add(uri);
			this.contents.add(content);
			this.map.put(uri, content);
		}
		
		public int size() {
			return contents.size();
		}
		
	}
	
	public static class ReadableContentProvider extends AbstractContentProvider<ReadableContent> implements Iterable<ReadableByteChannel> {
		
		private class It implements Iterator<ReadableByteChannel> {
			
			private final Iterator<ReadableContent> it;
			
			public It() {
				this.it = contents.iterator();
			}

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public ReadableByteChannel next() {
				try {
					return it.next().read();
				} catch (IOException ioe) {
					ioe.printStackTrace();
					return null;
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		}
		
		public ReadableByteChannel channel() throws IOException {
			return contents.get(0).read();
		}
		
		public ReadableByteChannel channel(int i) throws IOException {
			return contents.get(i).read();
		}
		
		public ReadableByteChannel channel(URI uri) throws IOException {
			ReadableContent content = map.get(uri);
			if (content != null) {
				return content.read();
			} else {
				return null;
			}
		}

		@Override
		public Iterator<ReadableByteChannel> iterator() {
			return new It();
		}
	}

	public static class WritableContentProvider extends AbstractContentProvider<WritableContent> implements Iterable<WritableByteChannel> {
		
		private class It implements Iterator<WritableByteChannel> {
			private final boolean append;
			private final Iterator<WritableContent> it;
			
			public It(boolean append) {
				this.append = append;
				this.it = contents.iterator();
			}

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public WritableByteChannel next() {
				try {
					return append ? it.next().append() : it.next().write();
				} catch (IOException ioe) {
					ioe.printStackTrace();
					return null;
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		}
		
		public WritableByteChannel channel() throws IOException {
			return contents.get(0).write();
		}

		public WritableByteChannel channel(int i) throws IOException {
			return contents.get(i).write();
		}

		public WritableByteChannel channel(URI uri) throws IOException {
			WritableContent content = map.get(uri);
			if (content != null) {
				return content.write();
			} else {
				return null;
			}
		}

		public WritableByteChannel appendChannel() throws IOException {
			return contents.get(0).append();
		}

		public WritableByteChannel appendChannel(int i) throws IOException {
			return contents.get(i).append();
		}

		@Override
		public Iterator<WritableByteChannel> iterator() {
			return new It(false);
		}

		public Iterator<WritableByteChannel> appendIterator() {
			return new It(true);
		}

		public WritableByteChannel appendChannel(URI uri) throws IOException {
			WritableContent content = map.get(uri);
			if (content != null) {
				return content.append();
			} else {
				return null;
			}
		}
	}
	
	public ReadableContentProvider getInput(CloverURI sourceList) {
		return getInput(sourceList, null);
	}

	public ReadableContentProvider getInput(CloverURI sourceList, ReadParameters params) {
		ReadableContentProvider result = new ReadableContentProvider();
		sourceList = sourceList.getAbsoluteURI();
		List<SingleCloverURI> sources = sourceList.split();
		for (SingleCloverURI sourceExpression: sources) {
			Operation operation = getOperation(OperationKind.READ, sourceExpression);
			IOperationHandler handler = findHandler(operation);
			if (handler != null) {
				for (SingleCloverURI sourceUri: resolve(sourceExpression)) {
					try {
						ReadableContent content = handler.getInput(sourceUri, params);
						result.add(sourceUri.toURI(), content);
					} catch (IOException ioe) {
						result.add(sourceUri.toURI(), null);
					}
				}
			} else {
				throw new UnsupportedOperationException(operation.toString());
			}
		}
		return result;
	}
	
	public WritableContentProvider getOutput(CloverURI targetList) {
		return getOutput(targetList, null);
	}

	public WritableContentProvider getOutput(CloverURI targetList, WriteParameters params) {
		WritableContentProvider result = new WritableContentProvider();
		targetList = targetList.getAbsoluteURI();
		List<SingleCloverURI> targets = targetList.split();
		for (SingleCloverURI targetExpression: targets) {
			Operation operation = getOperation(OperationKind.WRITE, targetExpression);
			IOperationHandler handler = findHandler(operation);
			if (handler != null) {
				for (SingleCloverURI targetUri: resolve(targetExpression)) {
					try {
						WritableContent content = handler.getOutput(targetUri, params);
						result.add(targetUri.toURI(), content);
					} catch (IOException ioe) {
						result.add(targetUri.toURI(), null);
					}
				}
			} else {
				throw new UnsupportedOperationException(operation.toString());
			}
		}
		return result;
	}

	public File getFile(CloverURI target) {
		return getFile(target, null);
	}

	public File getFile(CloverURI targetList, FileParameters params) {
		targetList = targetList.getAbsoluteURI();
		SingleCloverURI target = targetList.getSingleURI();
		Operation operation = getOperation(OperationKind.FILE, target);
		IOperationHandler handler = findHandler(operation);
		if (handler != null) {
			// resolve to deal with wildcards or maybe symbolic links etc.
			ResolveResult resolveResult = resolve(target);
			if (!resolveResult.success()) {
				throw new RuntimeException(format(FileOperationMessages.getString("FileManager.resolve_failed"), target.getPath()));  //$NON-NLS-1$
			}
			List<SingleCloverURI> resolved = resolveResult.getResult();
			if (resolved.size() > 1) {
				throw new IllegalArgumentException(format(FileOperationMessages.getString("FileManager.more_than_one_matching_file"), target.getPath())); //$NON-NLS-1$
			} else if (resolved.size() == 1) {
				target = resolved.get(0);
			}
			try {
				return handler.getFile(target, params);
			} catch (Exception ex) {}
		} else {
			throw new UnsupportedOperationException(operation.toString());
		}
		
		return null;
	}

	public DeleteResult delete(String target, DeleteParameters params) {
		if (target == null) {
			return new DeleteResult().setException(new NullPointerException(FileOperationMessages.getString("FileManager.delete_target_is_null"))); //$NON-NLS-1$
		}
		CloverURI targetCloverUri = CloverURI.createURI(target);
		return delete(targetCloverUri, params);
	}

	public DeleteResult delete(CloverURI targetList, DeleteParameters params) {
		DeleteResult result = new DeleteResult();
		targetList = targetList.getAbsoluteURI();
		List<SingleCloverURI> targets = targetList.split();
		for (SingleCloverURI targetExpression: targets) {
			Operation operation = getOperation(OperationKind.DELETE, targetExpression);
			IOperationHandler handler = findHandler(operation);
			if (handler != null) {
				for (SingleCloverURI target: resolve(targetExpression)) {
					try {
						SingleCloverURI deleted = handler.delete(target, params); 
						if (deleted != null) {
							result.add(deleted);
						} else {
							result.addError(target, FileOperationMessages.getString("FileManager.delete_failed")); //$NON-NLS-1$
						}
					} catch (FileNotFoundException fnfe) {
						result.addError(target, format(FileOperationMessages.getString("FileManager.file_not_found"), fnfe.getMessage())); //$NON-NLS-1$
					} catch (Exception ex) {
						result.addError(target, ex.getMessage());
					}
				}
			} else {
				throw new UnsupportedOperationException(operation.toString());
			}
		}
		return result;
	}
	
	public ResolveResult resolve(CloverURI targetList) {
		return resolve(targetList, null);
	}

	public ResolveResult resolve(CloverURI targetList, ResolveParameters params) {
		ResolveResult result = new ResolveResult();
		targetList = targetList.getAbsoluteURI();
		List<SingleCloverURI> targets = targetList.split();
		List<IOperationHandler> handlers = new ArrayList<IOperationHandler>(targets.size());
		for (SingleCloverURI targetExpression: targets) {
			Operation operation = getOperation(OperationKind.RESOLVE, targetExpression);
			IOperationHandler handler = findHandler(operation);
			if (!targetExpression.isQuoted() && (handler == null)) {
				return result.setException(new UnsupportedOperationException(operation.toString()));
			}
			handlers.add(handler);
		}
		Iterator<IOperationHandler> h = handlers.iterator();
		for (SingleCloverURI targetExpression: targets) {
			IOperationHandler handler = h.next();
			if (targetExpression.isQuoted()) {
				result.add(targetExpression, Arrays.asList(targetExpression));
			} else {
				try {
					List<SingleCloverURI> resolved = handler.resolve(targetExpression, params);
					if (resolved != null) {
						result.add(targetExpression, resolved);
					} else {
						result.addError(targetExpression, format(FileOperationMessages.getString("FileManager.resolve_failed"), targetExpression.getPath())); //$NON-NLS-1$
					}
				} catch (Exception ex) {
					result.addError(targetExpression, ex.getMessage());
				}
			}
		}
		return result;
	}
	
	private static final String ASTERISK = "*"; //$NON-NLS-1$
	private static final String QUESTION_MARK = "?"; //$NON-NLS-1$
	private static final char PATH_SEPARATOR = '/';

	static boolean hasWildcards(String path) {
		return path.contains(QUESTION_MARK) || path.contains(ASTERISK);
	}

	static List<String> getParts(String uri) {
		List<String> result = new ArrayList<String>();
		String remaining = uri;
		while (!remaining.isEmpty()) {
			int asteriskIdx = remaining.indexOf(ASTERISK);
			int questionIdx = remaining.indexOf(QUESTION_MARK);
			int wildcardIdx = -1;
			if (asteriskIdx >= 0) {
				wildcardIdx = asteriskIdx;
			}
			if (questionIdx >= 0 && (questionIdx < wildcardIdx || wildcardIdx < 0)) {
				wildcardIdx = questionIdx;
			}
			if (wildcardIdx >= 0) {
				int prevPathSepIdx = -1;
				for (int i = wildcardIdx - 1; i >= 0; i--) {
					if (remaining.charAt(i) == PATH_SEPARATOR) {
						prevPathSepIdx = i;
						break;
					}
				}
				if (prevPathSepIdx >= 0) {
					if (prevPathSepIdx > 0) {
						result.add(remaining.substring(0, prevPathSepIdx + 1));
						remaining = remaining.substring(prevPathSepIdx + 1);
					}
				}
				int nextPathSepIdx = -1;
				for (int i = 1; i < remaining.length(); i++) {
					if (remaining.charAt(i) == PATH_SEPARATOR) {
						nextPathSepIdx = i;
						break;
					}
				}
				if (nextPathSepIdx >= 0) {
					result.add(remaining.substring(0, nextPathSepIdx + 1));
					remaining = remaining.substring(nextPathSepIdx + 1);
				} else {
					result.add(remaining);
					break;
				}
			} else {
				result.add(remaining);
				break;
			}
		}
		return result;
	}
	
	private static class WildcardInfoFilter extends WildcardFilter {
		
		public WildcardInfoFilter(String pattern, boolean onlyDirectories) {
			super(pattern, onlyDirectories);
		}
		
		public boolean accept(Info file) {
			return accept(file.getName(), file.isDirectory());
		}
		
	}

	private List<Info> expand(Info base, String part, boolean directory) {
		if (base == null) {
			throw new NullPointerException("base"); //$NON-NLS-1$
		}
		if (!base.isDirectory()) {
			throw new IllegalArgumentException(format(FileOperationMessages.getString("FileManager.not_a_directory"), base)); //$NON-NLS-1$
		}
		if (hasWildcards(part)) {
			part = URIUtils.urlDecode(part);
			List<Info> children = list(CloverURI.createSingleURI(base.getURI())).getResult();
			List<Info> result = new ArrayList<Info>();
			WildcardInfoFilter filter = new WildcardInfoFilter(part, directory);
			for (Info child: children) {
				if (filter.accept(child)) {
					result.add(child);
				}
			}
			return result; 
		} else {
			URI child = URIUtils.getChildURI(base.getURI(), URI.create(part)); 
			Info info = getInstance().info(CloverURI.createSingleURI(child)).getInfo();
			if (info != null) {
				return Arrays.asList(info);
			} else {
				return new ArrayList<Info>(0);
			}
		}
	}

	public List<SingleCloverURI> defaultResolve(SingleCloverURI wildcards) {
		String uriString = wildcards.toString();
		if (wildcards.isRelative() || !hasWildcards(uriString)) {
			return Arrays.asList(wildcards);
		}
		
		List<String> parts = getParts(uriString);
		Info base = info((SingleCloverURI) CloverURI.createURI(parts.get(0))).getInfo();
		if (base == null) {
			return new ArrayList<SingleCloverURI>(0);
		}
		List<Info> bases = Arrays.asList(base);
		for (Iterator<String> it = parts.listIterator(1); it.hasNext(); ) {
			String part = it.next();
			List<Info> nextBases = new ArrayList<Info>(bases.size());
			boolean hasPathSeparator = part.endsWith(URIUtils.PATH_SEPARATOR);
			if (hasPathSeparator) {
				part = part.substring(0, part.length()-1);
			}
			for (Info i: bases) {
				nextBases.addAll(expand(i, part, it.hasNext() || hasPathSeparator));
			}
			bases = nextBases;
		}
		
		List<SingleCloverURI> result = new ArrayList<SingleCloverURI>(bases.size());
		for (Info info: bases) {
			result.add(CloverURI.createSingleURI(info.getURI()));
		}
		return result;
	}
	
	public boolean exists(CloverURI target) {
		for (SingleCloverURI uri: target.split()) {
			if (!info(uri).exists()) {
				return false;
			}
		}
		return true;
	}
	
	public boolean exists(SingleCloverURI target) {
		return info(target) != null;
	}
	
	public boolean isDirectory(CloverURI target) {
		for (SingleCloverURI uri: target.split()) {
			if (!info(uri).isDirectory()) {
				return false;
			}
		}
		return true;
	}
	
	public boolean isDirectory(SingleCloverURI target) {
		return info(target).isDirectory();
	}
	
	public boolean isFile(CloverURI target) {
		for (SingleCloverURI uri: target.split()) {
			if (!info(uri).isFile()) {
				return false;
			}
		}
		return true;
	}
	
	public boolean isFile(SingleCloverURI target) {
		return info(target).isFile(); 
	}
	
	public ListResult list(String target, ListParameters params) {
		if (target == null) {
			return new ListResult().setException(new NullPointerException(FileOperationMessages.getString("FileManager.list_target_is_null"))); //$NON-NLS-1$
		}
		CloverURI targetCloverUri = CloverURI.createURI(target);
		return list(targetCloverUri, params);
	}

	public ListResult list(CloverURI targetList, ListParameters params) {
		ListResult result = new ListResult();
		targetList = targetList.getAbsoluteURI();
		List<SingleCloverURI> targets = targetList.split();
		List<IOperationHandler> handlers = new ArrayList<IOperationHandler>(targets.size());
		
		for (SingleCloverURI expression: targets) {
			Operation listOperation = getOperation(OperationKind.LIST, expression);
			IOperationHandler listHandler = findHandler(listOperation);
			if (listHandler == null) {
				return result.setException(new UnsupportedOperationException(listOperation.toString()));
			}
			handlers.add(listHandler);
			// TODO find resolve handlers
		}
		Iterator<IOperationHandler> h = handlers.iterator();
		for (SingleCloverURI expression: targetList.split()) {
			IOperationHandler handler = h.next();
			ResolveResult resolved = resolve(expression);
			for (SingleCloverURI target: resolved) {
				try {
					List<Info> infos = handler.list(target, params); 
					if (infos != null) {
						result.add(target, infos);
					}
				} catch (FileNotFoundException fnfe) {
					result.addError(target, format(FileOperationMessages.getString("FileManager.file_not_found"), fnfe.getMessage())); //$NON-NLS-1$
				} catch (Exception ex) {
					result.addError(target, ex.getMessage());
				}
			}
		}
		return result;
	}
	
	private SingleCloverURI createSingle(SingleCloverURI target, CreateParameters params) throws IOException {
		params = params.clone();
		Operation operation = getOperation(OperationKind.CREATE, target);
		IOperationHandler handler = findHandler(operation);
		if (handler != null) {
			if (target.toURI().toString().endsWith(URIUtils.PATH_SEPARATOR)) {
				params.setDirectory(true);
			}
			return handler.create(target, params); 
		}
		throw new UnsupportedOperationException(operation.toString());
	}

	public CreateResult create(String target, CreateParameters params) {
		if (target == null) {
			return new CreateResult().setException(new NullPointerException(FileOperationMessages.getString("FileManager.create_target_is_null"))); //$NON-NLS-1$
		}
		CloverURI targetCloverUri = CloverURI.createURI(target);
		return create(targetCloverUri, params);
	}

	public CreateResult create(CloverURI target, CreateParameters params) {
		CreateResult result = new CreateResult();
		target = target.getAbsoluteURI();
		for (SingleCloverURI part: target.split()) {
			try {
				SingleCloverURI resultUri = createSingle(part, params);
				if (resultUri != null) {
					result.add(part, resultUri);
				} else {
					result.addError(part, FileOperationMessages.getString("FileManager.create_failed")); //$NON-NLS-1$
				}
			} catch (FileNotFoundException fnfe) {
				result.addError(part, format(FileOperationMessages.getString("FileManager.file_not_found"), fnfe.getMessage())); //$NON-NLS-1$
			} catch (Exception ex) {
				result.addError(part, ex.getMessage());
			}
		}
		return result;
	}
	
	public CopyResult copy(CloverURI source, CloverURI target) {
		return copy(source, target, new CopyParameters());
	}
	
	public MoveResult move(CloverURI source, CloverURI target) {
		return move(source, target, new MoveParameters());
	}

	public DeleteResult delete(CloverURI target) {
		return delete(target, new DeleteParameters());
	}

	public ListResult list(CloverURI parent) {
		return list(parent, new ListParameters());
	}
	
	public CreateResult create(CloverURI target) {
		return create(target, new CreateParameters());
	}

//	public List<Info> info(CloverURI target) {
//		List<Info> result = new ArrayList<Info>();
//		for (SingleCloverURI uri: target.split()) {
//			result.add(info(uri));
//		}
//		return result;
//	}
	
	public InfoResult info(CloverURI target) {
		return info(target, null);
	}

	public InfoResult info(CloverURI target, InfoParameters params) {
		InfoResult result = new InfoResult();
		if (!target.isSingle()) {
			return result.setException(new IllegalArgumentException(FileOperationMessages.getString("FileManager.single_URI_expected"))); //$NON-NLS-1$
		}
		SingleCloverURI singleTargetURI = target.getSingleURI().getAbsoluteURI();
		Operation operation = getOperation(OperationKind.INFO, singleTargetURI);
		IOperationHandler handler = findHandler(operation);
		if (handler == null) {
			return result.setException(new UnsupportedOperationException(operation.toString()));
		}
		try {
			Info info = handler.info(singleTargetURI, params); 
			result.add(singleTargetURI, info);
		} catch (Exception ex) {
			result.addError(singleTargetURI, ex.getMessage());
		}
		return result;
	}

	@Override
	public String toString() {
		return String.format("File Manager: %s", handlers); //$NON-NLS-1$
	}
	
	

}
