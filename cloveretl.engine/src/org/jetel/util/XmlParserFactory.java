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
package org.jetel.util;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.jetel.exception.JetelRuntimeException;

/**
 * This factory should be used for XML DOM parsing.
 * Instantiation of {@link DocumentBuilderFactory} and {@link DocumentBuilder} classes is
 * time consuming operation, so it is worth to reuse them if possible.
 * Recommended usage:
 * <code>
 * DocumentBuilderProvider documentBuilderProvider = null;
 * try {
 * 		documentBuilderProvider = XmlParserFactory.getDocumentBuilder();
 * 		documentBuilderProvider.getDocumentBuilder().parse()...
 * } finally {
 * 		XmlParserFactory.releaseDocumentBuilder(documentBuilderProvider);
 * }
 * 
 * </code>
 *  
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19. 9. 2016
 */
public class XmlParserFactory {

	private static Map<DocumentBuilderFactory, Queue<DocumentBuilderProvider>> documentBuildersCache = new HashMap<>();

	private static Map<DocumentBuilderFactoryConfig, DocumentBuilderFactory> documentBuilderFactoriesCache = new HashMap<>();

	/**
	 * @return {@link DocumentBuilder} created from default {@link DocumentBuilderFactory}
	 */
	public static DocumentBuilderProvider getDocumentBuilder() {
		return getDocumentBuilder(DocumentBuilderFactoryConfig.getDefault());
	}

	/**
	 * @param config
	 * @return {@link DocumentBuilder} created from default {@link DocumentBuilderFactory} with the given configuration
	 */
	public static DocumentBuilderProvider getDocumentBuilder(DocumentBuilderFactoryConfig config) {
		return getDocumentBuilder(getDocumentBuilderFactory(config));
	}

	/**
	 * @param documentBuilderFactory
	 * @return {@link DocumentBuilder} created from the given {@link DocumentBuilderFactory}
	 */
	public static synchronized DocumentBuilderProvider getDocumentBuilder(DocumentBuilderFactory documentBuilderFactory) {
		//try to find requested DocumentBuilder in the cache
		Queue<DocumentBuilderProvider> documentBuilders = documentBuildersCache.get(documentBuilderFactory);
		if (documentBuilders != null) {
			DocumentBuilderProvider documentBuilder = documentBuilders.poll();
			if (documentBuilder != null) {
				return documentBuilder;
			}
		}

		//the requested DocumentBuilder has not been found in cache - let's create new one
		try {
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			documentBuilder.setErrorHandler(null); // this avoid to print out to standard error output message "[Fatal Error] :1:1: Content is not allowed in prolog.", see CLO-1652
			return new DocumentBuilderProvider(documentBuilderFactory, documentBuilder);
		} catch (ParserConfigurationException e) {
			throw new JetelRuntimeException(e);
		}
	}

	/**
	 * This method should be used to return DocumentBuilder created by getDocumentBuilder() method to the cache.
	 * @param documentBuilderProvider
	 */
	public static synchronized void releaseDocumentBuilder(DocumentBuilderProvider documentBuilderProvider) {
		if (documentBuilderProvider != null) {
			Queue<DocumentBuilderProvider> documentBuilders = documentBuildersCache.get(documentBuilderProvider.documentBuilderFactory);
			if (documentBuilders == null) {
				documentBuilders = new ArrayDeque<>();
				documentBuildersCache.put(documentBuilderProvider.documentBuilderFactory, documentBuilders);
			}
			documentBuilders.add(documentBuilderProvider);
		}
	}
	
	private static synchronized DocumentBuilderFactory getDocumentBuilderFactory(DocumentBuilderFactoryConfig config) {
		DocumentBuilderFactory factory = documentBuilderFactoriesCache.get(config);
		if (factory == null) {
			factory = config.createDocumentBuilderFactory();
			documentBuilderFactoriesCache.put(config, factory);
		}
		return factory;
	}
	
	/**
	 * This class is handler for {@link DocumentBuilder}.
	 */
	public static class DocumentBuilderProvider {
		private DocumentBuilderFactory documentBuilderFactory;
		
		private DocumentBuilder documentBuilder;

		public DocumentBuilderProvider(DocumentBuilderFactory documentBuilderFactory, DocumentBuilder documentBuilder) {
			this.documentBuilderFactory = documentBuilderFactory;
			this.documentBuilder = documentBuilder;
		}
		
		public DocumentBuilder getDocumentBuilder() {
			return documentBuilder;
		}
		
		public DocumentBuilderFactory getDocumentBuilderFactory() {
			return documentBuilderFactory;
		}
	}
	
	/**
	 * This class represents configuration for {@link DocumentBuilderFactory}.
	 */
	public static class DocumentBuilderFactoryConfig {
		private boolean namespaceAware = false;
		private boolean coalescing = false;

		private static final DocumentBuilderFactoryConfig DEFAULT = new DocumentBuilderFactoryConfig();
		
		public static final DocumentBuilderFactoryConfig getDefault() {
			return DEFAULT;
		}

		private DocumentBuilderFactoryConfig() {
		}
		
		private DocumentBuilderFactory createDocumentBuilderFactory() {
			DocumentBuilderFactory result = DocumentBuilderFactory.newInstance();
			result.setNamespaceAware(namespaceAware);
			result.setCoalescing(coalescing);
			return result;
		}
		
		public DocumentBuilderFactoryConfig withNamespaceAware() {
			DocumentBuilderFactoryConfig result = duplicate();
			result.namespaceAware = true;
			return result;
		}

		public DocumentBuilderFactoryConfig withCoalescing() {
			DocumentBuilderFactoryConfig result = duplicate();
			result.coalescing = true;
			return result;
		}
		
		private DocumentBuilderFactoryConfig duplicate() {
			DocumentBuilderFactoryConfig result = new DocumentBuilderFactoryConfig();
			result.namespaceAware = this.namespaceAware;
			result.coalescing = this.coalescing;
			return result;
		}
		
		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (!(other instanceof DocumentBuilderFactoryConfig)) {
				return false;
			}
			DocumentBuilderFactoryConfig otherConfig = (DocumentBuilderFactoryConfig) other;
			
			return EqualsUtil.areEqual(this.namespaceAware, otherConfig.namespaceAware)
					&& EqualsUtil.areEqual(this.coalescing, otherConfig.coalescing);
		}
		
		@Override
		public int hashCode() {
			int result = HashCodeUtil.SEED;
			result = HashCodeUtil.hash(result, namespaceAware);
			result = HashCodeUtil.hash(result, coalescing);
			return result;
		}
	}
	
}
