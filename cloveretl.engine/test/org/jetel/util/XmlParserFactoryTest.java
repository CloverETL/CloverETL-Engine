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

import org.jetel.test.CloverTestCase;
import org.jetel.util.XmlParserFactory.DocumentBuilderFactoryConfig;
import org.jetel.util.XmlParserFactory.DocumentBuilderProvider;

/**
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21. 9. 2016
 */
public class XmlParserFactoryTest extends CloverTestCase {

	public void testGetDocumentBuilder() {
		DocumentBuilderProvider provider = XmlParserFactory.getDocumentBuilder();
		assertNotNull(provider.getDocumentBuilder());
		XmlParserFactory.releaseDocumentBuilder(provider);
		
		DocumentBuilderProvider provider1 = XmlParserFactory.getDocumentBuilder();
		assertTrue(provider.getDocumentBuilder() == provider1.getDocumentBuilder());
		XmlParserFactory.releaseDocumentBuilder(provider1);
		
		DocumentBuilderProvider provider2 = XmlParserFactory.getDocumentBuilder(DocumentBuilderFactoryConfig.getDefault());
		assertTrue(provider.getDocumentBuilder() == provider2.getDocumentBuilder());

		DocumentBuilderProvider provider3 = XmlParserFactory.getDocumentBuilder(DocumentBuilderFactoryConfig.getDefault());
		assertTrue(provider.getDocumentBuilder() != provider3.getDocumentBuilder());
		XmlParserFactory.releaseDocumentBuilder(provider2);
		XmlParserFactory.releaseDocumentBuilder(provider3);

		DocumentBuilderProvider provider4 = XmlParserFactory.getDocumentBuilder(DocumentBuilderFactoryConfig.getDefault().withNamespaceAware());
		assertNotNull(provider4.getDocumentBuilder());
		assertTrue(provider4.getDocumentBuilderFactory().isNamespaceAware());
		assertFalse(provider4.getDocumentBuilderFactory().isCoalescing());
		XmlParserFactory.releaseDocumentBuilder(provider4);
		
		DocumentBuilderProvider provider5 = XmlParserFactory.getDocumentBuilder(DocumentBuilderFactoryConfig.getDefault().withNamespaceAware());
		assertTrue(provider5.getDocumentBuilder() == provider5.getDocumentBuilder());
		XmlParserFactory.releaseDocumentBuilder(provider5);

		DocumentBuilderProvider provider6 = XmlParserFactory.getDocumentBuilder(DocumentBuilderFactoryConfig.getDefault().withCoalescing());
		assertFalse(provider6.getDocumentBuilderFactory().isNamespaceAware());
		assertTrue(provider6.getDocumentBuilderFactory().isCoalescing());
		XmlParserFactory.releaseDocumentBuilder(provider6);

		DocumentBuilderProvider provider7 = XmlParserFactory.getDocumentBuilder(DocumentBuilderFactoryConfig.getDefault().withCoalescing().withNamespaceAware());
		assertTrue(provider7.getDocumentBuilderFactory().isNamespaceAware());
		assertTrue(provider7.getDocumentBuilderFactory().isCoalescing());
		
		assertTrue(provider4.getDocumentBuilder() != provider6.getDocumentBuilder());
		assertTrue(provider4.getDocumentBuilder() != provider7.getDocumentBuilder());
		
		DocumentBuilderProvider provider8 = XmlParserFactory.getDocumentBuilder(DocumentBuilderFactoryConfig.getDefault().withCoalescing().withNamespaceAware());
		assertTrue(provider7.getDocumentBuilder() != provider8.getDocumentBuilder());
		XmlParserFactory.releaseDocumentBuilder(provider7);
		XmlParserFactory.releaseDocumentBuilder(provider8);
	}
	
}
