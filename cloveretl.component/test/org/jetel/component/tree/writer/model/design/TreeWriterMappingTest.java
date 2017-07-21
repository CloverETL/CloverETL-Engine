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
package org.jetel.component.tree.writer.model.design;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

import javax.xml.stream.XMLStreamException;

import org.jetel.test.CloverTestCase;

/**
 * @author Magdalena Krygielova (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 3. 9. 2014
 */
public class TreeWriterMappingTest extends CloverTestCase {

	private static final String TEST_RESOURCES_DIR = "test-data/"; // path relative to cloveretl.component project dir
	
	public void testFromXmlChunkedValues() throws FileNotFoundException, XMLStreamException {
		File mappingFile = new File(TEST_RESOURCES_DIR + "TreeWriterMappingTest_mapping.xml");
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(mappingFile);
			
			TreeWriterMapping mapping = TreeWriterMapping.fromXml(inputStream);
			ContainerNode xmlRoot = mapping.getRootElement();
			LinkedList<AbstractNode> queue = new LinkedList<AbstractNode>();
			queue.add(xmlRoot);
			while (!queue.isEmpty()) {
				AbstractNode node = queue.pollLast();
				// check that elements contain only one Value element
				if (node instanceof Value) {
					assertEquals(1,node.getParent().getChildren().size());
				}
				if (node instanceof ContainerNode) {
					queue.addAll(((ContainerNode) node).getChildren());
				}
			}
			
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
