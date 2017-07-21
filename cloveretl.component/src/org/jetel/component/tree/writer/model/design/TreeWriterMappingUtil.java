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

import java.util.ArrayList;
import java.util.List;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 24.11.2011
 */
public class TreeWriterMappingUtil {

	private TreeWriterMappingUtil() {
		// this class is not mean to be instantiated
	}

	public static boolean isNamespacePrefixAvailable(ContainerNode element, String prefix) {
		return isNamespacePrefixAvailable(element, prefix, null);
	}

	public static boolean isNamespacePrefixAvailable(ContainerNode container, String prefix, Namespace exclude) {
		if (container instanceof ObjectNode) {
			ObjectNode namespaceSupportingContainer = (ObjectNode) container;
			for (Namespace namespace : namespaceSupportingContainer.getNamespaces()) {
				if (exclude == namespace) {
					continue;
				}
				String otherPrefix = namespace.getProperty(MappingProperty.NAME);
				if (prefix != null && prefix.equals(otherPrefix)) {
					return true;
				}
			}
		}

		if (container.getParent() != null) {
			return isNamespacePrefixAvailable(container.getParent(), prefix, exclude);
		}
		return false;
	}

	public static ContainerNode getFirstContainerNode(String name, ContainerNode parent) {
		for (AbstractNode child : parent.getChildren()) {
			if ((child.getType() == AbstractNode.COLLECTION || child.getType() == AbstractNode.ELEMENT) && name.equals(child.getProperty(MappingProperty.NAME))) {
				return (ContainerNode) child;
			}
		}

		return null;
	}

	public static List<ObjectNode> getChildObjectNodes(String name, ContainerNode parent) {
		List<ObjectNode> children = new ArrayList<ObjectNode>();
		for (AbstractNode child : parent.getChildren()) {
			if (child.getType() == AbstractNode.ELEMENT && name.equals(child.getProperty(MappingProperty.NAME))) {
				children.add((ObjectNode) child);
			}
		}

		return children;
	}

}
