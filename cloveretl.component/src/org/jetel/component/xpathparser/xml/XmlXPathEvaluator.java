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
package org.jetel.component.xpathparser.xml;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.transform.Source;

import net.sf.saxon.om.Axis;
import net.sf.saxon.om.AxisIterator;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.sxpath.IndependentContext;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.tinytree.TinyNodeImpl;
import net.sf.saxon.trans.XPathException;

import org.jetel.component.xpathparser.XPathEvaluator;
import org.jetel.component.xpathparser.mappping.FieldMapping;
import org.jetel.component.xpathparser.mappping.MappingContext;
import org.jetel.component.xpathparser.mappping.MappingElement;
import org.jetel.exception.JetelRuntimeException;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 19 Jan 2012
 */
public class XmlXPathEvaluator implements XPathEvaluator {

	private net.sf.saxon.sxpath.XPathEvaluator evaluator = new net.sf.saxon.sxpath.XPathEvaluator();
	/* cache expression */
	private Map<String, XPathExpression> expressions = new HashMap<String, XPathExpression>();

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Object> iterate(String xpath, Map<String, String> namespaceBinding, Object context,
			MappingElement mapping) {
		setNamespacesToEvaluator(namespaceBinding);
		XPathExpression expression = getExpression(xpath);

		Source source;
		if (context instanceof Source) {
			source = (Source) context;
		} else {
			throw new IllegalStateException("unsupported context type");
		}

		try {
			return expression.evaluate(source).iterator();
		} catch (XPathException e) {
			throw new JetelRuntimeException("XPath evaluation failed");
		}
	}

	@Override
	public Object evaluatePath(String xpath, Map<String, String> namespaceBinding, Object context,
			MappingElement element) {
		setNamespacesToEvaluator(namespaceBinding);
		XPathExpression expression = getExpression(xpath);

		List<?> nodeList;
		try {
			nodeList = expression.evaluate((Source) context);
		} catch (XPathException e) {
			throw new JetelRuntimeException(e);
		}
		
		if (element instanceof FieldMapping) {
			return nodeList;
		} else if (element instanceof MappingContext) {
			switch (nodeList.size()) {
			case 0:
				return null;
			case 1:
				return nodeList.get(0);
			default:
				throw new JetelRuntimeException("XPath '" + xpath + "' contains two or more values!");
			}
		} else {
			throw new IllegalArgumentException("Unknown type of mapping element " + element);
		}
	}

	@Override
	public Object evaluateNodeName(String nodeName, Map<String, String> namespaceBinding, Object context,
			MappingElement element) {
		setNamespacesToEvaluator(namespaceBinding);
		NodeInfo typedContext = (NodeInfo) context;
		AxisIterator childIterator = typedContext.iterateAxis(Axis.CHILD);
		while (childIterator.moveNext()) {
			TinyNodeImpl child = (TinyNodeImpl) childIterator.current();
			if (child.getDisplayName().equals(nodeName)) {
				return child.getStringValue();
			}
		}
		return null;
	}

	@Override
	public void reset() {
	}

	private void setNamespacesToEvaluator(Map<String, String> namespaceBinding) {
		IndependentContext context = new IndependentContext();
		for (Entry<String, String> entry : namespaceBinding.entrySet()) {
			if (entry.getKey().equals("")) {
				evaluator.setDefaultElementNamespace(entry.getValue());
			} else {
				context.declareNamespace(entry.getKey(), entry.getValue());
			}

		}

		evaluator.setNamespaceResolver(context.getNamespaceResolver());
	}

	private XPathExpression getExpression(String xpath) {

		XPathExpression expression = expressions.get(xpath);
		if (expression == null) {
			try {
				expression = evaluator.createExpression(xpath);
			} catch (XPathException e) {
				throw new JetelRuntimeException(e);
			}
			expressions.put(xpath, expression);
		}
		return expression;
	}

}
