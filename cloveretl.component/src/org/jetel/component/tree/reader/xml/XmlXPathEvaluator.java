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
package org.jetel.component.tree.reader.xml;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.transform.Source;

import net.sf.saxon.Configuration;
import net.sf.saxon.functions.FunctionLibrary;
import net.sf.saxon.sxpath.IndependentContext;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.trans.XPathException;

import org.jetel.component.tree.reader.XPathEvaluator;
import org.jetel.component.tree.reader.mappping.FieldMapping;
import org.jetel.component.tree.reader.mappping.MappingContext;
import org.jetel.component.tree.reader.mappping.MappingElement;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.string.TagName;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 19 Jan 2012
 */
public class XmlXPathEvaluator implements XPathEvaluator {

	public static final String TAG_NAME_FUNCTIONS_NAMESPACE_URI = "http://www.cloveretl.com/ns/TagNameEncoder";
	
	private net.sf.saxon.sxpath.XPathEvaluator evaluator = new net.sf.saxon.sxpath.XPathEvaluator();
	/* cache expression */
	private Map<String, XPathExpression> expressions = new HashMap<String, XPathExpression>();
	/*
	 * Mule hack to increase performance, for clearing and setting namespaces on evaluator took too long.
	 * TODO update to Saxon 9 and investigate all possibilities for best performance.
	 */
	private IndependentContext context = new IndependentContext();
	public XmlXPathEvaluator() {
		FunctionLibrary javaFunctionLibrary = evaluator.getConfiguration().getExtensionBinder("java");
		Configuration.getPlatform().declareJavaClass(javaFunctionLibrary, TAG_NAME_FUNCTIONS_NAMESPACE_URI, TagName.class);
	}
	
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
			throw new JetelRuntimeException("XPath evaluation failed", e);
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
	public Object evaluateNodeName(String nodeName, Map<String, String> namespaceBinding, Object context, MappingElement element) {
		return evaluatePath(nodeName, namespaceBinding, context, element);
	}

	@Override
	public void reset() {
	}

	private void setNamespacesToEvaluator(Map<String, String> namespaceBinding) {
		context.clearAllNamespaces();
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

