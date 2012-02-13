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
package org.jetel.component.xpathparser.bean;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.jxpath.CompiledExpression;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.Pointer;
import org.jetel.component.xpathparser.XPathEvaluator;
import org.jetel.component.xpathparser.mappping.FieldMapping;
import org.jetel.component.xpathparser.mappping.MappingContext;
import org.jetel.component.xpathparser.mappping.MappingElement;

/**
 * XPath evaluator using JXPath Apache library to query beans.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.12.2011
 */
public class JXPathEvaluator implements XPathEvaluator {
	
	private Map<String, CompiledExpression> expressions = new HashMap<String, CompiledExpression>();
	
	private Map<Object, JXPathContext> relativeContexts = new IdentityHashMap<Object, JXPathContext>();
	
	@Override
	public void reset() {
		expressions.clear();
		relativeContexts.clear();
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Object> iterate(String xpath, Map<String, String> namespaceBinding, Object evaluationContext,
			final MappingElement mapping) {
		
		if (evaluationContext == null) {
			return Collections.emptyList().iterator();
		}
		CompiledExpression expression = getExpression(xpath);
		final JXPathContext context = getContext(mapping, evaluationContext);
		final Iterator<Pointer> iterator = expression.iteratePointers(context);
		/*
		 * TODO? we are here relying on iteration from parser,
		 * maybe better is to create all contexts before return
		 */
		return new Iterator<Object>() {

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Object next() {
				
				Pointer pointer = iterator.next();
				Object value = pointer.getValue();
				if (value != null && mapping instanceof MappingContext) {
					/*
					 * store relative context so that we can fetch it when
					 * evaluating upon value
					 */
					relativeContexts.put(value, context.getRelativeContext(pointer));
				}
				return value;
			}

			@Override
			public void remove() {
				iterator.remove();
			}
			
		};
	}

	@Override
	public Object evaluatePath(String xpath, Map<String, String> namespaceBinding, Object evaluationContext,
			MappingElement mapping) {
		
		if (evaluationContext == null) {
			return null;
		}
		CompiledExpression expression = getExpression(xpath);
		JXPathContext context = getContext(mapping, evaluationContext);
		Pointer pointer = expression.getPointer(context, null);
		Object value = pointer.getValue();
		if (value != null && mapping instanceof MappingContext) {
			/*
			 * store relative context so that we can fetch it when
			 * evaluating upon value
			 */
			relativeContexts.put(value, context.getRelativeContext(pointer));
		}
		return value;
	}

	@Override
	public Object evaluateNodeName(String nodeName, Map<String, String> namespaceBinding, Object evaluationContext,
			MappingElement mapping) {
		
		if (evaluationContext == null) {
			return null;
		}
		try {
			BeanInfo info = Introspector.getBeanInfo(evaluationContext.getClass(), Object.class);
			for (PropertyDescriptor descriptor : info.getPropertyDescriptors()) {
				if (descriptor.getName().equals(nodeName)) {
					return descriptor.getReadMethod().invoke(evaluationContext);
				}
			}
		} catch (IntrospectionException e) {
			return null;
		} catch (Exception e) {
			throw new RuntimeException("Error reading node " + nodeName + " from " + evaluationContext);
		}
		return null;
	}
	
	private JXPathContext getContext(MappingElement mapping, Object evaluationContext) {
		
		if (mapping instanceof FieldMapping) {
			return getContext(((FieldMapping)mapping).getParent(), evaluationContext);
		} else if (mapping instanceof MappingContext) {
			return getContext((MappingContext)mapping, evaluationContext);
		}
		return null;
	}
	
	private JXPathContext getContext(MappingContext mappingContext, Object evaluationContext) {
		
		if (mappingContext.getParent() == null) {
			/*
			 * root context
			 */
			JXPathContext context = JXPathContext.newContext(evaluationContext);
			context.setLenient(true);
			return context;
		}
		JXPathContext context = relativeContexts.get(evaluationContext);
		if (context == null) {
			throw new IllegalStateException("Could not find relative context for bean: " + evaluationContext);
		}
		return context;
	}

	private CompiledExpression getExpression(String xpath) {
		
		CompiledExpression expression = expressions.get(xpath);
		if (expression == null) {
			expression = JXPathContext.compile(xpath);
			expressions.put(xpath, expression);
		}
		return expression;
	}
}
