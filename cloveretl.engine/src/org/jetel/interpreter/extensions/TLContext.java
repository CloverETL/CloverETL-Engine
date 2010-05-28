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
package org.jetel.interpreter.extensions;

import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;

public class TLContext<T> {

	public T context;
	public TransformationGraph graph;

	public TransformationGraph getGraph() {
		return graph;
	}

	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	public void setContext(T ctx) {
		this.context = ctx;
	}

	public T getContext() {
		return context;
	}

	public static TLContext<TLValue> createStringContext() {
		TLContext<TLValue> context = new TLContext<TLValue>();
		context.setContext(TLValue.create(TLValueType.STRING));
		return context;
	}

	public static TLContext<TLValue> createIntegerContext() {
		TLContext<TLValue> context = new TLContext<TLValue>();
		context.setContext(TLValue.create(TLValueType.INTEGER));
		return context;
	}

	public static TLContext<TLValue> createLongContext() {
		TLContext<TLValue> context = new TLContext<TLValue>();
		context.setContext(TLValue.create(TLValueType.LONG));
		return context;
	}

	public static TLContext<TLValue> createDoubleContext() {
		TLContext<TLValue> context = new TLContext<TLValue>();
		context.setContext(TLValue.create(TLValueType.NUMBER));
		return context;

	}

	public static TLContext<TLValue> createDateContext() {
		TLContext<TLValue> context = new TLContext<TLValue>();
		context.setContext(TLValue.create(TLValueType.DATE));
		return context;

	}

	public static TLContext<TLValue> createByteContext() {
		TLContext<TLValue> context = new TLContext<TLValue>();
		context.setContext(TLValue.create(TLValueType.BYTE));
		return context;

	}

	public static TLContext<TLValue> createNullContext() {
		TLContext<TLValue> context = new TLContext<TLValue>();
		context.setContext(null);
		return context;
	}

	public static TLContext<TLValue> createListContext() {
		TLContext<TLValue> context = new TLContext<TLValue>();
		context.setContext(TLValue.create(TLValueType.LIST));
		return context;
	}

	public static TLContext<TLValue> createEmptyContext() {
		TLContext<TLValue> context = new TLContext<TLValue>();
		context.setContext(null);
		return context;
	}

}
