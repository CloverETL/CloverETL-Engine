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

/**
 * Class is generic structure for holding two any type instances and offers type-safe getters/setters. 
 * @author "Jan Kucera" (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created Apr 15, 2011
 */
public class Pair<T,U> {
	private T first;
	private U second;
	
	public Pair() {
	}

	public Pair(T first, U second) {
		this.first = first;
		this.second = second;
	}
	
	public T getFirst() {
		return first;
	}

	public U getSecond() {
		return second;
	}

	public void setSecond(U second) {
		this.second = second;
	}

	public void setFirst(T first) {
		this.first = first;
	}
	
	public String toString() {
		String firstStr = this.first != null ? this.first.toString() : "null";
		String secondStr = this.second != null ? this.second.toString() : "null";
		return new StringBuilder()
				.append("(").append(firstStr).append(", ")
				.append(secondStr).append(")").toString();
	}
}
