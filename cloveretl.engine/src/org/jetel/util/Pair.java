/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
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
