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
package org.jetel.hadoop.service.mapreduce;

/**
 * Represents a function that returns counter display name for given {@link Enum} that identifies Hadoop map/reduce counter.
 * The display name this operation provides is meant to be displayed to users as label for counter key.
 *    
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 */
public interface HadoopCounterDisplayNameGetter<T extends Enum<T>> {
	/**
	 * Provides display name for given Hadoop map/reduce counter key.
	 * @param key Hadoop API specific key for map/reduce counter.
	 * @return Display name for given key. The display name is intended to be displayed to users.
	 */
	String getDisplayNameForKey(T key);
}
