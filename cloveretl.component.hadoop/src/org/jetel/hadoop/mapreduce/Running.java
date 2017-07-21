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
package org.jetel.hadoop.mapreduce;

import org.jetel.graph.Node;

/**
 * Represents an object that can be run by user. The user user might decide to end running process. Example such objects
 * are all instances of classes inheriting from {@link Node}. Classes inheriting from {@link Node} can implement this
 * interface just by specifying just by explicitly specifying implementation in class header (they already have required
 * method).
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 * @see HadoopJobRunner#HadoopJobRunner(Running, Logger, int)
 * @see Node
 */
public interface Running {
	
	/**
	 * Returns {@code true} should the running process continue. Typically, this method indicates that process should
	 * continue running until the user decides to abort it.
	 * @return {@code true} if the running process should continue, {@code false} otherwise.
	 */
	boolean runIt();
}
