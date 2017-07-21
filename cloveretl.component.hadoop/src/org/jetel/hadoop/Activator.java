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
package org.jetel.hadoop;

import org.jetel.component.fileoperation.FileManager;
import org.jetel.component.fileoperation.hadoop.HadoopOperationHandler;
import org.jetel.plugin.PluginActivator;

public class Activator extends PluginActivator {

	@Override
	public void activate() {
		FileManager.getInstance().registerHandler(new HadoopOperationHandler());
	}

	@Override
	public void deactivate() {

	}

}
