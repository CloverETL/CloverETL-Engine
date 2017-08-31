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
package org.jetel.component;

import org.apache.log4j.Logger;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.3.2017
 */
public class HelloWorldComponent extends Node {

	private static final Logger log = Logger.getLogger(HelloWorldComponent.class);
	
	private static final String TYPE = "HELLO_WORLD";
	
	private String greeting = "Hello world!";
	private String type;
	
	public HelloWorldComponent(String id) {
		super(id);
	}
	
	@Override
	public String getType() {
		return type != null ? type : TYPE;
	}
	
	@Override
	protected Result execute() throws Exception {
		log.info(getId() + ": " + greeting);
		
		for (InputPort ip : getInPorts()) {
			DataRecord rec = DataRecordFactory.newRecord(ip.getMetadata());
			while (rec != null) {
				rec = ip.readRecord(rec);
			}
		}
		
		broadcastEOF();
		return Result.FINISHED_OK;
	}

	public String getGreeting() {
		return greeting;
	}

	public void setGreeting(String greeting) {
		this.greeting = greeting;
	}
	
	public void setType(String type) {
		this.type = type;
	}
}
