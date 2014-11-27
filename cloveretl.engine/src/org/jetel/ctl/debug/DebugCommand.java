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
package org.jetel.ctl.debug;



public class DebugCommand {

		protected CommandType type;
		protected Object value;

		public DebugCommand(CommandType type) {
			this.type = type;
			value = null;
		}

		public CommandType getType() {
			return type;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public Object getValue() {
			return value;
		}
		
		@Override
		public String toString(){
			return type.toString();
		}


	public enum CommandType {
		RESUME, 
		SUSPEND, 
		STEP_OVER, 
		STEP_IN, 
		STEP_OUT, 
		LIST_VARS, 
		GET_VAR, 
		SET_VAR, 
		LIST_BREAKPOINTS, 
		SET_BREAKPOINTS, 
		SET_BREAKPOINT, 
		REMOVE_BREAKPOINT, 
		GET_CALLSTACK, 
		INFO, 
		GET_AST;
	}

}