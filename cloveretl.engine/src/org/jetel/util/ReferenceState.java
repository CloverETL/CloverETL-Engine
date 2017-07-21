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
 * State of the reference at the graph.
 * 
 * @author Martin Slama (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created September 26th, 2014
 */
public enum ReferenceState {
	
	/**
	 * Valid relative reference.
	 */
	VALID_REFERENCE {
		
		@Override
		public String toString() {
			return "valid";
		}
	},
	/**
	 * Invalid relative reference. Such a reference is invalid forever.
	 */
	INVALID_REFERENCE {
		
		@Override
		public String toString() {
			return "invalid";
		}
	},
	/**
	 * Weak invalid relative reference. Such a reference can become valid.
	 */
	WEAK_INVALID_REFERENCE {
		
		@Override
		public String toString() {
			return "weakInvalid";
		}
	};
	
	/**
	 * @param state
	 * 
	 * @return True if given state is invalid, false otherwise.
	 */
	public static boolean isInvalidState(ReferenceState state) {
		return state != null && 
				(state == ReferenceState.INVALID_REFERENCE || state == ReferenceState.WEAK_INVALID_REFERENCE);
	}
	
	/**
	 * @param state
	 * @return true if the given state is valid, false otherwise
	 */
	public static boolean isValidState(ReferenceState state) {
		 return state != null && state  == ReferenceState.VALID_REFERENCE;
	}

	/**
	 * @param value
	 * 
	 * @return {@link ReferenceState} for given string representation.
	 */
	public static ReferenceState fromString(String value) {
		
		if (value != null) {
			for (ReferenceState refState: ReferenceState.values()) {
				if (value.equals(refState.toString())) {
					return refState;
				}
			}
		}
		throw new IllegalArgumentException("No state with text " + value);
	}
}