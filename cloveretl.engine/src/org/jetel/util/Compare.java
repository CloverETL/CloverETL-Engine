/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-05  David Pavlis <david_pavlis@hotmail.com> and others.
 *    
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *    
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
 *    Lesser General Public License for more details.
 *    
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Created on 18.11.2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.util;

/**
 * Miscelaneous comparison utilities
 * 
 * @author David Pavlis
 * @since  18.11.2005
 *
 */
public class Compare {

	
	/**
     * Compares two sequences lexicographically
     * 
	 * @param a
	 * @param b
	 * @return     -1;0;1 if (a>b); (a==b); (a<b)
	 */
	final static public int compare(CharSequence a,CharSequence b){
		int aLength = a.length();
		int bLength = b.length();
		int compLength = (aLength< bLength  ? aLength : bLength );
		for (int i = 0; i < compLength; i++) {
			if (a.charAt(i) > b.charAt(i)) {
				return 1;
			} else if (a.charAt(i) < b.charAt(i)) {
				return -1;
			}
		}
		// strings seem to be the same (so far), decide according to the length
		if (aLength == bLength) {
			return 0;
		} else if (aLength > bLength) {
			return 1;
		} else {
			return -1;
		}
	}
	
}
