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
 * @author david
 * @since  18.11.2005
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class Compare {

	
	final static public int compare(CharSequence a,CharSequence b){
		int aLenght = a.length();
		int bLenght = b.length();
		int compLength = (aLenght< bLenght  ? aLenght : bLenght );
		for (int i = 0; i < compLength; i++) {
			if (a.charAt(i) > b.charAt(i)) {
				return 1;
			} else if (a.charAt(i) < b.charAt(i)) {
				return -1;
			}
		}
		// strings seem to be the same (so far), decide according to the length
		if (aLenght == bLenght) {
			return 0;
		} else if (aLenght > bLenght) {
			return 1;
		} else {
			return -1;
		}
	}
	
	final static public int compare(double a,double b){
		if (a>b) return 1; else if (b>a) return -1; else return 0;
	}
	
	final static public int compare(int a,int b){
		if (a>b) return 1; else if (b>a) return -1; else return 0;
	}
	
	final static public int compare(long a,long b){
		if (a>b) return 1; else if (b>a) return -1; else return 0;
	}
	
	final static public int compare(Number a,Number b){
	    if (a instanceof Integer){
	        return compare(a.intValue() ,b.intValue());
	    }else if (a instanceof Long){
	        return compare(a.longValue() ,b.longValue());
	    }else {
	        return compare(a.doubleValue() ,b.doubleValue());
	    }
	}
    
}
