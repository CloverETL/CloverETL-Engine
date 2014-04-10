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
package org.jetel.util.string;

import java.text.RuleBasedCollator;
import java.util.List;

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
		final int aLength = a.length();
		final int bLength = b.length();
		final int compLength = (aLength< bLength  ? aLength : bLength );
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
	
    final static public int compare(CharSequence a,CharSequence b,RuleBasedCollator col) {

    	return col.compare(a.toString(), b.toString());

/*
 * pnajvar -
 * The following implementation is deprecated. 
 * There is no clear historical reason for such implementation.
 * 
 * Tests show using Collator.compare() is more efficient even on StringBuilders and works correctly
 * on characters like 't' and 't wedge', 'l' and 'l acute' etc.	
 */

/*    	
        CollationElementIterator iterA = col.getCollationElementIterator(
                        new CharSequenceCharacterIterator(a)); 
        CollationElementIterator iterB = col.getCollationElementIterator(
                new CharSequenceCharacterIterator(b)); 

        int elementA,elementB;
        int orderA,orderB;
        
        while ((elementA = iterA.next()) != CollationElementIterator.NULLORDER) {
            elementB=iterB.next();
            if (elementB != CollationElementIterator.NULLORDER){
                // check primary order
                orderA=CollationElementIterator.primaryOrder(elementA);
                orderB=CollationElementIterator.primaryOrder(elementB);
                if (orderA!=orderB){
                    return orderA-orderB;
                }
                orderA=CollationElementIterator.secondaryOrder(elementA);
                orderB=CollationElementIterator.secondaryOrder(elementB);
                if (orderA!=orderB){
                    return orderA-orderB;
                }
                orderA=CollationElementIterator.tertiaryOrder(elementA);
                orderB=CollationElementIterator.tertiaryOrder(elementB);
                if (orderA!=orderB){
                    return orderA-orderB;
                }
                
            }else{
                return 1; // first sequence seems to be longer than second
            }
        }
        elementB = iterB.next();
        if (elementB != CollationElementIterator.NULLORDER){
            return -1; // second sequence seems to be longer than first
        }else{
            return 0; // equal
        }
--
end of deprecated code */    
        
    }
    
	/**
	 * Compares two CharSequences for equality
	 * 
	 * @param a
	 * @param b
	 * @return true if equal, false otherwise
	 */
	public static boolean equals(CharSequence a, CharSequence b){
    	final int length=a.length();
    	if (length!=b.length()) return false;
    	for (int i = 0; i < length; i++) {
			if (a.charAt(i) != b.charAt(i))
				return false;
		}
    	return true;
    }

	/**
	 * Compares the given {@link CharSequence} with a list of {@link CharSequence}.
	 * CharSequence <code>a</code> is equal to the list if at least one of the
	 * CharSequences in the list is equal to Charsequence <code>a</code>.
	 * @param a
	 * @param b
	 * @return
	 */
	public static boolean equals(CharSequence a, List<? extends CharSequence> b) {
		for (CharSequence pattern : b) {
			if (equals(a, pattern)) {
				return true;
			}
		}
		return false;
    }

	/**
	 * Compares two CharSequences for equality ignoring letters' case
	 * 
	 * @param a
	 * @param b
	 * @return true if equal, false otherwise
	 */
	public static boolean equalsIgnoreCase(CharSequence a, CharSequence b){
    	final int length=a.length();
    	if (length!=b.length()) return false;
    	for (int i = 0; i < length; i++) {
			if (Character.toUpperCase(a.charAt(i)) != Character.toUpperCase(b.charAt(i)))
				return false;
		}
    	return true;
    }
    
}
