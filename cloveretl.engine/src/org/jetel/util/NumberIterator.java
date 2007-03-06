package org.jetel.util;

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Class for resolving integer number from given mask.<br>
 * Mask can be in form: 
 * <ul><li>*</li>
 * <li>number</li>
 * <li>minNumber-maxNumber</li>
 * <li>*-maxNumber</li>
 * <li>minNumber-*</li>
 * or as their combination separated by comma, eg. 1,3,5-7,9-*
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Feb 23, 2007
 *
 */
public class NumberIterator implements Iterator<Integer>{
	
	private String pattern;
	private String subPattern;
	private int index = 0;
	private int comaIndex;
	private int last;
	private int first;
	private IntervalIterator intervalIterator = null;
	private Integer next = null;
	private Integer tmp;
	
	/**
	 * Constructor from given mask
	 * 
	 * @param pattern
	 */
	public NumberIterator(String pattern, int first,int last){
		this.first = first;
		this.last = last;
		this.pattern = pattern.trim();
		if (pattern.equals("*")) {
			subPattern = pattern;
		}
		next = first - 1;
		next = getNext();
	}
	
	public NumberIterator(String pattern){
		this(pattern, IntervalIterator.FIRST_ELEMENT, IntervalIterator.LAST_ELEMENT);
	}
	
	private Integer getNext(){
		if (pattern.equals("*")) {
			next++;
			return next <= last ? next : null;
		}		
		//check if in current interval there is more numbers
		if (intervalIterator != null && intervalIterator.hasNext() ) {
			return intervalIterator.next();
		}
		//get next part of pattern
		if (index == pattern.length()) {//end of mask
			return null;
		}
		comaIndex = pattern.indexOf(',', index);
		if (comaIndex == -1) {
			subPattern = pattern.substring(index).trim();
			index = pattern.length();
		}else{
			subPattern = pattern.substring(index,comaIndex).trim();
			index = comaIndex + 1;
		}
		if (StringUtils.isInteger(subPattern) == 0 || 
				StringUtils.isInteger(subPattern) == 1) {
			intervalIterator = null;
			return Integer.parseInt(subPattern);
		}else {
			intervalIterator = new IntervalIterator(subPattern,first,last);
			if (intervalIterator.hasNext()) {
				return intervalIterator.next();
			}else{
				return getNext();
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	public boolean hasNext() {
		return next != null;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	public Integer next() {
		tmp = next;
		if (next == null) {
			throw new NoSuchElementException();
		}
		next = getNext();
		return tmp;
	}
	
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Class for resolving integer number from given mask. Mask has to be in form: 
	 * minNuber-maxNumber, when minNumber, maxNumber are integers or "*"
	 * 
	 * 
	 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
	 * (c) JavlinConsulting s.r.o.
	 *  www.javlinconsulting.cz
	 *
	 * @since Feb 23, 2007
	 *
	 */
	private class IntervalIterator implements Iterator<Integer>{
		
		public final static int FIRST_ELEMENT = Integer.MIN_VALUE;
		public final static int LAST_ELEMENT = Integer.MAX_VALUE;
		
		private final static char DASH = '-';
		
		private String firstPattern;
		private String lastPattern;
		private int next;
		private Integer last;
		
		/**
		 * Constructor from given pattern
		 * 
		 * @param pattern
		 */
		IntervalIterator(String pattern) {
			this(pattern,FIRST_ELEMENT,LAST_ELEMENT);
		}
		
		IntervalIterator(String pattern, int first, int last) {
			next = first;
			this.last = last;
			int dashIndex= pattern.trim().indexOf(DASH);
			if (dashIndex == -1) {
				throw new IllegalArgumentException("Not integer interval: " + pattern);
			}else if (dashIndex == 0) {
				dashIndex = pattern.indexOf(DASH, 1);
				if (dashIndex == -1) {
					throw new IllegalArgumentException("Not integer interval: " + pattern);
				}
			}
			firstPattern = pattern.substring(0,dashIndex).trim();
			lastPattern = pattern.substring(dashIndex + 1).trim();
			if (!firstPattern.equals("*")) {
				try {
					next = Integer.parseInt(firstPattern);
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Not integer interval: " + pattern);
				}
			}
			if (!lastPattern.equals("*")){
				try {
					this.last = Integer.parseInt(lastPattern);
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Not integer interval: " + pattern);
				}
			}
		}
		
		public boolean hasNext() {
			return (next <= last);
		}

		public Integer next() {
			return hasNext() ? next++ : null;
		}
		
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
}