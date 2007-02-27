package org.jetel.util;

import java.util.Iterator;
import java.util.regex.Pattern;


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
	
	private final static int FIRST_ELEMENT = 0;

	private String pattern;
	private String subPattern;
	private int index = 0;
	private int comaIndex;
	private int next = FIRST_ELEMENT;
	private PositiveIntervalIterator intervalIterator = null;
	
	/**
	 * Constructor from given mask
	 * 
	 * @param pattern
	 */
	public NumberIterator(String pattern){
		this.pattern = pattern.trim();
		if (pattern.equals("*")) {
			subPattern = pattern;
		}
	}
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	public boolean hasNext() {
		if (pattern.equals("*")) {
			return true;
		}		
		//check if in current interval there is more numbers
		if (intervalIterator != null && intervalIterator.hasNext() ) {
			return true;
		}
		//get next part of pattern
		if (index == pattern.length()) {//end of mask
			return false;
		}
		comaIndex = pattern.indexOf(',', index);
		if (comaIndex == -1) {
			subPattern = pattern.substring(index).trim();
			index = pattern.length();
		}else{
			subPattern = pattern.substring(index,comaIndex).trim();
			index = comaIndex + 1;
		}
		if (StringUtils.isInteger(subPattern)) {
			intervalIterator = null;
			return true;
		}else {
			intervalIterator = new PositiveIntervalIterator(subPattern);
			return intervalIterator.hasNext();
		}
	}
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	public Integer next() {
		if (intervalIterator != null) {//next from current interval
			return intervalIterator.next();
		}else{//subPattern is number or "*"
			if (StringUtils.isInteger(subPattern)) {
				return Integer.parseInt(subPattern);
			}else{
				return next++;
			}
		}
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
	private class PositiveIntervalIterator implements Iterator<Integer>{
		
		private final static int FIRST_ELEMENT = 0;
		
		private String firstPattern;
		private String lastPattern;
		private int next = FIRST_ELEMENT;
		private int last = FIRST_ELEMENT - 1;
		
		/**
		 * Constructor from given pattern
		 * 
		 * @param pattern
		 */
		PositiveIntervalIterator(String pattern) {
			if (!Pattern.matches("[0-9]*-[0-9]*|[0-9]*-\\*|\\*-[0-9]*", pattern)){
				throw new IllegalArgumentException("Not positive integer interval: " + pattern);
			}
			firstPattern = pattern.substring(0,pattern.indexOf('-')).trim();
			lastPattern = pattern.substring(pattern.indexOf('-') + 1).trim();
			if (!firstPattern.equals("*")) {
				next = Integer.parseInt(firstPattern);
			}
			if (!lastPattern.equals("*")){
				last = Integer.parseInt(lastPattern);
			}
		}
		
		public boolean hasNext() {
			return (last == FIRST_ELEMENT -1 || next <= last);
		}

		public Integer next() {
			return hasNext() ? next++ : null;
		}
		
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
}