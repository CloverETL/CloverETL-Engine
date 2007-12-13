package org.jetel.data;

public class SortOrder {

	boolean[] sortOrder;
	
	public SortOrder(int count) {
		sortOrder = new boolean[count];
	}
	
	public SortOrder(String input) {
		fromString(input);
	}
	
	public SortOrder(boolean[] input) {
		sortOrder = input;
	}
	
	public boolean getOrder(int index) {
		return sortOrder[index];
	}
	
	public void setOrder(int index, boolean value) {
		sortOrder[index] = value;
	}
	
	public String toString() {
		String result = "";
		for (int i = 0; i < sortOrder.length; i++) {
			result += sortOrder[i] ? "a" : "d"; 
			if (i < sortOrder.length - 1)
				result += "|";
		}
		return result;
	}
	
	public void fromString(String input) {
		
		String[] tmp = input.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX); 
		sortOrder = new boolean[tmp.length];
		
		for (int i = 0 ; i < tmp.length ; i++) {
			sortOrder[i] = tmp[i].matches("^[Aa].*"); 
		}
	}
	
	public void fromString(String input, int minLength) {
		
		String[] tmp = input.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX); 
		sortOrder = new boolean[Math.max(tmp.length, minLength)];
		boolean lastValue = true;
		
		for (int i = 0 ; i < tmp.length ; i++) {
			lastValue = sortOrder[i] = tmp[i].matches("^[Aa].*"); 
		}
		
		for (int i = tmp.length; i < minLength; i++) {
			sortOrder[i] = lastValue;
		}
	}

}
