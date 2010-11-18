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
package org.jetel.data.parser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jetel.util.string.StringUtils;

public class AhoCorasick {
	boolean failureFunctionDone = false;
	NodeTrie rootTrie;
	NodeTrie currentNode;
	int minPattern;
    int maxPattern;
    
	/**
	 * Constructor of Aho-Corasick algorithm.
	 * @param patterns searched patterns
	 */
	public AhoCorasick() {
		rootTrie = new NodeTrie(null, '\0');
		currentNode = rootTrie;
	}
	
	/**
	 * Constructor of Aho-Corasick algorithm.
	 * @param patterns searched patterns
	 */
	public AhoCorasick(String[] patterns) {
		rootTrie = new NodeTrie(null, '\0');
		currentNode = rootTrie;
		
		//construct trie
		for(int i = 0; i < patterns.length; i++) {
			addPattern(patterns[i], i);
		}
		
		//construct failure function
		compile();
	}

	/**
	 * Create failure function.
	 */
	public void compile() {
		NodeTrie qa, q, r;
		char c;
		
		failureFunctionDone = true;
		
		//level 0
		rootTrie.fail = rootTrie;
		
		//level 1
		List<NodeTrie> currentNodes = rootTrie.getChildren();
		for(NodeTrie nt : currentNodes) {
			nt.fail = rootTrie;
		}
		
		//other levels
		while(!(currentNodes = getNextLevelNode(currentNodes)).isEmpty()) {
			for(NodeTrie nt : currentNodes) {
				qa = nt;
				q = qa.parent;
				c = qa.transition;
				r = q.fail;
				while(r != rootTrie && r.children[c] == null) {
					r = r.fail;
				}
				qa.fail = r.children[c];
				if(qa.fail == null) qa.fail = rootTrie;
				qa.patterns.addAll(qa.fail.patterns);
			}
		}
        
        //create patternsFinal (bit array) from patterns set - for fast isPattern() method
        currentNodes = new ArrayList<NodeTrie>();
        currentNodes.add(rootTrie);
        do {
            for(NodeTrie nt : currentNodes) {
                qa = nt;
                qa.patternsFinal = new boolean[maxPattern - minPattern + 1];
                for(MyInt myInt : qa.patterns) {
                    qa.patternsFinal[myInt.value - minPattern] = true;
                }
            }
        } while(!(currentNodes = getNextLevelNode(currentNodes)).isEmpty());
        
	}
	
	/**
	 * Add next searched pattern.
	 * @param s pattern
	 * @param idx pattern identifier 
	 */
	public void addPattern(String s, int idx) {
		if(failureFunctionDone) {
			throw new IllegalArgumentException("AhoCorasick: failureFunction is already done.");
		}
		if(!StringUtils.isEmpty(s)) {
			NodeTrie iterator = rootTrie;
			for(int i = 0; i < s.length(); i++) {
				if(iterator.children[s.charAt(i)] == null) {
					iterator.children[s.charAt(i)] = new NodeTrie(iterator, s.charAt(i));
				}
				iterator = iterator.children[s.charAt(i)]; 
			}
			iterator.patterns.add(new MyInt(idx));
		}
		
        if(idx < minPattern) minPattern = idx;
        if(idx > maxPattern) maxPattern = idx;
	}

	/**
	 * Update state of Aho-Corasick algorithm.
	 * @param c incoming char of text
	 */
	public void update(char c) {
		while(currentNode != rootTrie && currentNode.children[c] == null) {
			currentNode = currentNode.fail;
		}
		currentNode = currentNode.children[c];
		if(currentNode == null) currentNode = rootTrie;
	}
	
	public boolean isPattern(int idx) {
		return currentNode.patternsFinal[idx - minPattern];
	}
	
	public int getMatchLength() {
		return currentNode.depth;
	}
	
    /**
     * Resets AhoCorasick automat.
     */
    public void reset() {
        currentNode = rootTrie;
    }
    
	/**
	 * Find first matching pattern in given string
	 * @param patterns Patterns to be found
	 * @param str String to be searched
	 * @return {position, patternIdx} for pattern patterns[patternIdx] at position charIdx,
	 * {-1, -1} otherwise
	 */
	public int[] firstMatch(String[] patterns, CharSequence seq) {
		reset();
		for (int charIdx = 0; charIdx < seq.length(); charIdx++) {
			update(seq.charAt(charIdx));
			for (int patternIdx = 0; patternIdx < patterns.length; patternIdx++) {
				if (isPattern(patternIdx)) {	// match found
					return new int[]{charIdx + 1 - patterns[patternIdx].length(), patternIdx};
				}
			}
		}
		// no match
		return new int[]{-1, -1};
	}

	public static List<NodeTrie> getNextLevelNode(List<NodeTrie> level) {
		List<NodeTrie> ch = new ArrayList<NodeTrie>();
		for (NodeTrie nt : level) {
			ch.addAll(nt.getChildren());
		}
		return ch;
	}
	
	/**
	 * Search trie node.
	 */
	private static class NodeTrie {
		int depth;
		NodeTrie parent;
		char transition;
		NodeTrie[] children;
		Set<MyInt> patterns;
        boolean[] patternsFinal;
		NodeTrie fail;
		
		/**
		 * Constructor.
		 * @param parent parent node
		 * @param transition transition char from parent
		 */
		public NodeTrie(NodeTrie parent, char transition) {
			children = new NodeTrie[Character.MAX_VALUE];
			patterns = new HashSet<MyInt>();
			this.parent = parent;
			this.transition = transition;
			if(this.parent == null) {
				depth = 0;
			} else {
				depth = parent.depth + 1;
			}
		}
		
		/**
		 * Returns all children of this node.
		 * @return all children of this node
		 */
		public List<NodeTrie> getChildren() {
			List<NodeTrie> ch = new ArrayList<NodeTrie>();
			for(int i = 0; i < children.length; i++) {
				if(children[i] != null) ch.add(children[i]);
			}
			return ch;
		}
	}
	
	/**
	 * My implementation of integer class.
	 */
	private static class MyInt {
		int value;
		
		/**
		 * Constructor.
		 */
		private MyInt() {
		}
		
		/**
		 * Constructor.
		 * @param value integer value to store
		 */
		private MyInt(int value) {
			this.value = value;
		}

		@Override
		public int hashCode() {
	    	return value;
	    }

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof MyInt)) {
				return false;
			}
			final MyInt other = (MyInt) obj;

			return other.value == value;
		}
	}
	
	/**
	 * Testing main method.
	 * @param args
	 */
	public static void main(String[] args) {
		String[] p = { "ab", "b" };
		String t = "ab";
		AhoCorasick ac = new AhoCorasick(p);
		
		for(int i = 0; i < t.length(); i++) {
			ac.update(t.charAt(i));
			if(ac.isPattern(0)) {
				System.out.println("0 " + i);
			}
			if(ac.isPattern(1)) {
				System.out.println("1 " + i);
			}
		}
	}
}
