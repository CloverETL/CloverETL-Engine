/*
 * Created on Jun 2, 2003
 *
 *
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 *  Created on May 31, 2003
 */
package org.jetel.util;

import java.util.HashMap;

/**
 * The purpose for this class is to handle parsing java code enhanced with
 * cloverETL syntax.  Initially cloverETL syntax will support references
 * to field values. The future enhancement will be to add support for aggregate
 * functions (similar to SQL agregation).
 * 
 * @author Wes Maciorowski
 * @version 1.0
 *
 */
public class CodeParser {

	/**
	 * @param inputFieldRefs
	 * @param recordFieldRefs
	 */
	public CodeParser(HashMap inputFieldRefs, HashMap recordFieldRefs) {
		
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	public void parse() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * @return
	 */
	public int[][] getInputRecordFieldDependencies() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @return
	 */
	public int[] getIntraRecordFieldDependencies() {
		// TODO Auto-generated method stub
		return null;
	}

}
