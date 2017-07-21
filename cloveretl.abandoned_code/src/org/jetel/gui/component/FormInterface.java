/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Apr 4, 2003
 *  Copyright (C) 2003, 2002  David Pavlis, Wes Maciorowski
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
 */

package org.jetel.gui.component;

import org.jetel.gui.fileformatwizard.FileFormatDataModel;


public interface  FormInterface {
    
	/**
	 * Typically, data is retrieved from the frame's UI
	 * objects and tested for validity. If there are any
	 * problems, often <code>badDataAlert</code> is used
	 * to communicate it to the user.
	 * <p>
	 * If all of the data is valid, this should return null
	 * so that the caller can proceed (usually by storing
	 * the result somewhere and destroying the frame.)
	 * Naturally, error message should be returned if there is
	 * any invalid data.
	 * <p>
	 * @return <code>null</code> if the data in the dialog is acceptable,
	 * <code>String message</code> if the data fails to meet validation criteria.
	 *
	 */
	public String validateData();
    
	/**
	 * Normally, if the data is valid (see {@link #validateData validateData},)
	 * This is then called to store the data before the dialog is
	 * destroyed.
	 * <p>
	 */
	  public void saveData();


	/**
	 * Used to populate the form with data.
	 * <p>
	 */
	  public void loadData();
	  
	  
	/**
	 * Used to expose access to data model.
	 * <p>
	 */
	  public FileFormatDataModel getFileFormatDataModel();
}
