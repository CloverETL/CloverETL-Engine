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

package org.jetel.gui.fileformatwizard;

import org.jetel.gui.component.PhaseFrame;

/**
 * @author maciorowski
 *
 */
public class FileFormatWizard extends PhaseFrame{

	/**
	 * @param inTitle
	 * @param inDispatcher
	 */
	public FileFormatWizard() {
		super("File Format Wizard");
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		FileFormatWizard aFileFormatWizard = new FileFormatWizard();
		// create the phased process control object and launch it
		FileFormatDispatcher aFileFormatDispatcher = new FileFormatDispatcher(aFileFormatWizard);
		aFileFormatWizard.setDispatcher(aFileFormatDispatcher);
		aFileFormatDispatcher.beginProcess();
		aFileFormatWizard.pack();
		aFileFormatWizard.show();
	}
}
