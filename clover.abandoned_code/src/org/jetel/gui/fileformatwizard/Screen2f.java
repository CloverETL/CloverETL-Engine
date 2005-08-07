/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2003,2002  David Pavlis, Wes Maciorowski
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

import javax.swing.JPanel;
import javax.swing.JScrollPane;

import java.awt.GridBagLayout;
import javax.swing.JLabel;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.awt.Font;
import javax.swing.JTextPane;

import org.jetel.gui.component.FormInterface;
import org.jetel.gui.component.RulerPanel;
import org.jetel.util.QSortAlgorithm;

import java.awt.SystemColor;

public class Screen2f extends JPanel implements  FormInterface
{
  private GridBagLayout gridBagLayout1 = new GridBagLayout();
  private JLabel jLabel1 = new JLabel();
  private JLabel jLabel2 = new JLabel();
  private RulerPanel aRulerPanel = null;
  private JPanel jPanel2 = new JPanel();
  private JLabel jLabel3 = new JLabel();
  private JTextPane jTextPane1 = new JTextPane();
	 
  private FileFormatDispatcher aDispatcher;
  private FileFormatDataModel aFileFormatDataModel;
  private short[] fieldWidths;	
  
  public Screen2f(FileFormatDispatcher aDispatcher, FileFormatDataModel aFileFormatDataModel)
  {
	this.aDispatcher = aDispatcher;
	this.aFileFormatDataModel = aFileFormatDataModel;
  	
	try
	{
		aRulerPanel = new RulerPanel(new Font("Courier New", 0, 11));
	  jbInit();
	}
	catch(Exception e)
	{
	  e.printStackTrace();
	}

  }
  public Screen2f()
  {
	try
	{
		aRulerPanel = new RulerPanel(new Font("Courier New", 0, 11));
	  jbInit();
	}
	catch(Exception e)
	{
	  e.printStackTrace();
	}

  }

  private void jbInit() throws Exception
  {
    this.setLayout(gridBagLayout1);
    jLabel1.setText("Screen 2 of 4");
    jLabel1.setFont(new Font("Dialog", 1, 11));
    jLabel2.setText("Specify Fields");
    jTextPane1.setText("Here goes screen help text.");
	jTextPane1.setEditable(false);
    jTextPane1.setBackground(SystemColor.control);
    
	//loadData();
	
    this.add(jLabel1, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.NORTHWEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jLabel2, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
	JScrollPane scrollPane = new JScrollPane(aRulerPanel,JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED,JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
	this.add(scrollPane, new GridBagConstraints(0, 3, 2, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
	this.setPreferredSize(new Dimension(300, 200));
    this.add(jPanel2, new GridBagConstraints(0, 4, 2, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    this.add(jLabel3, new GridBagConstraints(0, 2, 2, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jTextPane1, new GridBagConstraints(0, 1, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
  }


  /**
   *  Setter for linesFromFile
   *
   *@param  linesFromFile           A String arrary containing first few lines in the file.
   *@since                   		April 2, 2003
   */
	public void setLinesFromFile(String[] linesFromFile) {
		aRulerPanel.setLinesFromFile( linesFromFile);
		repaint();
	}
	
	/**
	 *  Getter for linesFromFile
	 *
	 *@return                  linesFromFile	A String arrary.
	 *@since                   April 2, 2003
	 */
	public String[] getLinesFromFile() {
		return aRulerPanel.getLinesFromFile();
	}
	
	/**
	 * Used to populate the form with data.
	 * <p>
	 */ 
	public void loadData() {
		   if(aFileFormatDataModel != null) {
				if(aFileFormatDataModel.fileName != null) {   	
				  jLabel3.setText(aFileFormatDataModel.fileName);
				}
				if(aFileFormatDataModel.linesFromFile != null) {
				  setLinesFromFile(aFileFormatDataModel.linesFromFile);
					
				}
				if(aFileFormatDataModel.oneRecordPerLine) {
					jTextPane1.setText("You can place marks by clicking your mouse on desired location. Click once more to remove erroneus marks.\n\nUse mouse to mark the end of each field.  You should not mark the last field as it is defined with the end of line.");
				} else {
					jTextPane1.setText("You can place marks by clicking your mouse on desired location. Click once more to remove erroneus marks.\n\nUse mouse to mark the end of each field.  The last mark will also denote the end of record.");
				}

		   } else {
			  jLabel3.setText("filename goes here...");
		   }
	}

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
	 *
	 * @see org.jetel.gui.component.PhasedPanelInterface#validateData()
	 */
	public String validateData() {
		int oldPos = 0;
		int newPos = 0;
		Object[] objects = aRulerPanel.getPointList();
		int[] tmpFieldPos = new int[objects.length];
		for(int i=0; i < objects.length ; i++ ) {
			tmpFieldPos[i] = ((Integer)objects[i]).intValue();
		}
		try {
			QSortAlgorithm.sort(tmpFieldPos);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(aFileFormatDataModel.oneRecordPerLine) {
			String[] tmp = getLinesFromFile();				
			short[] fieldWidths = new short[objects.length+1];
		
			for(int i=0; i < tmpFieldPos.length ; i++ ) {
				newPos = tmpFieldPos[i] -1;
				if(newPos>tmp[0].length()) {
					return "Mark at position "+ Integer.toString(newPos) + " goes beyond the end of line!";
				}
				fieldWidths[i]=(short)(newPos - oldPos );
				oldPos = newPos;
			}
			fieldWidths[fieldWidths.length-1]=(short)(tmp[0].length()-oldPos);
			if(fieldWidths[fieldWidths.length-1]>0) { 
				this.fieldWidths = fieldWidths;
			}
		} else { // if more than one record per line then each record should have its end marked
			short[] fieldWidths = new short[objects.length];
		
			for(int i=0; i < tmpFieldPos.length ; i++ ) {
				newPos = tmpFieldPos[i] -1;
				fieldWidths[i]=(short)(newPos - oldPos );
				oldPos = newPos;
			}
			this.fieldWidths = fieldWidths;
		}
			
		return null;
	}
	/**
	 * Normally, if the data is valid (see {@link #validateData validateData},)
	 * This is then called to store the data before the dialog is
	 * destroyed.
	 * <p>
	 *
	 * @see org.jetel.gui.component.PhasedPanelInterface#saveData()
	 */
	public void saveData() {
		aFileFormatDataModel.recordSizes = fieldWidths;
		aFileFormatDataModel.recordMeta.bulkLoadFieldSizes(fieldWidths);
	}

	/**
	 * Used to expose access to data model.
	 * @see org.jetel.gui.component.FormInterface#getFileFormatDataModel()
	 */
	public FileFormatDataModel getFileFormatDataModel() {
		return aFileFormatDataModel;
	}
}