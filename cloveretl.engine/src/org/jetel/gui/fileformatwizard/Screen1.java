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

import java.awt.GridBagLayout;
import javax.swing.JLabel;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import javax.swing.JButton;
import javax.swing.JTextPane;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.awt.Font;

import javax.swing.ButtonGroup;
import javax.swing.JRadioButton;
import javax.swing.JPanel;
import javax.swing.JFileChooser;
import javax.swing.JScrollPane;
import javax.swing.SwingConstants;

import org.jetel.gui.component.FormInterface;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.data.Defaults;

import java.awt.Dimension;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.Channels;

public class Screen1 extends JPanel implements FormInterface
{
	 private GridBagLayout gridBagLayout1 = new GridBagLayout();
	 private JLabel jLabel1 = new JLabel();
	 private JButton FileChooserButton = new JButton();
	 private JLabel jLabel2 = new JLabel();
	 private JLabel jLabel3 = new JLabel();
	 private JTextPane jTextPane1 = new JTextPane();
	 private JLabel jLabel4 = new JLabel();
	 private JRadioButton FixedWidthRadioButton = new JRadioButton();
	 private JRadioButton DelimitedRadioButton = new JRadioButton();
	 private JRadioButton OneRecordPerLineRadioButton = new JRadioButton();
	 private JRadioButton ManyRecordPerLineRadioButton = new JRadioButton();
	 private JPanel jPanel1 = new JPanel();
	 private JLabel jLabel5 = new JLabel();
	 private JLabel jLabel6 = new JLabel();
	 
	 private FileFormatDispatcher aDispatcher;
	 private FileFormatDataModel aFileFormatDataModel;
	 private String[] linesFromFile = new String[5];
	
  public Screen1(FileFormatDispatcher aDispatcher, FileFormatDataModel aFileFormatParameters)
  {
  	this.aDispatcher = aDispatcher;
  	this.aFileFormatDataModel = aFileFormatParameters;
  	
	    try
    {
      jbInit();
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }

  }
  
  public Screen1() {
		try
	{
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
    this.setSize(new Dimension(425, 303));
   
	//loadData();
   jLabel1.setText("Screen 1 of 4");
   jLabel1.setFont(new Font("Dialog", 1, 12));
   FileChooserButton.setText("Select File");
   FileChooserButton.addActionListener(new ActionListener()
	 {
	   public void actionPerformed(ActionEvent e)
	   {
		 FileChooserButton_actionPerformed(e);
	   }
	 });
   jLabel3.setText("Preview File (first 5 lines):");
   jLabel3.setFont(new Font("Dialog", 1, 11));
   jLabel4.setText("Choose the file type that best describes your data:");
   jLabel4.setFont(new Font("Dialog", 1, 11));
   FixedWidthRadioButton.setText("Fixed Width - Fields are aligned in columns with optional spaces " + "between them.");
   DelimitedRadioButton.setText("Delimited     - Characters such as tab, comma separate fields.");
   OneRecordPerLineRadioButton.setText("There is only one record per line.");
   ManyRecordPerLineRadioButton.setText("All records are on one line.");
   jLabel6.setText(" ");
   jTextPane1.setEditable(false);
   jLabel5.setText("Select File");
   jLabel5.setAlignmentY((float)0.0);
   jLabel5.setAlignmentX((float)0.5);
   jLabel5.setHorizontalTextPosition(SwingConstants.CENTER);
   jLabel5.setHorizontalAlignment(SwingConstants.CENTER);
   this.add(jLabel1, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.NORTHWEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
   this.add(FileChooserButton, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
   this.add(jLabel2, new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
   this.add(jLabel3, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 5, 0, 0), 0, 0));
   JScrollPane scrollPane = new JScrollPane(jTextPane1,JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED,JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
   this.add(scrollPane, new GridBagConstraints(0, 3, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 50));
   this.add(jLabel4, new GridBagConstraints(0, 4, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 5, 0, 0), 0, 0));
   this.add(FixedWidthRadioButton, new GridBagConstraints(0, 5, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 10, 0, 0), 0, 0));
   this.add(DelimitedRadioButton, new GridBagConstraints(0, 6, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.VERTICAL, new Insets(0, 10, 0, 0), 0, 0));
   this.add(jLabel6, new GridBagConstraints(0, 7, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 5, 0, 0), 0, 0));
   this.add(OneRecordPerLineRadioButton, new GridBagConstraints(0, 8, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 10, 0, 0), 0, 0));
   this.add(ManyRecordPerLineRadioButton, new GridBagConstraints(0, 9, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.VERTICAL, new Insets(0, 10, 0, 0), 0, 0));
   this.add(jPanel1, new GridBagConstraints(1, 10, 2, 1, 0.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
   this.add(jLabel5, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

   // Group the radio buttons.
   ButtonGroup group = new ButtonGroup();
   group.add(FixedWidthRadioButton);
   group.add(DelimitedRadioButton);

   ButtonGroup group2 = new ButtonGroup();
   group2.add(OneRecordPerLineRadioButton);
   group2.add(ManyRecordPerLineRadioButton);
   
  }
  
  public void setPreviewFileNameLabel(String previewFileNameLabel) {
	jLabel2.setText( previewFileNameLabel);
  }

  public String getPreviewFileNameLabel() {
	  return jLabel2.getText();
  }

private void FileChooserButton_actionPerformed(ActionEvent e)
{
  //Create a file chooser
  final JFileChooser fc = new JFileChooser();

  //In response to a button click:
  int returnVal = fc.showOpenDialog(this);
  if (returnVal == JFileChooser.APPROVE_OPTION) {
		setPreviewFileNameLabel(fc.getSelectedFile().getPath());
		File file = fc.getSelectedFile();
		try {
			BufferedReader reader = new BufferedReader(Channels.newReader( (new FileInputStream(file).getChannel()), Defaults.DataParser.DEFAULT_CHARSET_DECODER ) );
			try {
				int i = 0;
				String line = null;
				StringBuffer buf = new StringBuffer();
				line =reader.readLine();
				while(line != null) {
					buf.append( line ).append("\n" );	
					if(i< linesFromFile.length) {
						linesFromFile[i] = line;
						i++;
					}
					line = reader.readLine();
				}
				reader.close();
				jTextPane1.setText(buf.toString());
				aDispatcher.getAFileFormatWizard().enableNextButton();

			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	  }
}


/**
 * Used to populate the form with data.
 * <p>
 */ 
	public void loadData() {
		   if(aFileFormatDataModel != null) {
				if(aFileFormatDataModel.fileName != null) {   	
					jLabel2.setText(aFileFormatDataModel.fileName);
				} else {
				 	jLabel2.setText("(No File Selected)");
				}
				if(aFileFormatDataModel.linesFromFile != null) {
					StringBuffer buf = new StringBuffer();
					for(int i = 0 ; i < aFileFormatDataModel.linesFromFile.length; i++ ) {
						if( aFileFormatDataModel.linesFromFile[i] != null)
							buf.append(aFileFormatDataModel.linesFromFile[i] ).append("\n" );	
					}
					jTextPane1.setText(buf.toString());
				}
				if( aFileFormatDataModel.isFileDelimited ) {
					DelimitedRadioButton.setSelected(true);
				} else {
					FixedWidthRadioButton.setSelected(true);
				}
			if( aFileFormatDataModel.oneRecordPerLine ) {
				OneRecordPerLineRadioButton.setSelected(true);
			} else {
				ManyRecordPerLineRadioButton.setSelected(true);
			}
		   } else {
			jLabel2.setText("(No File Selected)");
			jTextPane1.setText("");
			OneRecordPerLineRadioButton.setSelected(true);
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
	 * @see org.jetel.gui.component.PhasedPanelInterface#validateData()
	 */
	public String validateData() {
		// at minimum we need to have file selected
		if(jLabel2.getText().equalsIgnoreCase("(No File Selected)")){
			return "No File Selected!";
		}
		if( ManyRecordPerLineRadioButton.isSelected() && DelimitedRadioButton.isSelected() ) {
			return "This option is not implemented yet.";
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
		aFileFormatDataModel.fileName = jLabel2.getText();
		//aFileFormatDataModel.recordMeta.setName(jLabel2.getText());
		if(DelimitedRadioButton.isSelected()){
			aFileFormatDataModel.recordMeta.setRecType(DataRecordMetadata.DELIMITED_RECORD);
		} else {
			aFileFormatDataModel.recordMeta.setRecType(DataRecordMetadata.FIXEDLEN_RECORD);
		}
		
		aFileFormatDataModel.isFileDelimited = DelimitedRadioButton.isSelected();
		aFileFormatDataModel.linesFromFile = linesFromFile;
		aFileFormatDataModel.oneRecordPerLine=OneRecordPerLineRadioButton.isSelected();
	}

	/**
	 * Used to expose access to data model.
	 * @see org.jetel.gui.component.FormInterface#getFileFormatDataModel()
	 */
	public FileFormatDataModel getFileFormatDataModel() {
		return aFileFormatDataModel;
	}

}