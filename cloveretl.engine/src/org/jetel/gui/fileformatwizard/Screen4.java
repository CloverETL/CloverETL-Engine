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
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.awt.SystemColor;

import javax.swing.JTextPane;

import org.jetel.gui.component.FormInterface;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;

import java.awt.Font;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class Screen4 extends JPanel implements  FormInterface
{
  private GridBagLayout gridBagLayout1 = new GridBagLayout();
  private JLabel jLabel1 = new JLabel();
  private JTextPane jTextPane2 = new JTextPane();
  private JTextPane jTextPane1 = new JTextPane();
  private JLabel jLabel2 = new JLabel();
	 
  private FileFormatDispatcher aDispatcher;
  private FileFormatDataModel aFileFormatDataModel;
  private DataRecordMetadata aDataRecordMetadata;
	
  
  public Screen4(FileFormatDispatcher aDispatcher, FileFormatDataModel aFileFormatParameters)
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

  private void jbInit() throws Exception
  {
    this.setLayout(gridBagLayout1);
    jLabel1.setText("Screen 4 of 4");
    jLabel1.setFont(new Font("Dialog", 1, 11));
	jTextPane1.setText("You can edit record information and any changes you make will be imported back into the wizard.\n\nPlease note that there is no 'Undo' button.");
	jTextPane1.setBackground(SystemColor.control);
	jTextPane1.setEditable(false);
    jLabel2.setText(" Preview Final XML");
	JScrollPane scrollPane = new JScrollPane(jTextPane2);
    this.add(jLabel1, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
	this.add(jTextPane1, new GridBagConstraints(0, 1, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    this.add(scrollPane, new GridBagConstraints(0, 2, 2, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jLabel2, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
	//loadData();
  }

	/* (non-Javadoc)
	 * @see org.jetel.gui.component.PhasedPanelInterface#validateData()
	 */
	public String validateData() {
		//parse the document
		DataRecordMetadataXMLReaderWriter aReader = new DataRecordMetadataXMLReaderWriter();
		aDataRecordMetadata = aReader.read(new ByteArrayInputStream(jTextPane2.getText().getBytes()));
		if( aDataRecordMetadata == null) {
			return "Errors Parsing Record elements!";
		}
		return null;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.gui.component.PhasedPanelInterface#saveData()
	 */
	public void saveData() {
		aFileFormatDataModel.recordMeta = aDataRecordMetadata;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.gui.component.FormInterface#loadData()
	 */
	public void loadData() {
		aDataRecordMetadata= aFileFormatDataModel.recordMeta;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataRecordMetadataXMLReaderWriter aWriter = new DataRecordMetadataXMLReaderWriter();
		aWriter.write(aDataRecordMetadata,baos);
		jTextPane2.setText(baos.toString());
	}
	
	/**
	 * Used to expose access to data model.
	 * @see org.jetel.gui.component.FormInterface#getFileFormatDataModel()
	 */
	public FileFormatDataModel getFileFormatDataModel() {
		return aFileFormatDataModel;
	}
}