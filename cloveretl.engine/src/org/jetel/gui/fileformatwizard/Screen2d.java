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
import java.awt.GridBagLayout;
import javax.swing.JLabel;

import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.awt.SystemColor;

import javax.swing.JTextPane;
import java.awt.Font;

import javax.swing.JCheckBox;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.BorderFactory;
import javax.swing.border.EtchedBorder;
import javax.swing.table.DefaultTableModel;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.Dimension;
import java.util.StringTokenizer;

import javax.swing.JFrame;
import javax.swing.JComboBox;
import javax.swing.SwingConstants;

import org.jetel.gui.component.FormInterface;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

public class Screen2d extends JPanel implements  FormInterface
{
  protected boolean FirstLineFieldNames = false;
  
  private GridBagLayout gridBagLayout1 = new GridBagLayout();
  private JLabel jLabel1 = new JLabel();
  private JLabel jLabel2 = new JLabel();
  private JTextPane jTextPane1 = new JTextPane();
  private JPanel jPanel1 = new JPanel();
  //private JPanel DataPreviewPanel = new JPanel();
  private JPanel FillerPanel = new JPanel();
  private GridBagLayout gridBagLayout2 = new GridBagLayout();
  private JCheckBox TabCheckBox = new JCheckBox();
  private JCheckBox SpaceCheckBox = new JCheckBox();
  private JCheckBox SemicolonCheckBox = new JCheckBox();
  private JCheckBox CommaCheckBox = new JCheckBox();
  private JCheckBox OtherCheckBox = new JCheckBox();
  private JTextField OtherTextField = new JTextField();
  private JPanel jPanel2 = new JPanel();
  private JCheckBox FirstLineFieldNamesCheckBox = new JCheckBox();
  private GridBagLayout gridBagLayout3 = new GridBagLayout();
  private JLabel jLabel3 = new JLabel();
  private JComboBox TextQualifierComboBox = new JComboBox();
  private JTable jTable1 = null;
  
  private DataRecordMetadata aDataRecordMetadata;
  private String[] columnNames = null;	
  private String delimiters = null;
  private FileFormatDispatcher aDispatcher;
  private FileFormatDataModel aFileFormatDataModel;
	
  
  public Screen2d(FileFormatDispatcher aDispatcher, FileFormatDataModel aFileFormatDataModel)
  {
	this.aDispatcher = aDispatcher;
	this.aFileFormatDataModel = aFileFormatDataModel;
	
    try
    {
      jbInit();
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }

  }
  public Screen2d()
  {
	try
	{
	  jbInit();
	}
	catch(Exception e)
	{
	  e.printStackTrace();
	}

  }

  public static void main(String[] args)
  {
    Screen2d screen2d = new Screen2d();
        JFrame f = new JFrame("RulerPanel");
        f.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {System.exit(0);}
        });
        f.getContentPane().add("Center", screen2d);
        f.setSize(new Dimension(400,20));
        f.pack();
        f.show();
  }

  private void jbInit() throws Exception
  {
    this.setLayout(gridBagLayout1);
    this.setSize(new Dimension(441, 300));
    jLabel1.setText("Screen 2 of 4");
    jLabel1.setFont(new Font("Dialog", 1, 11));
    jLabel2.setText("Specify Field Delimiters");
    jLabel2.setHorizontalTextPosition(SwingConstants.CENTER);
    jLabel2.setHorizontalAlignment(SwingConstants.CENTER);
	jTextPane1.setText("You can choose one or more of predefined delimiters or check 'Others' and enter your own.  Below you can also preview how your file will parse with preselected delimiters.");
	jTextPane1.setBackground(SystemColor.control);
	jTextPane1.setEditable(false);
    jPanel1.setLayout(gridBagLayout2);
    jPanel1.setBorder(BorderFactory.createEtchedBorder(EtchedBorder.LOWERED));
    TabCheckBox.setText("Tab");
	TabCheckBox.addActionListener(new ActionListener()
	  {
		public void actionPerformed(ActionEvent e)
		{
			delimiters_actionPerformed();
		}
	  });

    SpaceCheckBox.setText("Space");
	SpaceCheckBox.addActionListener(new ActionListener()
	  {
		public void actionPerformed(ActionEvent e)
		{
			delimiters_actionPerformed();
		}
	  });

    SemicolonCheckBox.setText("Semicolon");
	SemicolonCheckBox.addActionListener(new ActionListener()
	  {
		public void actionPerformed(ActionEvent e)
		{
			delimiters_actionPerformed();
		}
	  });

    CommaCheckBox.setText("Comma");
	CommaCheckBox.addActionListener(new ActionListener()
	  {
		public void actionPerformed(ActionEvent e)
		{
			delimiters_actionPerformed();
		}
	  });

    OtherCheckBox.setText("Other");
	OtherCheckBox.addActionListener(new ActionListener()
	  {
		public void actionPerformed(ActionEvent e)
		{
			delimiters_actionPerformed();
		}
	  });

	OtherTextField.addFocusListener(new java.awt.event.FocusAdapter()
	  {
		public void focusLost(FocusEvent e)
		{
			delimiters_actionPerformed();
		}
	  });

    jPanel2.setLayout(gridBagLayout3);
	FirstLineFieldNamesCheckBox.setText("Field names are in the first line.");
	FirstLineFieldNamesCheckBox.addActionListener(new ActionListener()
	  {
		public void actionPerformed(ActionEvent e)
		{
			if(FirstLineFieldNamesCheckBox.isSelected()) {
				FirstLineFieldNames = true;				
			} else {
				FirstLineFieldNames = false;				
			}
			delimiters_actionPerformed();
		}
	  });

	
	
	TextQualifierComboBox.addItem("'");
	TextQualifierComboBox.addItem("\"");
	TextQualifierComboBox.addItem("{None}");
    jLabel3.setText("Text Delimited by");
    
	jTable1 = new JTable();
	jTable1.setEnabled(false);
//	Create the scroll pane and add the table to it. 
	  JScrollPane scrollPane = new JScrollPane(jTable1);
//	jTable1.setColumnSelectionAllowed(true);
//	jTable1.setCellSelectionEnabled(false);


    this.add(jLabel1, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jLabel2, new GridBagConstraints(1, 0, 2, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jTextPane1, new GridBagConstraints(0, 1, 3, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    jPanel1.add(TabCheckBox, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    jPanel1.add(SpaceCheckBox, new GridBagConstraints(2, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    jPanel1.add(SemicolonCheckBox, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    jPanel1.add(CommaCheckBox, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    jPanel1.add(OtherCheckBox, new GridBagConstraints(1, 1, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    jPanel1.add(OtherTextField, new GridBagConstraints(2, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 30, 0));
    this.add(jPanel1, new GridBagConstraints(0, 2, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
	this.add(FirstLineFieldNamesCheckBox, new GridBagConstraints(0, 3, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 10, 0, 0), 0, 0));
    this.add(scrollPane, new GridBagConstraints(0, 4, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 100));
    this.add(FillerPanel, new GridBagConstraints(2, 5, 3, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    //jPanel2.add(ConseqDelimitCheckBox, new GridBagConstraints(0, 0, 3, 1, 1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    jPanel2.add(jLabel3, new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    jPanel2.add(TextQualifierComboBox, new GridBagConstraints(2, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jPanel2, new GridBagConstraints(2, 2, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, -7));

  }

	/**
	 * @param e
	 */
	protected void delimiters_actionPerformed() {
		generateDelimiters();
		reloadFile();
	}

	/**
	 * 
	 */
	private void reloadFile() {
		StringTokenizer st = new StringTokenizer(aFileFormatDataModel.linesFromFile[0],delimiters);
		int aCount = st.countTokens();
		columnNames = new String[aCount];
		Object[][] data = null;
		if(FirstLineFieldNames) {
			data = new Object[aFileFormatDataModel.linesFromFile.length-1 ][aCount];
			for(int j=0 ; j<aFileFormatDataModel.linesFromFile.length ; j++) {
				if(aFileFormatDataModel.linesFromFile[j] != null ) {
					st = new StringTokenizer(aFileFormatDataModel.linesFromFile[j],delimiters);
					int i = 0;
					while (st.hasMoreTokens() && i < aCount) {
						if(j == 0) {
							columnNames[i] = st.nextToken();
						} else {
							data[j-1][i] = st.nextToken();
						}
						i++;
					}
				}
			}
		
		}else {
			data = new Object[aFileFormatDataModel.linesFromFile.length ][aCount];
			for(int j=0 ; j<aFileFormatDataModel.linesFromFile.length ; j++) {
				if(aFileFormatDataModel.linesFromFile[j] != null ) {
					st = new StringTokenizer(aFileFormatDataModel.linesFromFile[j],delimiters);
					int i = 0;
					while (st.hasMoreTokens() && i < aCount) {
						data[j][i] = st.nextToken();
						i++;
					}
				}
			}
		
			columnNames = new String[aCount];
			for(int i=0; i < columnNames.length ; i++ ) {
				columnNames[i] ="Field "+Integer.toString(i);
			}
			
		}

		DefaultTableModel aModel = new DefaultTableModel(data, columnNames);
		jTable1.setModel(aModel);
	}
	/**
	 * 
	 */
	private void generateDelimiters() {
		StringBuffer buf = new StringBuffer();
		if(TabCheckBox.isSelected()) buf.append('\t' );
		if(SpaceCheckBox.isSelected()) buf.append(' ' );
		if(SemicolonCheckBox.isSelected()) buf.append(';' );
		if(CommaCheckBox.isSelected()) buf.append(',' );
		if(OtherCheckBox.isSelected() && OtherTextField.getText().length()>0) {
			buf.append( OtherTextField.getText() );
		} 
		
		delimiters = buf.toString();
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
		// nothing to validate as no action is required on this screen.
		
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
		DefaultTableModel aModel = (DefaultTableModel) jTable1.getModel();
		String aColumnName = null;
		String aDelimiter = null;
		DataFieldMetadata aDataFieldMetadata = null;
		StringTokenizer st = new StringTokenizer(aFileFormatDataModel.linesFromFile[0],delimiters,true);
		int aSize = aModel.getColumnCount();
		for(int i= 0 ; i < aSize ; i++ ) {
			aColumnName = aModel.getColumnName(i);
			//get token
			if(st.hasMoreTokens()) st.nextToken();
			//get delimiter
			if(st.hasMoreTokens()) {
				aDelimiter = st.nextToken();
			} else {
				aDelimiter = "\n";
			}
			
			if(i < aFileFormatDataModel.recordMeta.getNumFields() ) {
				aDataFieldMetadata = aFileFormatDataModel.recordMeta.getField(i);
				if(!aColumnName.equalsIgnoreCase(aDataFieldMetadata.getName())) {
					aDataFieldMetadata.setName(aColumnName);
				}
				if(!aDelimiter.equalsIgnoreCase(aDataFieldMetadata.getDelimiter())) {
					aDataFieldMetadata.setDelimiter(aDelimiter);
				}
				} else {
					// new field - just add it
					aFileFormatDataModel.recordMeta.addField( new DataFieldMetadata(aColumnName,aDelimiter));
				}
		}
		aFileFormatDataModel.firstLineFieldNames = FirstLineFieldNames;
	}
	/**
	 * Used to populate the form with data.
	 * <p>
	 * @see org.jetel.gui.component.FormInterface#loadData()
	 */
	public void loadData() {
		Object[][] data = null;
		
		delimiters = aFileFormatDataModel.fieldDelimiters;
		if(delimiters == null) {
			delimiters = "";
		}
		aDataRecordMetadata = aFileFormatDataModel.recordMeta;
		if(aDataRecordMetadata.getNumFields()==0) {
			// nothing before so lets initialize it with defaults
			generateDelimiters();
			reloadFile();
		} else {
			FirstLineFieldNamesCheckBox.setSelected(aFileFormatDataModel.firstLineFieldNames);
			FirstLineFieldNames = aFileFormatDataModel.firstLineFieldNames;
			loadExistingDelimitedRecords();						
		}

	}

	/**
	 * 
	 */
	private void loadExistingDelimitedRecords() {
		Object[][] data = null;
		StringBuffer buf = new StringBuffer();
		DataFieldMetadata aDataFieldMetadata = null;
		
		columnNames = new String[aDataRecordMetadata.getNumFields()];
		for( int i = 0 ; i < columnNames.length ; i++) {
			aDataFieldMetadata = aDataRecordMetadata.getField(i);
			columnNames[i] = aDataFieldMetadata.getName();
			buf.append(aDataFieldMetadata.getDelimiter());
		}
		String delimiters= buf.toString();

		data = new Object[aFileFormatDataModel.linesFromFile.length ][columnNames.length];
		StringTokenizer st = new StringTokenizer(aFileFormatDataModel.linesFromFile[0],delimiters);
		int aCount = st.countTokens();
		
		if(aFileFormatDataModel.firstLineFieldNames) {
			data = new Object[aFileFormatDataModel.linesFromFile.length-1 ][aCount];
			for(int j=0 ; j<aFileFormatDataModel.linesFromFile.length ; j++) {
				if(aFileFormatDataModel.linesFromFile[j] != null ) {
					st = new StringTokenizer(aFileFormatDataModel.linesFromFile[j],delimiters);
					int i = 0;
					while (st.hasMoreTokens() && i < aCount) {
						if(j != 0) {
							data[j-1][i] = st.nextToken();
						}
						i++;
					}
				}
			}
		
		}else {
			data = new Object[aFileFormatDataModel.linesFromFile.length ][aCount];
			for(int j=0 ; j<aFileFormatDataModel.linesFromFile.length ; j++) {
				if(aFileFormatDataModel.linesFromFile[j] != null ) {
					st = new StringTokenizer(aFileFormatDataModel.linesFromFile[j],delimiters);
					int i = 0;
					while (st.hasMoreTokens() && i < aCount) {
						data[j][i] = st.nextToken();
						i++;
					}
				}
			}
		}

		DefaultTableModel aModel = new DefaultTableModel(data, columnNames);
		jTable1.setModel(aModel);
	}
	
	
	/**
	 * Used to expose access to data model.
	 * @see org.jetel.gui.component.FormInterface#getFileFormatDataModel()
	 */
	public FileFormatDataModel getFileFormatDataModel() {
		return aFileFormatDataModel;
	}
}