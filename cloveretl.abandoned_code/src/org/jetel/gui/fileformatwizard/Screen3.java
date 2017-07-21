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
import javax.swing.JTextPane;
import java.awt.SystemColor;
import java.awt.Font;
import javax.swing.JTable;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.MouseEvent;
import java.util.StringTokenizer;

import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.DefaultTableModel;

import org.jetel.gui.component.FormInterface;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CloverProperties;

public class Screen3 extends JPanel implements FormInterface {
  private GridBagLayout gridBagLayout1 = new GridBagLayout();
  private JLabel jLabel1 = new JLabel();
  private JLabel jLabel2 = new JLabel();
  private JTextPane jTextPane1 = new JTextPane();
  private JTable jTable1 = null;
  private JPanel jPanel1 = new JPanel();
  private JPanel jPanel2 = new JPanel();
  private GridBagLayout gridBagLayout2 = new GridBagLayout();
  private JLabel jLabel3 = new JLabel();
  private JLabel jLabel4 = new JLabel();
  private JTextField jTextField1 = new JTextField();
  private JCheckBox jCheckBox1 = new JCheckBox();
  private JComboBox jComboBox1 = null;
   private JPanel jPanel3 = new JPanel();
   private JLabel jLabel5 = new JLabel();
   private JLabel jLabel6 = new JLabel();
   private JLabel jLabel7 = new JLabel();
   	 
  private FileFormatDispatcher aDispatcher;
  private FileFormatDataModel aFileFormatDataModel;
  private DataRecordMetadata aDataRecordMetadata;
  private String[] columnNames = null;	
  private int selectedCol = -1;
  
  public Screen3(FileFormatDispatcher aDispatcher, FileFormatDataModel aFileFormatDataModel)
  {
	this.aDispatcher = aDispatcher;
	this.aFileFormatDataModel = aFileFormatDataModel;
  	
    try
    {
		String[] petStrings = CloverProperties.getTypeNameList();
		jComboBox1 = new JComboBox(petStrings);
		//loadData();
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
    jLabel1.setText("Screen 3 of 4");
    jLabel1.setFont(new Font("Dialog", 1, 11));
    jLabel2.setText("Specify Field Attributes");
    jTextPane1.setText("You can edit field attributes by clicking on the table and selecting a column that corresponds to field you want to change.  Now you can change field name and any other attributes that are displayed.\n\nPlease note that all attributes are saved automatically.  There is no 'Cancel' button.");
    jTextPane1.setBackground(SystemColor.control);
	jTextPane1.setEditable(false);
    jPanel2.setLayout(gridBagLayout2);
    jLabel3.setText("Field Name");
    jLabel4.setText("Data Type");
    jTextField1.setText("Nothing");
    
	jLabel6.setText("Nullable?");
	//jPanel3.setBackground(new Color(73, 66, 236));
	jLabel5.setText("is selected for editing");
	jLabel7.setText("Field x");
	//jLabel7.setHorizontalTextPosition(SwingConstants.LEFT);
	//jLabel7.setHorizontalAlignment(SwingConstants.LEFT);
    
	jTextField1.addFocusListener(new java.awt.event.FocusAdapter()
	  {
		public void focusLost(FocusEvent e)
		{
		  jTextField1_focusLost(e);
		}
	  });

	jCheckBox1.addActionListener(new ActionListener()
	  {
		public void actionPerformed(ActionEvent e)
		{
		  jCheckBox1_actionPerformed(e);
		}
	  });

	jComboBox1.addActionListener(new ActionListener()
	  {
		public void actionPerformed(ActionEvent e)
		{
		  jComboBox1_actionPerformed(e);
		}
	  });

    
    
    
	jTable1 = new JTable();
	jTable1.setRowSelectionAllowed(false);
	jTable1.setColumnSelectionAllowed(true);
	//jTable1.setCellSelectionEnabled(false);
	jTable1.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
	jTable1.addMouseListener(new java.awt.event.MouseAdapter()
	  {
		public void mouseClicked(MouseEvent e)
		{
		  jTable1_mouseClicked(e);
		}
	  });

	ListSelectionModel colSM = jTable1.getColumnModel().getSelectionModel();
	colSM.addListSelectionListener(new ListSelectionListener() {
		public void valueChanged(ListSelectionEvent e) {
			//Ignore extra messages.
			if (e.getValueIsAdjusting()) return;
//			System.out.println("ListSelectionEvent " + e.getFirstIndex());

			ListSelectionModel lsm = (ListSelectionModel)e.getSource();
			if (lsm.isSelectionEmpty()) {
				selectedCol = -1;
				jTextField1.setText("");
				jTextField1.setEnabled(false);
				jCheckBox1.setEnabled(false);
				jComboBox1.setEnabled(false);
				jLabel7.setText("Nothing");
				//System.out.println("No columns are selected.");
			} else {
				selectedCol = lsm.getMinSelectionIndex();
//				System.out.println("Column " + selectedCol
//								   + " is now selected.");
				jTextField1.setEnabled(true);
				jCheckBox1.setEnabled(true);
				jComboBox1.setEnabled(true);
				String colName = jTable1.getColumnName(selectedCol);
				if (colName != null) {
					jLabel7.setText(colName);
					jTextField1.setText(colName);
					DataFieldMetadata aField = aDataRecordMetadata.getField(jTable1.getColumnName(selectedCol));
					if (aField != null) {
						jCheckBox1.setSelected( aField.isNullable() );
						jComboBox1.setSelectedItem( CloverProperties.getTypeName( aField.getType()) );
					}
				}
			}
		}
	});
//	Create the scroll pane and add the table to it. 
	  JScrollPane scrollPane = new JScrollPane(jTable1);

   
    this.add(jLabel1, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.NORTHWEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jLabel2, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    this.add(jTextPane1, new GridBagConstraints(0, 1, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    this.add(scrollPane, new GridBagConstraints(0, 3, 2, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
	//this.add(jPanel1, new GridBagConstraints(0, 4, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
	this.add(jPanel2, new GridBagConstraints(0, 2, 2, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
	jPanel2.add(jLabel3, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
	jPanel2.add(jLabel4, new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
	jPanel2.add(jTextField1, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL, new Insets(0, 5, 5, 5), 50, 0));
	jPanel2.add(jCheckBox1, new GridBagConstraints(2, 2, 1, 1, 0.0, 1.0, GridBagConstraints.NORTH, GridBagConstraints.NONE, new Insets(0, 5, 5, 5), 0, 0));
	jPanel2.add(jComboBox1, new GridBagConstraints(1, 2, 1, 1, 0.0, 0.0, GridBagConstraints.NORTH, GridBagConstraints.NONE, new Insets(0, 5, 5, 5), 0, 0));
	jPanel2.add(jLabel6, new GridBagConstraints(2, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
	jPanel3.add(jLabel7, null);
	jPanel3.add(jLabel5, null);
	jPanel2.add(jPanel3, new GridBagConstraints(0, 0, 3, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
  }

  private void jTable1_mouseClicked(MouseEvent e)
  {
  	//System.out.println("jTable1_mouseClicked " + e.getX() + " " + e.getY());
  }

private void jCheckBox1_actionPerformed(ActionEvent e)
{
	Object source = e.getSource();
	//System.out.println("ActionEvent e " + jCheckBox1.isSelected());
	if(selectedCol!= -1) { //reset values only if something is selected
		aDataRecordMetadata.getField(selectedCol).setNullable(jCheckBox1.isSelected());
	}
}

private void jComboBox1_actionPerformed(ActionEvent e)
{
	//System.out.println("ActionEvent e " + jComboBox1.getSelectedItem());
	if(selectedCol!= -1) { //reset values only if something is selected
		aDataRecordMetadata.getField(selectedCol).setType(CloverProperties.getTypeOfName((String)jComboBox1.getSelectedItem()));
	}
}

private void jTextField1_focusLost(FocusEvent e)
{
	//System.out.println("FocusEvent e " + " "+ jTextField1.getText());
	if(selectedCol!= -1) { //reset values only if something is selected
		aDataRecordMetadata.getField(selectedCol).setName(jTextField1.getText());
		columnNames[selectedCol]=jTextField1.getText();
		jTable1.getColumnModel().getColumn(selectedCol).setHeaderValue(jTextField1.getText());
//		Force the header to resize and repaint itself
		jTable1.getTableHeader().resizeAndRepaint();
	}
}

	/**
	 * Used to populate the form with data.
	 * <p>
	 * @see org.jetel.gui.component.FormInterface#loadData()
	 */
	public void loadData() {
		aDataRecordMetadata = aFileFormatDataModel.recordMeta;
		if(aDataRecordMetadata.getRecType() == DataRecordMetadata.FIXEDLEN_RECORD) {
			loadFixedData();
		} else {
			loadDelimitedData();
		}
	}
	
	
	/**
	 * 
	 */
	private void loadDelimitedData() {
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
	 * 
	 */
	private void loadFixedData() {
		Object[][] data = null;
		
		columnNames = new String[aDataRecordMetadata.getNumFields()];
		for( int i = 0 ; i < columnNames.length ; i++) {
			columnNames[i] = aDataRecordMetadata.getField(i).getName();
		}
		data = new Object[aFileFormatDataModel.linesFromFile.length ][columnNames.length];
		int fieldSize =  0;
		int fieldStart = 0;
		int fieldEnd = 0;
		int recordSize = 0;
		
		if(aFileFormatDataModel.oneRecordPerLine) {
			for( int i = 0 ; i < columnNames.length ; i++) {
				fieldSize = aDataRecordMetadata.getField(i).getSize();
				fieldEnd = fieldEnd + fieldSize;
				for(int j=0 ; j<aFileFormatDataModel.linesFromFile.length ; j++) {
					if(aFileFormatDataModel.linesFromFile[j] != null ) {
							data[j][i] = aFileFormatDataModel.linesFromFile[j].substring(fieldStart, fieldEnd);
					}
				}
				fieldStart = fieldStart + fieldSize;
			}
		} else {  // more than oneRecordPerLine
			for( int i = 0 ; i < columnNames.length ; i++) {
				recordSize = recordSize +aDataRecordMetadata.getField(i).getSize();
			}
			for( int i = 0 ; i < columnNames.length ; i++) {
				fieldSize = aDataRecordMetadata.getField(i).getSize();
				fieldEnd = fieldEnd + fieldSize;
				for(int j=0 ; j<aFileFormatDataModel.linesFromFile.length ; j++) {
					// all on one line
					if(aFileFormatDataModel.linesFromFile[0] != null ) {
						if(aFileFormatDataModel.linesFromFile[0]!= null && (recordSize*j+fieldEnd)<aFileFormatDataModel.linesFromFile[0].length()) {
							data[j][i] = aFileFormatDataModel.linesFromFile[0].substring((recordSize*j+fieldStart), (recordSize*j+fieldEnd));
						} else {
							data[j][i] = null;
						}
					}
				}
				fieldStart = fieldStart + fieldSize;
			}
		}	

		DefaultTableModel aModel = new DefaultTableModel(data, columnNames);
		jTable1.setModel(aModel);
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
		//no validation required; if the user does not make any changes then we just
		//have default values
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
		aFileFormatDataModel.recordMeta = aDataRecordMetadata ;
		
	}

	/**
	 * Used to expose access to data model.
	 * @see org.jetel.gui.component.FormInterface#getFileFormatDataModel()
	 */
	public FileFormatDataModel getFileFormatDataModel() {
		return aFileFormatDataModel;
	}
}