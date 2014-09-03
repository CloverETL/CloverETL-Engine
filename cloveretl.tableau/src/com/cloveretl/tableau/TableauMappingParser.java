package com.cloveretl.tableau;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.string.StringUtils;

import com.tableausoftware.DataExtract.Type;

public class TableauMappingParser {
	
	public static final String TABLEAU_MAPPING_PROPERTY_DELIMITER = ",";//$NON-NLS-1$
	public static final String DEFAULT_COLLATION = "default";//$NON-NLS-1$
	public static final String DEFAULT_TABLEAU_TYPE = "automatic"; //$NON-NLS-1$

	private HashMap<String, TableauMapping> tableauMapping = new HashMap<String, TableauMapping>();
	DataRecordMetadata inMetadata;
	boolean paramsAllowed;

	private List<String> errors = new ArrayList<String>();

	public TableauMappingParser(String mappingString, boolean paramsAllowed,
			DataRecordMetadata inMetadata) {
		this.inMetadata = inMetadata;

		this.paramsAllowed = paramsAllowed;

		parseMapping(StringUtils.split(mappingString));

		// fill in missing values (use defaults)

		for (DataFieldMetadata field : inMetadata.getFields()) {
			String fieldName = field.getName();
			TableauMapping mapping = tableauMapping.get(fieldName);
			if (mapping == null) {
				mapping = new TableauMapping(fieldName, DEFAULT_TABLEAU_TYPE,
						DEFAULT_COLLATION);
				tableauMapping.put(fieldName, mapping);
			}
			if (mapping.getTableauType() == null) {
				mapping.setTableauType(DEFAULT_TABLEAU_TYPE);
			}
			if (mapping.getCollation() == null) {
				mapping.setCollation(DEFAULT_COLLATION);
			}

			if (!mapping.getTableauType().equals(DEFAULT_TABLEAU_TYPE)) {
				try {
					checkTypeCompatibility(field,
							Type.valueOf(mapping.getTableauType()));
				} catch (NoClassDefFoundError e) {
					errors.add("Tableau libraries need to be set on environment variable PATH!");
				}
			}
		}
	}

	private void checkTypeCompatibility(DataFieldMetadata field,
			Type tableauType) {
		if (tableauType == null) {
			errors.add("Invalid tableau type set for field: " + field.getName());
			return;
		}
		if (!Arrays.asList(TableauWriter.getCompatibleTypes(field)).contains(
				tableauType)) {
			errors.add("Incompatible types set for field " + field.getName()
					+ ": " + field.getDataType() + "," + tableauType.name());
			return;
		}

	}

	private void parseMapping(String[] mapping) {
		for (String expression : mapping) {
			String expr2 = expression.trim();
			if (expr2.equals("")) {
				continue;
			}

			try {
				parseTableauMapping(expr2);
			} catch (Exception e) {
				String message = "Invalid mapping '" + expr2 + "'";
				errors.add(ExceptionUtils.getMessage(message, e));
			}
		}
	}

	private void parseTableauMapping(String expr) {
		String[] parsedExpression = expr.split(Defaults.ASSIGN_SIGN);
		String inputField = parsedExpression[0].trim().substring(
				Defaults.CLOVER_FIELD_INDICATOR.length()); // skip the leading "$"

		parsedExpression = parsedExpression[1]
				.split(TABLEAU_MAPPING_PROPERTY_DELIMITER);

		String tableauType = parsedExpression[0].trim();
		String collation = parsedExpression[1].trim();

		TableauMapping mapping = new TableauMapping(inputField, tableauType,
				collation);

		tableauMapping.put(inputField, mapping);
	}

	public HashMap<String,TableauMapping> getTableauMapping() {
		return tableauMapping;
	}

	public List<String> getErrors() {
		return errors;
	}

	public static class TableauMapping {
		private String collation;
		private String inputField;
		private String tableauType;

		public TableauMapping(String inputField, String tableauType,
				String collation) {
			super();
			this.collation = collation;
			this.inputField = inputField;
			this.tableauType = tableauType;
		}

		public String getCollation() {
			return collation;
		}

		public void setCollation(String collation) {
			this.collation = collation;
		}

		public String getInputField() {
			return inputField;
		}

		public void setInputField(String inputField) {
			this.inputField = inputField;
		}

		public String getTableauType() {
			return tableauType;
		}

		public void setTableauType(String tableauType) {
			this.tableauType = tableauType;
		}
	}
}
