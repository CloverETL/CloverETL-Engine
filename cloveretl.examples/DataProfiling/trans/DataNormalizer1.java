import java.util.*;

import org.jetel.component.normalize.DataRecordNormalize;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * @author avackova
 *
 */
public class DataNormalizer1 extends DataRecordNormalize {
	
	protected final static int NAME = 0;
	protected final static int TYPE = 1;
	protected final static int VALUE_BOOLEAN = 2;	
	protected final static int VALUE_BYTE = 3;	
	protected final static int VALUE_DATE = 4;	
	protected final static int VALUE_NUMBER = 5;	
	protected final static int VALUE_STRING = 6;	
	
	private int count;
	private DataRecordMetadata sourceMetadata;
	
	private boolean[] stat;
	
	private String fieldName;
	
	public boolean init(Properties parameters,
			DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		this.sourceMetadata = sourceMetadata;
		count = sourceMetadata.getNumFields();
		String statFields = getGraph().getGraphProperties().getStringProperty("STATISTIC_FIELDS");
		String notStatFields = getGraph().getGraphProperties().getStringProperty("NOT_STATISTIC_FIELDS");
		String[] field;
		stat = new boolean[sourceMetadata.getNumFields()];
		if (!StringUtils.isEmpty(statFields)) {
			field = statFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			for (int i = 0; i < stat.length; i++) {
				stat[i] = StringUtils.findString(sourceMetadata.getField(i).getName(), field) > -1;
			}
		}else if (!StringUtils.isEmpty(notStatFields)){
			field = notStatFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			for (int i = 0; i < stat.length; i++) {
				stat[i] = StringUtils.findString(sourceMetadata.getField(i).getName(), field) == -1;
			}
		}else{
			Arrays.fill(stat, true);
		}
		return super.init(parameters, sourceMetadata, targetMetadata);
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.normalize.RecordNormalize#count(org.jetel.data.DataRecord)
	 */
	public int count(DataRecord arg0) {
		return count;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.normalize.RecordNormalize#transform(org.jetel.data.DataRecord, org.jetel.data.DataRecord, int)
	 */
	public int transform(DataRecord source, DataRecord target, int idx) throws TransformException{
		fieldName = sourceMetadata.getField(idx).getName();
		if (!stat[idx]) {
			return SKIP;
		}
		char fieldType = sourceMetadata.getFieldType(idx);
		target.getField(NAME).setValue(fieldName);
		target.getField(TYPE).setValue(DataFieldMetadata.type2Str(fieldType));
		switch (fieldType) {
		case DataFieldMetadata.DATE_FIELD:
			target.getField(VALUE_DATE).setValue(source.getField(idx));
			break;
		case DataFieldMetadata.DECIMAL_FIELD:
		case DataFieldMetadata.INTEGER_FIELD:
		case DataFieldMetadata.LONG_FIELD:
		case DataFieldMetadata.NUMERIC_FIELD:
			target.getField(VALUE_NUMBER).setValue(source.getField(idx));
			break;
		case DataFieldMetadata.STRING_FIELD:
			target.getField(VALUE_STRING).setValue(source.getField(idx));
			break;
		case DataFieldMetadata.BOOLEAN_FIELD:
			target.getField(VALUE_BOOLEAN).setValue(source.getField(idx));
			break;
		case DataFieldMetadata.BYTE_FIELD:
		case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
			target.getField(VALUE_BYTE).setValue(source.getField(idx));
			break;
		}
		return OK;
	}

	public String getMessage() {
		return "Skipping field " + fieldName;
	}
	
}
