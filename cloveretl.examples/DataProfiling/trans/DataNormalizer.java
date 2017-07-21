import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Pattern;

import org.jetel.component.normalize.DataRecordNormalize;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * @author avackova
 *
 */
public class DataNormalizer extends DataRecordNormalize {
	
	protected final static int NAME = 0;
	protected final static int TYPE = 1;
	protected final static int VALUE = 2;	
	protected final static int FIRST_NOT_NULL = 3;	
	protected final static int IS_ASCII = 4;	
	protected final static int IS_NUMBER = 5;	
	
	private int count;

	DataRecordMetadata sourceMetadata;
	
	private boolean[] isAscii;//indicator if all values for given field has been Ascii 
	private boolean[] firstNotNull;//indicator if it has been found not null value for given field
	private boolean[] isNumber;//indicator if all values for given field has been number 
	
	public boolean init(Properties parameters,
			DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		count = sourceMetadata.getNumFields();
		this.sourceMetadata = sourceMetadata;
		isAscii = new boolean[count];
		Arrays.fill(isAscii, true);
		firstNotNull = new boolean[count];
		Arrays.fill(firstNotNull, false);
		isNumber = new boolean[count];
		Arrays.fill(isNumber, true);
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
		char fieldType = sourceMetadata.getFieldType(idx);
		target.getField(NAME).setValue(sourceMetadata.getField(idx).getName());
		target.getField(TYPE).setValue(DataFieldMetadata.type2Str(fieldType));
		if (source.getField(idx).isNull()) {
			target.getField(VALUE).setNull(true);
			return OK;
		}
		Object value = source.getField(idx).getValue();
		switch (fieldType) {
		case DataFieldMetadata.DATE_FIELD:
			target.getField(VALUE).setValue(((Date)value).getTime());
			break;
		case DataFieldMetadata.DECIMAL_FIELD:
		case DataFieldMetadata.INTEGER_FIELD:
		case DataFieldMetadata.LONG_FIELD:
		case DataFieldMetadata.NUMERIC_FIELD:
			target.getField(VALUE).setValue(value);
			break;
		case DataFieldMetadata.STRING_FIELD:
			target.getField(VALUE).setValue(((CharSequence)value).length());
			if (isAscii[idx] && !Pattern.matches("\\p{ASCII}*", ((CharSequence) value))) {
				target.getField(IS_ASCII).setValue(false);
				isAscii[idx] = false;
			}
			if (!firstNotNull[idx]) {
				target.getField(FIRST_NOT_NULL).setValue(value);
				firstNotNull[idx] = true;
			}
			if (isNumber[idx] && !StringUtils.isNumber((CharSequence) value)) {
				target.getField(IS_NUMBER).setValue(false);
				isNumber[idx] = false;
			}
			break;
		}
		return OK;
	}

}
