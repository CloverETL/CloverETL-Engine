
import java.io.ByteArrayInputStream;

import org.jetel.component.DataRecordTransform;
import org.jetel.component.XmlXPathReader;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.XPathParser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.util.XmlUtils;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Document;

public class ReformatOrders extends DataRecordTransform{

	int counter=0;
	private XPathParser parser;
	private boolean skipOnError = false;

	public boolean init() throws ComponentNotReadyException {
		//create and init XPathParser
		String mapping = 
	              " <Context xpath=\"/records/customer\" outPort=\"0\"> " + 
	              " <Context xpath=\"/records/customer/order\" outPort=\"1\" /> " +
	          	  " </Context> ";
		
		try {
			Document doc = XmlUtils.createDocumentFromString(mapping);
			parser = new XPathParser(doc);
			parser.init();
		} catch (Exception e) {
			throw new ComponentNotReadyException(getNode(),e);
		}
		//get property from component custom properties
		skipOnError = Boolean.parseBoolean(parameters.getProperty("skip_on_error", "false"));
	    return true;
	}
	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		if (!getGraph().firstRun()){
			parser.reset();
		}
		super.preExecute();
	}

	public int transform(DataRecord[] source, DataRecord[] target) throws TransformException{
   		System.out.println("============== XPath transform ==============");
		for (int i=0; i<target.length; i++){
	   		System.out.println("assign port:"+i);
			parser.assignRecord(target[i], i);
		}// for

		String s = source[0].getField(0).toString();
	    System.out.println("source XML:"+s);
		try {
			parser.setDataSource( new ByteArrayInputStream(StringUtils.stringToSpecChar(s).getBytes("UTF-8")) );
			
			boolean[] flags = new boolean[target.length];
			DataRecord dr = null;
			while ((dr = parser.getNext()) != null){
				int port = parser.getActualPort();
				if (flags[port])
					continue;
				flags[port] = true;	
				target[port] = dr.duplicate();
			    System.out.println("OUTport:"+port+" data record:"+target[port]);
			} // while
		} catch (Exception e) {
			throw new TransformException("Transormation  failed", e, counter, 0);
		} 

		counter++;
		return ALL;
	}
	
	@Override
	public int transformOnError(Exception exception, DataRecord[] sources,
			DataRecord[] target) throws TransformException {
		if (skipOnError) {
			System.err.println("Skipping invalid record. Error: " + exception.getCause().getMessage());
			return SKIP;//ignore invalid records
		}
		return super.transformOnError(exception, sources, target);//throw exception
	}
}

