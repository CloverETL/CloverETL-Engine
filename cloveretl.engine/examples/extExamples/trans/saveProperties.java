import java.io.OutputStream;
import java.io.File;
import java.net.URL;

import org.jetel.component.BasicJavaRunnable;
import org.jetel.util.file.FileUtils;


public class saveProperties extends BasicJavaRunnable {
	
	private final static String FILE_NAME = "dbInc.txt";

	public void run() {
		try {
			URL file = FileUtils.getFileURL(getGraph().getProjectURL(), getGraph().getGraphProperties().getProperty("PROJECT") + File.separator + FILE_NAME);
			getGraph().getLogger().info("Writing properties to file: " + file);
			OutputStream out = FileUtils.getOutputStream(getGraph().getProjectURL(), 
					getGraph().getGraphProperties().getProperty("PROJECT") + File.separator + FILE_NAME, false, -1);
			parameters.store(out, null);
			out.flush();
			out.close();
		} catch (Exception e) {
			throw new RuntimeException("Can't save parameters", e);
		} 
	}

}
