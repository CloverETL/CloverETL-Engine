import java.io.File;

import org.jetel.component.BasicJavaRunnable;
 
public class testJavaRunnable extends BasicJavaRunnable {
 
	public void run() {
		File currentDir = new File(".");
		File connDir = new File(getGraph().getGraphProperties().getStringProperty("CONN_DIR"));
		getGraph().getLogger().info("Currennt directory: " + currentDir.getAbsolutePath());
		getGraph().getLogger().info("Connection directory: " +  connDir.getAbsolutePath());
		File[] file = connDir.listFiles();
		getGraph().getLogger().info("Files in connection directory: " );
		for (int i = 0; i < file.length; i++) {
			getGraph().getLogger().info(file[i].getName());
		}
	}
}