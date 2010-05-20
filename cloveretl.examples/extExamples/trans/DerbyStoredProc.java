import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


/**
This class contains stored procedures for derby. 
It must be compiled by java 1.5 and packaged into extExamples/data-in/derby.db/jar/APP/GET_CITY_INFO.jar.G1271404804210 .
*/
public class DerbyStoredProc {
	
	public static void cityInfo(int city_id, ResultSet[] rsOut) throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("SELECT * FROM app.city WHERE id = ?");
		ps.setInt(1, city_id);
		rsOut[0] = ps.executeQuery();
		conn.close();
	}
}
