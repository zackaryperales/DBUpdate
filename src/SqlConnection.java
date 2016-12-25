import java.sql.Connection;
import java.sql.DriverManager;

import javax.swing.JOptionPane;

public class SqlConnection {
	//Login Table = "jdbc:mysql://localhost:3306/login";
	//Stocks Table = "jdbc:mysql://localhost:3306/stocks";
			
			public static Connection getConnection(String url) throws Exception {
				try {
					String driver = "com.mysql.jdbc.Driver";
					String username = "root";
					String password = "dick";
					Class.forName(driver);
					
					Connection conn = DriverManager.getConnection(url,username,password);
					return conn;
					
				} catch(Exception e) {
					JOptionPane.showMessageDialog(null, e);
				}
				
				return null;
			}
}
