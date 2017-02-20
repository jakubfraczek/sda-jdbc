package pl.sda;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestJBDC {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("org.postgresql.Driver");
		Connection conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/postgres", "postgres",	"postgres");
		
		String query = "SELECT ename FROM SDA.EMP";
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		
		int id = 0;
		while (rs.next()){
			String ename = rs.getString("ename");
			System.out.println("Employee no. " + ++id + ": " + ename);
		}
		
		conn.close();
	}
}
