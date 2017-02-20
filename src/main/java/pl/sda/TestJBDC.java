package pl.sda;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class TestJBDC {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("org.postgresql.Driver");
		Connection conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/postgres", "postgres",	"postgres");
		
		String enameTofinde = "KING";
		
		String query = "SELECT *" +" FROM SDA.EMP" + " where ename = ?";
		
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.setString(1, enameTofinde);
		ResultSet rs = stmt.executeQuery();
		
		int id = 0;
		while (rs.next()){
			String ename = rs.getString("ename");
			String job = rs.getString("job");
			Date hireDate = rs.getDate("hiredate");
			System.out.println("Employee no. " + ++id + ": " + ename + ", job: " + job + ", hiredate: " + hireDate);
		}
		
		conn.close();
	}
}
