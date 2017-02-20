package pl.sda.dao;

import pl.sda.domain.Employee;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

/**
 * Created by pzawa on 02.02.2017.
 */
public class EmpDAOJdbcImpl implements EmpDAO {
	private static String QUERY_BY_ID = "SELECT empno, ename, job, manager, hiredate, salary, commision, deptno FROM emp WHERE empno = ?";
	private static String INSERT_STMT = "INSERT INTO emp(empno, ename, job, manager, hiredate, salary, commision, deptno) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
	
	private final JdbcConnectionManager jdbcConnectionManager;

	public EmpDAOJdbcImpl(JdbcConnectionManager jdbcConnectionManager) {
		this.jdbcConnectionManager = jdbcConnectionManager;
	}

	@Override
	public Employee findById(int id) throws Exception {
		try (Connection conn = jdbcConnectionManager.getConnection()) {
			PreparedStatement ps = conn.prepareStatement(QUERY_BY_ID);
			ps.setInt(1, id);

			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				Employee employee = mapFromResult(rs);
				return employee;
			}
		}
		return null;
	}

	private Employee mapFromResult(ResultSet rs) throws SQLException {
		int empno = rs.getInt("empno");
		String ename = rs.getString("ename");
		String job = rs.getString("job");
		Integer manager = rs.getInt("manager");
		Date hiredate = rs.getDate("hiredate");
		BigDecimal salary = rs.getBigDecimal("salary");
		BigDecimal commision = rs.getBigDecimal("commision");
		int deptno = rs.getInt("deptno");
		return new Employee(empno, ename, job, manager, hiredate, salary, commision, deptno);
	}

	@Override
	public void create(Employee employee) throws Exception {
		try (Connection conn = jdbcConnectionManager.getConnection()) {
			PreparedStatement ps = conn.prepareStatement(INSERT_STMT);
			ps.setInt(1, employee.getEmpno());
			ps.setString(2, employee.getEname());
			ps.setString(3, employee.getJob());
			ps.setInt(4, employee.getManager());
			ps.setDate(5, new java.sql.Date(employee.getHiredate().getTime()));
			ps.setBigDecimal(6, employee.getSalary());
			ps.setBigDecimal(7, employee.getCommision());
			ps.setInt(8, employee.getDeptno());
			
			ps.executeUpdate();
		}

	}

	@Override
	public void update(Employee employee) throws Exception {

	}

	@Override
	public void delete(int id) throws Exception {

	}

	@Override
	public void create(List<Employee> employees) throws Exception {
	}

	@Override
	public BigDecimal getTotalSalaryByDept(int dept) throws Exception {
		return null;
	}
}
