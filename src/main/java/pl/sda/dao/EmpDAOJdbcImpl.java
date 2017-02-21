package pl.sda.dao;

import pl.sda.domain.Employee;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by pzawa on 02.02.2017.
 */
public class EmpDAOJdbcImpl implements EmpDAO {
	private static String QUERY_BY_ID = "SELECT empno, ename, job, manager, hiredate, salary, commision, deptno FROM emp WHERE empno = ?";
	private static String INSERT_STMT = "INSERT INTO emp(empno, ename, job, manager, hiredate, salary, commision, deptno) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
	private static String UPDATE_STMT = "UPDATE emp set ename = ?, job = ?, manager = ?, hiredate = ? , salary = ? , commision = ? , deptno = ?  WHERE empno = ?";
	private static String DELETE_STMT = "DELETE FROM emp WHERE empno = ?";
	private static String GET_TOTAL_SALARY_BY_DEPT = "{ ?= call sda.calculate_salary_by_dept(?)}";

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
		try (Connection conn = jdbcConnectionManager.getConnection()) {
			PreparedStatement ps = conn.prepareStatement(UPDATE_STMT);
			ps.setString(1, employee.getEname());
			ps.setString(2, employee.getJob());
			ps.setInt(3, employee.getManager());
			ps.setDate(4, new java.sql.Date(employee.getHiredate().getTime()));
			ps.setBigDecimal(5, employee.getSalary());
			ps.setBigDecimal(6, employee.getCommision());
			ps.setInt(7, employee.getDeptno());
			ps.setInt(8, employee.getEmpno());

			ps.executeUpdate();
		}
	}

	@Override
	public void delete(int id) throws Exception {
		try (Connection conn = jdbcConnectionManager.getConnection()) {
			PreparedStatement ps = conn.prepareStatement(DELETE_STMT);
			ps.setInt(1, id);

			ps.executeUpdate();
		}
	}

	@Override
	public void create(List<Employee> employees) throws Exception {
		/*
		 * podejscie pierwsze, wylapanie po stronie javy
		 */
//		checkList(employees);
//		for (Employee e : employees) {
//			create(e);
//		}

		/*
		 * podejscie drugie, transakcja i wylapanie po stronie bazy
		 */
		try(Connection conn = jdbcConnectionManager.getConnection()) {
			conn.setAutoCommit(false);
			PreparedStatement ps = conn.prepareStatement(INSERT_STMT);
			for (Employee e : employees) {
				ps.setInt(1, e.getEmpno());
				ps.setString(2, e.getEname());
				ps.setString(3, e.getJob());
				ps.setInt(4, e.getManager());
				ps.setDate(5, new java.sql.Date(e.getHiredate().getTime()));
				ps.setBigDecimal(6, e.getSalary());
				ps.setBigDecimal(7, e.getCommision());
				ps.setInt(8, e.getDeptno());
				
//				zamiast executeUpdate() - lepsze wydajnosciowe, doczytac
				ps.addBatch();
				
//				ps.executeUpdate();
				
			}
//			zamiast executeUpdate() - lepsze wydajnosciowe, doczytac
			ps.executeBatch();
			conn.commit();
		}
		
	}

	private void checkList(List<Employee> employees) throws SQLException {
		Set<Employee> set = new HashSet<>(employees);
		if (set.size() < employees.size()) {
			throw new SQLException();
		}
	}

	@Override
	public BigDecimal getTotalSalaryByDept(int dept) throws Exception {
		BigDecimal out;
		try (Connection conn = jdbcConnectionManager.getConnection()) {
			CallableStatement stmt = conn.prepareCall(GET_TOTAL_SALARY_BY_DEPT);
			stmt.registerOutParameter(1, java.sql.Types.DECIMAL);
			stmt.setInt(2, dept);
			stmt.execute();
			out = stmt.getBigDecimal(1);
		}
		return out;
	}
}
