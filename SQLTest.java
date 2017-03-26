import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Scanner;

public class SQLTest {
	
	private static final String JDBCDRIVER = "com.mysql.jdbc.Driver";
	private static final String DBLOC = "jdbc:mysql://localhost:3306/billDB";
	private static final String DBNAME = "billDB";
	private static final String USER = "root";
	private static final String PW = "";
	
	
	
	public static Connection initConnection(){
		try {
			Class.forName(JDBCDRIVER);
			Connection con = DriverManager.getConnection(DBLOC,USER,PW);
			return con;
		}catch (ClassNotFoundException e) {
			System.out.println("Thou has not placed thy driver in thy proper place!");
			System.out.println(e);
		}catch (SQLException e) {
			System.out.println("SQL no likes you");
			System.out.println(e);
		}
		
		return null;
	}
	


	public static void main(String[] args){
		Scanner sc = getScanner();
		try {
			Connection con = initConnection();
			System.out.println("Successful connection to Database: "+ DBLOC);
			System.out.println();
			
			String answer;
			char string2char;
			do{
				System.out.println("Tables in "+ DBNAME);
				DatabaseMetaData md = con.getMetaData();
				ResultSet rs = md.getTables(null, null, "%", null);
				while (rs.next()) {
				  System.out.print(rs.getString(3) + "\t");
				}
				System.out.println("\n");
				System.out.println("Please select which table you wish to work on: ");
				String table = sc.nextLine();
				singleTableOptions(con, sc,table);
				
				System.out.println("Do you wish to continue? (Y/N)");
				answer = sc.next();
				string2char = answer.charAt(0);
				
			}while(Character.toUpperCase(string2char) == 'Y');
			
		} 
		catch (SQLException e) {
			System.out.println("An SQL EXCEPTION HAS OCCURED: " + e);
		}
		
		System.out.println("Program terminated.");
		
	}
	private static Scanner getScanner(){
		Scanner sc = new Scanner(System.in);
		return sc;
		
	}
	
	private static void executeSQL(Statement st, String sql){
		try{
			st.execute(sql);
			System.out.println("Database successfuly update!");
		}catch(SQLException e){
			System.out.print("SQL no like you\n" + e);
		}
		
	}

	private static void update(Connection con, String table) throws SQLException {
		Scanner sc = getScanner();
		int row = 0;
		System.out.print("Please enter in the row number(unique ID) you wish to update:\n");
		row = sc.nextInt();
		System.out.print("Please enter in the updated information in the following format \n column1=newValue1, column2=newValue2... \n");
		String sql = "UPDATE "+table+" SET "+ sc.next()+" WHERE ID="+row+";";
		Statement st = con.createStatement();
		executeSQL(st, sql);
		
	}


	private static void del(Connection con, String table) throws SQLException {
		String answer, sql;
		char string2char;
		Scanner sc = getScanner();
		Statement st = con.createStatement();
		System.out.print("Do you wish to delete all table entries?(Y/N) \n");
		answer = sc.next();
		string2char = answer.charAt(0);
		if (Character.toUpperCase(string2char) == 'Y'){
			sql = "DELETE FROM " + table + ";";
			executeSQL(st, sql);
			System.out.println("Returning to main menu...");
		}else{
			do{
				System.out.println("Please enter the column and its in this format \nColumn1=value1");
				sql = "DELETE FROM "+table+" WHERE "+sc.next()+";";
				executeSQL(st, sql);
				System.out.print("Do you wish to delete more entries?(Y/N) \n");
				answer = sc.next();
				string2char = answer.charAt(0);
			
			}while(Character.toUpperCase(string2char) == 'Y');
			
		}
	}


	private static void add(Connection con, String table) throws SQLException {
		String answer, sql;
		StringBuilder sqlBuilder = new StringBuilder();
		char string2char;
		Scanner sc = getScanner();
		Statement st = con.createStatement();
		do{
			System.out.println("Please enter the columns being added in the new row. \ncolumn1, column2, column3");
			
			sqlBuilder.append("INSERT INTO "+table+" (");
			sqlBuilder.append(sc.nextLine());
			sqlBuilder.append(") VALUES (");
			
			System.out.println("Please enter the values begin added in the new row. \nvalue1, value2, value3");
			
			sqlBuilder.append(sc.nextLine());
			sqlBuilder.append(");");
			
			sql = sqlBuilder.toString();
			executeSQL(st, sql);
			
			System.out.print("Do you wish to add more entries?(Y/N) \n");
			answer = sc.next();
			string2char = answer.charAt(0);
			
		}while(Character.toUpperCase(string2char) == 'Y');
			
	}


	private static void List(Connection con, String table) throws SQLException {
		Statement st = con.createStatement();
		DatabaseMetaData md = con.getMetaData();
		ResultSet rs = md.getColumns(null, null, table, null);
		while (rs.next()) {
			System.out.print(rs.getString(4)+"\t\t\t\t");
		}
		System.out.print("\n\n");
		String query = "SELECT * FROM "+table+";";
		rs = st.executeQuery(query);
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		while (rs.next()) {
			//Print one row          
			for(int i = 1 ; i <= columnsNumber; i++){
		
				System.out.print(rs.getString(i) + "\t\t"); //Print one element of a row
			}
			System.out.println();//Move to the next line to print the next row.
		}
		
		System.out.println("\n");
		
	}
	
	private static void join (Connection con, String table) throws SQLException{
		String displayTableColumns;
		DatabaseMetaData md = con.getMetaData();
		ResultSet rs = md.getTables(null, null, "%", null);
		while (rs.next()) {
			if(!(rs.getString(3).equals(table)))
				System.out.print(rs.getString(3) + "\t");
		}
		System.out.print("\n\n");
		Scanner sc = getScanner();
		System.out.print("Please select which table you wish to join to "+table+" table:");
		String table2 = sc.nextLine();
		while(table.equals(table2)){
			System.out.println("This table has already been selected");
			System.out.print("Please select which table you wish to join to "+table+" table:");
			table2 = sc.nextLine();
		}
		System.out.println("\nColumns in "+ table + "...\n");
		rs = md.getColumns(null, null, table, null);
		while (rs.next()){
			System.out.print(rs.getString(4) + "\t");
		}
		System.out.print("\n\n");
		System.out.println("\nColumns in "+ table2 + "...\n");
		rs = md.getColumns(null, null, table2, null);
		while (rs.next()){
			System.out.print(rs.getString(4) + "\t");
		}
		System.out.print("\n\n");
		System.out.print("Please ensure that colums names are in the same order as table order.\n");
		System.out.print("Please select which columns you wish to join between table "+table+" and "+table2+":\n");
		String column = sc.next();
		String column2 = sc.nextLine();
		System.out.println("\n");
		
		System.out.print("Please enter in TABLENAME.TABLECOLUMN formate\n");
		System.out.println("Please select which columns you wish to be joined:");
		displayTableColumns = sc.nextLine();
		Statement st = con.createStatement();
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("SELECT "+displayTableColumns+" FROM "+table+" INNER JOIN "+table2+" ON "+ table+ "."+column+"="+table2+"."+column2+";");
		String sql = sqlBuilder.toString();
		ResultSet display = st.executeQuery(sql);
		while(display.next()){
			String display1 = display.getString(1);
			String display2 = display.getString(2);
			
			System.out.println(display1 +" "+display2);
		}
		
		
	}
	
	
	private static void singleTableOptions(Connection con, Scanner sc, String table) throws SQLException{
		int operator = 0;
		String answer;
		char string2char;
		System.out.println("\nColumns in "+ table + "\n");
		DatabaseMetaData md = con.getMetaData();
		ResultSet rs = md.getColumns(null, null, table, null);
		while (rs.next()) {
		  System.out.print(rs.getString(4)+"\t");
		}
		System.out.print("\n\n");
		do{
			System.out.print("What would you like to do? \n\t 1. View \n\t 2. Add \n\t 3. Delete \n\t 4. Update \n\t 5. Change Tables/Exit \n\t 6. Joining Tables\n");
			operator = sc.nextInt();
			switch(operator){
				case 1:
					List(con, table);
					break;
				case 2:
					add(con, table);
					break;
				case 3:
					del(con, table);
					break;
				case 4:
					update(con, table);
					break;
				case 5:
					break;
				case 6:
					join(con, table);
					break;
			}
			if(operator==5)
				break;
			System.out.println("Do you wish to continue using this table? (Y/N)");
			answer = sc.next();
			string2char = answer.charAt(0);
			
		}while(Character.toUpperCase(string2char) == 'Y');
	}
}
