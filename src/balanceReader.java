import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;

public class balanceReader {
	private String columnName;
	public static void main(String[] args) throws Exception {
	try {
		Connection conn = SqlConnection.getConnection("jdbc:mysql://localhost:3306/stocks");
		Statement statement = conn.createStatement();
		String createTable = "CREATE TABLE IF NOT EXISTS BalanceSheet (BalanceSheet_id INT PRIMARY KEY NOT NULL AUTO_INCREMENT)";
		String addColumns = "ALTER TABLE BalanceSheet ADD COLUMN companies_id INT, "
						+ "ADD COLUMN Companies VARCHAR(100), "
						+ "ADD COLUMN ticker VARCHAR(20), " 
						+ "ADD COLUMN Year VARCHAR(100), "
						+ "ADD COLUMN Shares_Outstanding VARCHAR(100), "
						+ "ADD COLUMN Cash VARCHAR(100), "
						+ "ADD COLUMN Marketable_Securities VARCHAR(100), "
						+ "ADD COLUMN Receivables VARCHAR(100), "
						+ "ADD COLUMN Inventory VARCHAR(100), "
						+ "ADD COLUMN Raw_Materials VARCHAR(100), "
						+ "ADD COLUMN Work_In_Progress VARCHAR(100), "
						+ "ADD COLUMN Finished_Goods VARCHAR(100), "
						+ "ADD COLUMN Notes_Receivable VARCHAR(100), "
						+ "ADD COLUMN Other_Current_Assets VARCHAR(100), "
						+ "ADD COLUMN Total_Current_Assets VARCHAR(100), "
						+ "ADD COLUMN Property_Plant_And_Equipment VARCHAR(100), "
						+ "ADD COLUMN Accumulated_Depreciation VARCHAR(100), "
						+ "ADD COLUMN Net_Property_Plant_And_Equipment VARCHAR(100), "
						+ "ADD COLUMN Investment_And_Advances VARCHAR(100), "
						+ "ADD COLUMN Other_Non_Current_Assets VARCHAR(100), "
						+ "ADD COLUMN Deferred_Charges VARCHAR(100), "
						+ "ADD COLUMN Intangibles VARCHAR(100), "
						+ "ADD COLUMN Deposits_And_Other_Assets VARCHAR(100), "
						+ "ADD COLUMN Total_Assets VARCHAR(100), "
						+ "ADD COLUMN Notes_Payable VARCHAR(100), "
						+ "ADD COLUMN Accounts_Payable VARCHAR(100), "
						+ "ADD COLUMN Current_Portion_Of_Long_Term_Debt VARCHAR(100), "
						+ "ADD COLUMN Current_Portion_Of_Capital_Leases VARCHAR(100), "
						+ "ADD COLUMN Accrued_Expenses VARCHAR(100), "
						+ "ADD COLUMN Income_Taxes_Payable VARCHAR(100), "
						+ "ADD COLUMN Other_Current_Liabilities VARCHAR(100), "
						+ "ADD COLUMN Total_Current_Liabilities VARCHAR(100), "
						+ "ADD COLUMN Mortgages VARCHAR(100), "
						+ "ADD COLUMN Deferred_Charges_Taxes_Income VARCHAR(100), "
						+ "ADD COLUMN Convertible_Debt VARCHAR(100), "
						+ "ADD COLUMN Long_Term_Debt VARCHAR(100), "
						+ "ADD COLUMN Non_Current_Capital_Leases VARCHAR(100), "
						+ "ADD COLUMN Other_Long_Term_Liabilities VARCHAR(100), "
						+ "ADD COLUMN Total_Liabilities VARCHAR(100), "
						+ "ADD COLUMN Minority_Interest VARCHAR(100), "
						+ "ADD COLUMN Preferred_Stock VARCHAR(100), "
						+ "ADD COLUMN Common_Stock_Net VARCHAR(100), "
						+ "ADD COLUMN Capital_Surplus VARCHAR(100), "
						+ "ADD COLUMN Retained_Earnings VARCHAR(100), "
						+ "ADD COLUMN Treasury_Stock VARCHAR(100), "
						+ "ADD COLUMN Other_Liabilities VARCHAR(100), "
						+ "ADD COLUMN Shareholders_Equity VARCHAR(100), "
						+ "ADD COLUMN Total_Liabilities_And_Shareholders_Equity VARCHAR(100), "
						+ "ADD COLUMN update_Date VARCHAR(100)";
						
		statement.executeUpdate(createTable);
		statement.executeUpdate(addColumns);
		
	} catch (Exception e) {
		System.out.println(e);
		return;
	} finally {
	
	
	Connection conn = SqlConnection.getConnection("jdbc:mysql://localhost:3306/stocks");
	Statement statement = conn.createStatement();
		
		File path = new File("/Users/Zack/Desktop/JavaDB/BALANCESHEETS");
		int counter = 0;
		for(File file: path.listFiles()) {
			
			if (file.isFile()) {
				
				String fileName = file.getName();
				String ticker = fileName.split("\\_")[0];
				if (ticker.equals("ASB") || ticker.equals("FRC")) {
					if (ticker.equals("ASB")) {
						ticker = ticker + "PRD";
					}
					if (ticker.equals("FRC")) {
						ticker = ticker + "PRD";
					}
				}
				
				for (int z = 0; z < 10; z++) {
				String writeCompanyValues = "INSERT INTO BalanceSheet (companies_id, Companies, ticker) "
	    				+ "SELECT companies.companies_id, companies.Companies, companies.ticker FROM companies WHERE companies.ticker = '" + ticker + "'";
				statement.executeUpdate(writeCompanyValues);
				}
		       
				Reader reader = new BufferedReader(new FileReader(file));
	            StringBuilder builder = new StringBuilder();
	            
		        int c;
		    	while ((c = reader.read()) != -1) {
		    	builder.append((char) c);
		    	}
		    	
		    	String string = builder.toString();
		    	
		    	ArrayList<String> stringResult = new ArrayList<String>();
		    	
		    	if (string != null) {
		    		String[] splitData = string.split("\\s*,\\s*|\\n");
		    		for (int i = 0; i <splitData.length; i++) {
		    			if (!(splitData[i] == null) || !(splitData[i].length() ==0)) {
		    				stringResult.add(splitData[i].trim());
		    			}
		    		}
		    	}
		    	
		    	
		    	
		    	String columnName = null;
		    	int yearCount = 0;
		    	for (int i = 0; i < stringResult.size(); i++) {
		    		int sL = stringResult.get(i).length();
		    		
		    		
		    		for (int x = 0; x < sL; x++) {
		    			if (Character.isLetter(stringResult.get(i).charAt(x))) {
		    				yearCount = 0;
		    				System.out.println("index: " + i);
		    				columnName = stringResult.get(i);
		    				columnName = columnName.replace(" ", "_");
		    				System.out.println(columnName);
		    				i++;
		    				break;
		    			}
		    		}
		    		yearCount++;
    				String value = stringResult.get(i);
    				System.out.println("Year: " + stringResult.get(yearCount) + " Value: " + value + " Stock: " + ticker + " Column: " + columnName );
    				if (!(columnName == null)) {
					String writeValues3 = "UPDATE BalanceSheet "
								+ "SET Year = '" + stringResult.get(yearCount) + "' "
								+ "WHERE BalanceSheet_id = '" + (yearCount+counter) + "'";
					String writeValues4 = "UPDATE BalanceSheet " 
										+ "SET " + columnName + " = '" + value + "' "
										+ "WHERE Year = '" + stringResult.get(yearCount) + "' AND ticker = '" + ticker + "'";
					statement.executeUpdate(writeValues3);
					statement.executeUpdate(writeValues4);
					
    				}
		    	}
				
				
		    counter = counter + 10;
			}
		}
	}
	}
}
