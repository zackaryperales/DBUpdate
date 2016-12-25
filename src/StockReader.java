import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

public class StockReader {

	public static void main(String[] args) throws Exception {
		URL url = new URL("http://finance.yahoo.com/d/quotes.csv?s=AAPL+GOOGL+GOOG+MSFT+BRKB+BRKA+AMZN+XOM+FB+JNJ+JPM+GE+WFC"
				   + "+T+BABA+PG+CHL+WMT+RDS-B+BAC+CVX+VZ+RDS-A+BUD+PFE+KO+TM+MRK+INTC+CMCSA+ORCL+NVS+V+C+HD+HSBC+DIS+IBM+TSM"
				   + "+ASB+CSCO+PEP+UNH+PM+MO+PTR+FRC+UL+UN+MA&f=snd1l1yr");
	HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
	InputStream inputStream = new BufferedInputStream(urlConnection.getInputStream());
	
	Reader reader = new InputStreamReader(inputStream);
	
	Statement st;
	ResultSet rs;

	StringBuilder builder = new StringBuilder();
	int c;
	while ((c = reader.read()) != -1) {
		System.out.print((char) c);
		builder.append((char) c);
	}
	
	String string = builder.toString();
	
	ArrayList<String> stringResult = new ArrayList<String>();
	
	if (string != null) {
		String[] splitData = string.split("\\s*,\\s*");
		for (int i = 0; i <splitData.length; i++) {
			if (!(splitData[i] == null) || !(splitData[i].length() ==0)) {
				stringResult.add(splitData[i].trim());
			}
		}
	}
	
	String timeStamp = new SimpleDateFormat("M/d/yyyy").format(new Date());
	System.out.println(timeStamp);
	DateFormat dateFormat = new SimpleDateFormat("M/d/yyyy");
	Calendar cal = Calendar.getInstance();
	cal.add(Calendar.DATE, -1);
	String yesterday = dateFormat.format(cal.getTime());
	System.out.println(yesterday);
	int count = 0;
	for (int i = 0; i < stringResult.size(); i++) {
		System.out.println(stringResult.get(i));
		if (stringResult.get(i).contains(timeStamp) || stringResult.get(i).contains(yesterday)) {
			i++;
			String stock = stringResult.get(i);
			System.out.println("Stock value: " + stock);
			count++;
			double stockV = Double.parseDouble(stock);
			
			Connection conn = null;
			conn = SqlConnection.getConnection("jdbc:mysql://localhost:3306/stocks");
			st = conn.createStatement();
			String sql = "SELECT * FROM stocks.stockvalue";
			rs = st.executeQuery(sql);
			ResultSetMetaData metaData = rs.getMetaData();
			int rowCount = metaData.getColumnCount();
			
			boolean isMyColumnPresent = false;
			String myColumnName = timeStamp;
			myColumnName = myColumnName.replace("/", "_");
			
			for (int z = 1; z <= rowCount; z++) {
				if (myColumnName.equals(metaData.getColumnName(z))) {
					isMyColumnPresent = true;
				}
			}
			
			if(!isMyColumnPresent) {
				String myColumnType = "DOUBLE";
				st.executeUpdate("ALTER TABLE stocks.stockvalue ADD stockvalue." + myColumnName + " " + myColumnType);
			}
			
			String updateTableSQL = ("UPDATE stocks.stockvalue SET stockvalue." + myColumnName + " = ? WHERE stockvalue.stockvalue_id = " + count);
			PreparedStatement preparedStatement = conn.prepareStatement(updateTableSQL);
			preparedStatement.setDouble(1, stockV);
			preparedStatement.executeUpdate();
			System.out.println("Done");
			preparedStatement.close();
			conn.close();
			
			
			
			
			
			
		}
	}
	}
}

