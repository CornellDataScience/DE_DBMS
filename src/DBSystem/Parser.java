package DBSystem;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class Parser {
	private static String queriesFile = "";

	public static void main(String[] args) {
		try {
			queriesFile = args[0] + "/queries.sql";
			dbCatalog.catalog = new dbCatalog(args[0]);
			ArrayList<Table> tables = dbCatalog.getInstance();
			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
			Statement statement;
			while ((statement = parser.Statement()) != null) {
				try {
					System.out.println("Read statement: " + statement);
					Select select = (Select) statement;
					System.out.println("Select body is " + select.getSelectBody());
				} catch (Exception e) {
					System.err.println("Exception occurred during parsing a single sql query");
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}
