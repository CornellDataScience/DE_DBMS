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

import operator.*;

public class Parser {
	private static String queriesFile = "";

	public static void main(String[] args) {
		int queryNumber = 0;
		try {
			queriesFile = args[0] + "/queries.sql";
			dbCatalog.catalog = new dbCatalog(args[0]);
			ArrayList<Table> tables = dbCatalog.getInstance();
			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
			Statement statement;
			while ((statement = parser.Statement()) != null) {
				try {
					queryNumber++;
					System.out.println("Read statement: " + statement);
					Select select = (Select) statement;
					PlainSelect s = (PlainSelect) select.getSelectBody();
					
					// there is no table to join
					if (s.getJoins() == null) {
						String tableName;
						if (s.getFromItem().getAlias() != null)
							tableName = dbCatalog.setAlias(s.getFromItem());
						else
							tableName = s.getFromItem().toString();
						Operator ScanOp = new ScanOperator(tableName);
						SelectOperator selOp = new SelectOperator(ScanOp, s.getWhere());
						ProjectOperator projOp = new ProjectOperator(selOp, s.getSelectItems());
						SortOperator sortOp = new SortOperator(projOp, s.getOrderByElements());
						DuplicateEliminationOperator distOp = new DuplicateEliminationOperator(sortOp, s.getDistinct(), args[1], queryNumber);
						distOp.dump();
					} 
					//TODO there is table to join
					
					
					
					
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
