package operator;

import DBSystem.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import DBSystem.Tuple;
import DBSystem.dbCatalog;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;

public class Select extends Operator {

	/**
	 * Select Operator is called when WHERE keyword is called in the SQL Language. 
	 */

	Table table;
	Table origTable;
	Expression where;


	public Select(Operator scanOp, Expression where) {
		if(!(scanOp instanceof ScanOperator)) {
			throw new IllegalArgumentException("Must pass in Scan Operator");
		}
		ScanOperator scan = (ScanOperator)scanOp;
		table = scan.getTable();
		this.where = where;		
		
	}
	public Select(String tableName, Expression where) {
		
		table = dbCatalog.getTable(tableName);
		this.where = where;		
		
	}

	@Override
	public Table operate() {
		ExpressionVisitorClass ex = new ExpressionVisitorClass(where, table);
		List<Integer> selectedIndices = ex.operate();
		return table.getRows(selectedIndices);

		
	}

	@Override
	public void reset() {
		table = origTable;
	}
}