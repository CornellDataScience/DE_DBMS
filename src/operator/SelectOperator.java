package operator;

import DBSystem.Table;

import java.util.Set;

import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends Operator {

	/**
	 * Select Operator is called when WHERE keyword is called in the SQL Language.
	 */

	Table table;
	Table origTable;
	Expression where;

	public SelectOperator(Operator scanOp, Expression where) {
		table = scanOp.operate();
		this.where = where;
	}

	public Table operate() {
		if (where == null) {
			// no select requirement
			return table;
		} else {
			ExpressionVisitorClass ex = new ExpressionVisitorClass(where, table);
			where.accept(ex);
			Set<Integer> selectedIndices = ex.evaluate();
			return table.getRows(selectedIndices);
		}
	}

	@Override
	public void reset() {
		table = origTable;
	}
}