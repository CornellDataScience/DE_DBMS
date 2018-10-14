package Project2.Operators.Logical;

import Project2.Operators.Physical.PhysicalOperator;
import Project2.Visitors.LogicalOperatorVisitor;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;

/**
 * Class for a logical full table scan operator to return all tuples
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class LogicalScanOperator extends LogicalOperator {

	private Table table;
	private ArrayList<String> cols;

	/**
	 * Creates a logical scan operator for the given table
	 *
	 * @param t the table to scan
	 */
	public LogicalScanOperator(Table t, ArrayList<String> cols) {
		table = t;
		this.cols = cols;
	}

	@Override
	public Table getTable() {
		return table;
	}

	@Override
	public ArrayList<String> getColumns() {
		return cols;
	}

	@Override
	public PhysicalOperator accept(LogicalOperatorVisitor visitor, int i, boolean b) {
		return visitor.visit(this, i, b);
	}

	@Override
	public String toString() {
//		return getClass().getSimpleName();
		return "Scan[" + table.getName() + "]";
	}

	public String toStringLeaf(int printDashNum) {
		String result = "";
		for (int i = 0; i < printDashNum; i++){
			result += "-";
		}
		result += "Leaf[" + table.getName() + "]";
		return result;
	}
}
