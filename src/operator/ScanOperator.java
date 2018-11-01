package operator;

import DBSystem.Table;
import DBSystem.Column;
import DBSystem.Tuple;
import DBSystem.dbCatalog;

public class ScanOperator extends Operator {

	/**
	 * ScanOperator which is the basic component in the operator tree. The
	 * operator tree we constructed should have scan operator as its leaves
	 * 
	 * @author Akhil Gopu, Rong Tan
	 */

	Table table;

	public ScanOperator(String tableName) {
		table = dbCatalog.getTable(tableName);
	}

	@Override
	public Tuple getNextTuple() {
		// TODO
		return null;
	}

	@Override
	public void reset() {
	}

}