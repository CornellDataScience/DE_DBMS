package operator;

import DBSystem.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import DBSystem.ColumnTab;
import DBSystem.Tuple;
import DBSystem.dbCatalog;
import net.sf.jsqlparser.expression.Expression;

public class JoinOperator extends Operator {

	/**
	 * Join Operator is called when JOIN keyword is called in the SQL Language. 
	 */

	List<Table> tables;
	List<Table> origTables;
	String tableName;
	List<String> colNames;



	public JoinOperator(List<String> tableNames, List<String> colNames, Expression join) {
		tables = new ArrayList<Table>();
		origTables = new ArrayList<Table>();
		for(String s: tableNames) {
			tables.add(dbCatalog.getTable(s));
		}
		origTables = tables;
		this.colNames = colNames;

	}

	@Override
	public Table operate() {
		//Converting list of column names to columns 
		List<ColumnTab> cols = new ArrayList<ColumnTab>();
		for(int i = 0; i < tables.size(); i ++) {
			cols.add(tables.get(i).getCol(colNames.get(i)));
		}
		//Creating hashmaps for each table in tables to use in hash join
		for(int i = 0; i < cols.size(); i ++) {
			HashMap<Integer,elem> map = new HashMap<Integer, elem>();
			for(int j = 0; j < cols.get(i).getSize(); j ++) {
				//Put hash mapped to elem instance of object and its index
				Object currentElem = cols.get(i).getData(j);
				map.put(currentElem.hashCode(), new elem(j,currentElem));
			}
		}
		return new Table(tableName, cols);
	}

	@Override
	public void reset() {
		tables = origTables;
	}
	private class elem {
		private Object o;
		private int index;

		public elem(int index, Object o) {
			this.o = o;
			this.index = index;
		}
	}
}