package operator;

import DBSystem.Table;

import java.util.ArrayList;
import java.util.List;

import DBSystem.ColumnTab;
import DBSystem.Tuple;
import DBSystem.dbCatalog;

public class ProjectOperator extends Operator {

	/**
	 * Project Operator is used when the select keyword is called without 
	 * any selection criteria. Therefore, certain columns are projected and 
	 * returned as a new Table. 
	 */

	Table table;
	Table origTable;
	String tableName;
	List<ColumnTab> cols;
	List<Object> selectItems;
	
	public ProjectOperator(Operator op, List selectItems) {
		table = op.operate();
		this.selectItems = selectItems;		
	}

	public Table operate() {
		if (selectItems.toString().equals("[*]"))
			return table;
		List<ColumnTab> cols = new ArrayList<ColumnTab>();
		for(Object o : selectItems) {
			String s = o.toString().split("\\.")[1];
			// o.toString().split("\\.");
			cols.add(table.getCol(s));
		}
		this.cols = cols;
		return new Table(tableName, cols);
	}

	@Override
	public void reset() {
		table = origTable;
	}

	

}