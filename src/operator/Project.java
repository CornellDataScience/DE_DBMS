package operator;

import DBSystem.Table;

import java.util.ArrayList;
import java.util.List;

import DBSystem.ColumnTab;
import DBSystem.Tuple;
import DBSystem.dbCatalog;

public class Project extends Operator {

	/**
	 * Project Operator is used when the select keyword is called without 
	 * any selection criteria. Therefore, certain columns are projected and 
	 * returned as a new Table. 
	 */

	Table table;
	Table origTable;
	String tableName;
	List<ColumnTab> cols;
	List<String> colNames;
	
	

	public Project(String tableName, List<String> colNames) {
		table = dbCatalog.getTable(tableName);
		this.tableName = tableName;
		origTable = table;
		this.colNames = colNames;
		
	}

	@Override
	public Table operate() {
		List<ColumnTab> cols = new ArrayList<ColumnTab>();
		for(String s: colNames) {
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