package operator;

import DBSystem.Table;

import java.util.ArrayList;
import java.util.List;

import DBSystem.Column;
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
	List<Column> cols;
	
	

	public Project(String tableName, List<String> colNames) {
		table = dbCatalog.getTable(tableName);
		this.tableName = tableName;
		origTable = table;
		List<Column> cols = new ArrayList<Column>();
		for(String s: colNames) {
			cols.add(table.getCol(s));
		}
		this.cols = cols;
	}

	@Override
	public Table operate() {		
		return new Table(tableName, cols);
	}

	@Override
	public void reset() {
		table = origTable;
	}

	

}