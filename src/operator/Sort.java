package operator;

import java.util.ArrayList;
import java.util.List;

import DBSystem.ColumnTab;
import DBSystem.Table;

public class Sort extends Operator {

	Table table;
	List ElementsToSort;

	public Sort(Operator op, List ElementsToSort) {
		table = op.operate();
		this.ElementsToSort = ElementsToSort;
	}
	
	@Override
	public Table operate() {
		if (ElementsToSort == null) {
			return table;
		}
		// sort the table according to colName, and output the table
		// Now we can only sort one element in the table
		String colName = ElementsToSort.get(0).toString().split("\\.")[1];
		ColumnTab columnToBeSorted = table.getCol(colName);
		int[] indices = columnToBeSorted.sortedIndices();
		List<ColumnTab> cols = new ArrayList<ColumnTab>();
		for (ColumnTab c : table.getColumns()) {
			ColumnTab new_c = c.clone();
			new_c.sortWithIndices(indices);
			cols.add(new_c);
		}
		return new Table("sortedTable", cols);
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
	}
	
}
