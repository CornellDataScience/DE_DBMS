package Project2.Operators.Physical;

import Project2.DBCatalog;
import Project2.Tuple.Tuple;
import Project2.Tuple.TupleBinaryReader;
import Project2.btree.rid;
import net.sf.jsqlparser.schema.Table;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Class for a physical full table scan operator to return all tuples
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class PhysicalScanOperator extends PhysicalOperator {

	private TupleBinaryReader tupleBinaryReader;
	private Table table;
	private String fileName;

	/**
	 * Creates a physical scan operator to read all rows in table
	 *
	 * @param t the base table
	 */
	public PhysicalScanOperator(Table t) {
		table = t;
		fileName = DBCatalog.getFilename(t.getName());
	}

	/**
	 * Creates a physical scan operator to read all rows in table
	 *
	 * @param filename the filename for the base table
	 */
	public PhysicalScanOperator(String filename) {
		table = null;
		fileName = filename;
	}

	@Override
	public Tuple getNextTuple() throws IOException {
		if (tupleBinaryReader == null) {
			tupleBinaryReader = new TupleBinaryReader(fileName);
		}

		ArrayList<Integer> tupleValue = tupleBinaryReader.read();
		if (tupleValue == null) {
			tupleBinaryReader.close();
			return null;
		} else {
			return new Tuple(tupleValue);
		}
	}

	@Override
	public ArrayList<String> getColumns() {
		return DBCatalog.getColumns(table.getName(), table.getAlias());
	}

	@Override
	public void reset() throws IOException {
		if (tupleBinaryReader == null) {
			tupleBinaryReader = new TupleBinaryReader(fileName);
		}

		tupleBinaryReader.reset();
	}

	@Override
	public String toString() {
//		return getClass().getSimpleName();
		return "TableScan[" + table.getName() + "]";
	}

	/**
	 *	take a rid to get a tuple
	 */
	public Tuple getNextTuple(rid r) throws IOException {
		if (tupleBinaryReader == null) {
			tupleBinaryReader = new TupleBinaryReader(fileName);
		}

		ArrayList<Integer> tupleValue = tupleBinaryReader.read(r);
		if (tupleValue == null) {
			tupleBinaryReader.close();
			return null;
		} else {
			return new Tuple(tupleValue);
		}
	}

	/**
	 *	take a the table name in the scan operator
	 */
	public String getTableName(){
		return table.getName();
	}
}
