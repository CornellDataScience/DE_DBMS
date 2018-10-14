package Project2.Operators.Physical;

import Project2.Tuple.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class PhysicalSMJOperator extends PhysicalJoinOperator {
	// left and right operators are already sorted
	public PhysicalSortOperator leftOperator, rightOperator;
	private Tuple leftTp, rightTp;
	private TupleComp cp = null;
	private int partitionIndex = 0;  // the index of the first tuple in the current partition
	private int curRightIndex = 0;  // the index of the current tuple of left table

	/**
    * Creates a Physical SMJ Operator to combine two tables
    * @param left the left Physical Operator to sort and merge
    * @param right the right Physical Operator to sort and merge
	 * @param leftOrders the left columns which the tuples need to be compared
    * @param rightOrders the right columns which the tuples need to be compared
    */
	public PhysicalSMJOperator(PhysicalSortOperator left, PhysicalSortOperator right,
							   List<Integer> leftOrders, List<Integer> rightOrders) {
		rightOperator = right;
		leftOperator = left;
		cp = new TupleComp(leftOrders, rightOrders);
	}

	@Override
	public Tuple getNextTuple() throws IOException{

		if (leftTp == null && rightTp == null){
			leftTp = leftOperator.getNextTuple();
			rightTp = rightOperator.getNextTuple();
		}

		while (leftTp != null && rightTp != null) {
			if (cp.compare(leftTp, rightTp) < 0) {
				leftTp = leftOperator.getNextTuple();
				continue;
			}

			if (cp.compare(leftTp, rightTp) > 0) {
				rightTp = rightOperator.getNextTuple();
				curRightIndex++;
				partitionIndex = curRightIndex;
				continue;
			}
			Tuple rst = null;
			// create new tuple if the left and right tuples can be joined
			if (leftTp != null || rightTp != null)
				rst = new Tuple(leftTp, rightTp);

			rightTp = rightOperator.getNextTuple();
			curRightIndex++;

			if (rightTp == null || cp.compare(leftTp, rightTp) != 0) {
				leftTp = leftOperator.getNextTuple();
				rightOperator.reset(curRightIndex - partitionIndex);
				curRightIndex = partitionIndex;
				rightTp = rightOperator.getNextTuple();
			}

			if (rst != null) return rst;
		}

		return null;
	}

	@Override
	public ArrayList<String> getColumns() {
		ArrayList<String> ret = new ArrayList<>(leftOperator.getColumns());
		ret.addAll(rightOperator.getColumns());
		return ret;
	}

	@Override
	public void reset() throws IOException {
		leftOperator.reset();
		rightOperator.reset();
		leftTp = null;
		rightTp = null;
	}

	/*
	 * a public subclass for comparing tuples based on the two arraylist of columns
	 */
	public class TupleComp implements Comparator<Tuple> {
		List<Integer> leftOrders = null; // the order of attributes in left table
		List<Integer> rightOrders = null;// the order of attributes in right table
		@Override
		public int compare(Tuple left, Tuple right) {
			for(int i = 0; i< leftOrders.size();i++){
				int leftVal = left.getColValue(leftOrders.get(i));
				int rightVal = right.getColValue(rightOrders.get(i));
				int cmp = Integer.compare(leftVal, rightVal);
				if (cmp != 0) return cmp;
			}
			return 0;
		}

		public TupleComp(List<Integer> leftOrders, List<Integer> rightOrders){
			this.leftOrders = leftOrders;
			this.rightOrders = rightOrders;
		}
	}

	@Override
	public String toString() {
		String result = "SMJ[";
		if (leftOperator instanceof PhysicalExternalSortOperator){
			result += ((PhysicalExternalSortOperator)leftOperator).sortName + " = ";
		} else {	// InPlaceSort
			result += ((PhysicalInPlaceSortOperator)leftOperator).sortBy.get(0) + " = ";
		}
		if (rightOperator instanceof PhysicalExternalSortOperator){
			result += ((PhysicalExternalSortOperator)rightOperator).sortName + "]";
		} else {	// InPlaceSort
			result += ((PhysicalInPlaceSortOperator)rightOperator).sortBy.get(0) + "]";
		}
		return result;
	}
}
