package Project2.Operators.Physical;

import Project2.Tuple.Tuple;
import Project2.Tuple.TupleBinaryReader;
import Project2.Tuple.TupleWriter;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;

import static Project2.DBCatalog.*;

/**
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class PhysicalExternalSortOperator extends PhysicalSortOperator {

    public PhysicalOperator operator;
    private ArrayList<String> columns;
    private int tuplesPerRun;
    private int currIndex;
    private int colNum;
    private TupleComp tpCmp;
    private ArrayList<TupleBinaryReader> finalReaders;
    private int finalReadCounter = 0;
    private boolean loaded = false;
    public String tmpDirectory, sortName;

    public PhysicalExternalSortOperator(PhysicalOperator op, ArrayList<String> cols, ArrayList<String> sort) {
        operator = op;
        columns = cols;
        sortName = sort.get(0).toString();
        // keeps cols parameter from being altered
        cols = new ArrayList<>(cols);
        cols.removeAll(sort);
        sort.addAll(cols);
        tpCmp = new TupleComp(sort);

        tuplesPerRun = getPageSize(columns.size()) * getBufferPages();

        currIndex = 0;
    }

    @Override
    public Tuple getNextTuple() throws IOException {
        if (!loaded) {
            SecureRandom rand = new SecureRandom();
            String id = Integer.toString(rand.nextInt(Integer.MAX_VALUE));
            tmpDirectory = getTmp() + File.separator + id + File.separator;
            File dir = new File(tmpDirectory);
            dir.mkdir();

            ArrayList<Tuple> tuples = new ArrayList<>(tuplesPerRun);
            int i = 0;
            while (true) {
                tuples.clear();
                Tuple tuple;
                int newCount = tuplesPerRun;
                while (newCount-- > 0 && (tuple = operator.getNextTuple()) != null) {
                    tuples.add(tuple);
                }
                if (tuples.isEmpty())
                    break;
                colNum = tuples.get(0).getColNum();
                tuples.sort(tpCmp);
                TupleWriter tw = new TupleWriter(genFileName(0, i));
                tw.writeAll(tuples);
                tw.end(colNum);
                i++;
                if (tuples.size() < tuplesPerRun)
                    break;
            }

            if (i == 1) {
                finalReaders = new ArrayList<>();
                finalReaders.add(new TupleBinaryReader(genFileName(0, 0)));
            } else if (i != 0) {
                merge(i);
            }
            loaded = true;
        }

        if (finalReaders == null || finalReaders.size() == 0)
            return null;
        ArrayList<Integer> tup = finalReaders.get(finalReadCounter).read();
        currIndex++;
        if (tup == null) {
            finalReaders.get(finalReadCounter).close();
            finalReadCounter++;
            if (finalReadCounter == finalReaders.size()) {
                finalReadCounter--;
                return null;
            }
            tup = finalReaders.get(finalReadCounter).read();
            currIndex = 1;
            if (tup == null) {
                return null;
            }
        }
        return new Tuple(tup);
    }

    private void merge(int i) throws IOException {
        int currPage = -1;
        PriorityQueue<Tuple> pq = new PriorityQueue<>(tuplesPerRun, tpCmp);
        int pass = 0;

        // adds all tr to array list
        ArrayList<Queue<TupleBinaryReader>> tupleReaders = new ArrayList<>();
        while ((currPage ++) < i - 1) { // check
            TupleBinaryReader tr = new TupleBinaryReader(genFileName(pass, currPage));
            Queue<TupleBinaryReader> trs = new ArrayDeque<>();
            trs.add(tr);
            tupleReaders.add(trs);
        }

        while (tupleReaders.size() > tuplesPerRun) {
            currPage = 0;
            pass ++;
            for (int x = 0; x < tupleReaders.size() && x < tuplesPerRun; x ++) {
                List<Queue<TupleBinaryReader>> miniTRs = new ArrayList<>();
                int count = 0;
                int size = tupleReaders.size();
                while (tupleReaders.listIterator().hasNext()) {
                    if (count >= x && count < Math.min(x + tuplesPerRun, size)) {
                        miniTRs.add(tupleReaders.remove(x));
                    } else if (count == Math.min(x + tuplesPerRun, size)) {
                        break;
                    }
                    count ++;
                }

                // add first tuple to PQ and set tr for tuple
                int currTr = 0;
                outerloop1:
                for (int y = 0; y < tuplesPerRun; y++) {
                    Queue<TupleBinaryReader> queue = miniTRs.get(currTr);
                    TupleBinaryReader tr = queue.peek();
                    ArrayList<Integer> tup = tr.read();

                    while (tup == null) { // if tr is empty
                        if (queue.size() <= 1) {
                            if (queue.size() == 1) {
                                queue.poll().close();
                                miniTRs.remove(queue);
                            }
                            if (miniTRs.isEmpty()) {
                                break outerloop1;
                            }

                            break;
                        } else {
                            queue.poll().close();
                            tr = queue.peek();
                        }

                        tup = tr.read();
                    }

                    if (tup != null) {
                        Tuple tuple = new Tuple(tup);
                        tuple.queue = queue;
                        tuple.tr = tr;
                        pq.add(tuple);
                    }

                    currTr++;
                    if (currTr >= miniTRs.size())
                        currTr = 0;
                }

                TupleWriter tw = new TupleWriter(genFileName(pass, currPage));
                Queue<TupleBinaryReader> newMiniTRs = new ArrayDeque<>();
                newMiniTRs.add(new TupleBinaryReader(genFileName(pass, currPage)));
                int tupleNum = 0;
                currTr = 0;
                while (!pq.isEmpty()) {
                    // gets from PQ and writes to file
                    Tuple newTup = pq.poll();
                    tw.write(newTup.getAllCols());
                    tupleNum++;
                    if (tupleNum == tuplesPerRun) { // if file is done close and get new one
                        tw.end(colNum);
                        currPage++;
                        tupleNum = 0;
                        tw = new TupleWriter(genFileName(pass, currPage));
                        newMiniTRs.add(new TupleBinaryReader(genFileName(pass, currPage)));
                    }

                    // get value to replace the pop from PQ
                    TupleBinaryReader tr = newTup.tr;
                    Queue<TupleBinaryReader> queue = newTup.queue;
                    ArrayList<Integer> tup = tr.read();
                    while (tup == null) { // if tr is empty
                        if (queue.size() <= 1) {
                            if (queue.size() == 1) {
                                queue.poll().close();
                                miniTRs.remove(queue);
                            }
                            if (miniTRs.isEmpty()) {
                                break;
                            }

                            if (currTr >= miniTRs.size())
                                currTr = 0;
                            queue = miniTRs.get(currTr);
                            currTr++;
                            tr = queue.peek();
                        } else {
                            queue.poll().close();
                            tr = queue.peek();
                        }

                        tup = tr.read();
                    }

                    if (tup != null) { // add new tuple to PQ and set its tr
                        Tuple tuple = new Tuple(tup);
                        tuple.tr = tr;
                        tuple.queue = queue;
                        pq.add(tuple);
                    }
                }
                tw.end(colNum);
                tupleReaders.add(x, newMiniTRs);
            }
        }

        // add first tuple to PQ and set tr for tuple
        currPage = 0;
        int currTr = 0;
        outerloop2:
        for (int y = 0; y < tuplesPerRun; y++) {
            Queue<TupleBinaryReader> queue = tupleReaders.get(currTr);
            TupleBinaryReader tr = queue.peek();
            ArrayList<Integer> tup = tr.read();

            while (tup == null) { // if tr is empty
                if (queue.size() <= 1) {
                    if (queue.size() == 1) {
                        queue.poll().close();
                        tupleReaders.remove(queue);
                    }
                    if (tupleReaders.isEmpty()) {
                        break outerloop2;
                    }

                    break;
                } else {
                    queue.poll().close();
                    tr = queue.peek();
                }

                tup = tr.read();
            }

            if (tup != null) {
                Tuple tuple = new Tuple(tup);
                tuple.queue = queue;
                tuple.tr = tr;
                pq.add(tuple);
            }

            currTr++;
            if (currTr >= tupleReaders.size())
                currTr = 0;
        }

        TupleWriter tw = new TupleWriter(tmpDirectory + currPage);
        finalReaders = new ArrayList<>();
        finalReaders.add(new TupleBinaryReader(tmpDirectory + currPage));
        int tupleNum = 0;
        currTr = 0;
        while (!pq.isEmpty()) {
            // gets from PQ and writes to file
            Tuple newTup = pq.poll();
            tw.write(newTup.getAllCols());
            tupleNum++;
            if (tupleNum == tuplesPerRun) { // if file is done close and get new one
                tw.end(colNum);
                currPage++;
                tupleNum = 0;
                tw = new TupleWriter(tmpDirectory + currPage);
                finalReaders.add(new TupleBinaryReader(tmpDirectory + currPage));

            }

            // get value to replace the pop from PQ
            TupleBinaryReader tr = newTup.tr;
            Queue<TupleBinaryReader> queue = newTup.queue;
            ArrayList<Integer> tup = tr.read();
            while (tup == null) { // if tr is empty
                if (queue.size() <= 1) {
                    if (queue.size() == 1) {
                        queue.poll().close();
                        tupleReaders.remove(queue);
                    }
                    if (tupleReaders.isEmpty()) {
                        break;
                    }

                    if (currTr >= tupleReaders.size())
                        currTr = 0;
                    queue = tupleReaders.get(currTr);
                    currTr++;
                    tr = queue.peek();
                } else {
                    queue.poll().close();
                    tr = queue.peek();
                }

                tup = tr.read();
            }

            if (tup != null) { // add new tuple to PQ and set its tr
                Tuple tuple = new Tuple(tup);
                tuple.tr = tr;
                tuple.queue = queue;
                pq.add(tuple);
            }
        }
        tw.end(colNum);
    }

    private String genFileName (int pass, int run) {
        return tmpDirectory + String.valueOf(pass) + "_" + String.valueOf(run);
    }

    @Override
    public void reset() throws IOException {
        if (finalReaders != null) {
            finalReadCounter = 0;
            for (int i = 0; i < finalReaders.size(); i++)
                finalReaders.set(i, new TupleBinaryReader(tmpDirectory + finalReaders.get(i).getFileName())).close();
        }
    }

    @Override
    public void reset(int numTuples) throws IOException {
       TupleBinaryReader tr = finalReaders.get(finalReadCounter);
       finalReaders.set(finalReadCounter, new TupleBinaryReader(tmpDirectory + tr.getFileName())).close();

       if (numTuples < currIndex) {
          for (int i = 0; i < currIndex - numTuples - 1; i++) {
             finalReaders.get(finalReadCounter).read();
          }
          currIndex = currIndex - numTuples - 1;
       } else {
          finalReadCounter--;
          int tuplesBack = numTuples - currIndex;
          int pagesBack = tuplesBack / tuplesPerRun;
          for (int i = 0; i < pagesBack; i++) {
             finalReaders.set(finalReadCounter, new TupleBinaryReader(tmpDirectory + tr.getFileName())).close();
             finalReadCounter--;
          }
          int extraTuplesBack = tuplesBack % tuplesPerRun;
          if (extraTuplesBack > 0) {
             finalReaders.set(finalReadCounter, new TupleBinaryReader(tmpDirectory + tr.getFileName())).close();
             for (int i = 0; i < tuplesPerRun - extraTuplesBack; i++)
                finalReaders.get(finalReadCounter).read();
          }
          currIndex = tuplesPerRun - extraTuplesBack;
       }
    }

    public class TupleComp implements Comparator<Tuple> {
        private ArrayList<String> sortBy = null;

        public int compare(Tuple o1, Tuple o2) {
            for (String sortByCol : sortBy) {
                if (o1.getColValue(columns.indexOf(sortByCol)) > o2.getColValue(columns.indexOf
                      (sortByCol))) {
                    return 1;
                } else if (o1.getColValue(columns.indexOf(sortByCol)) < o2.getColValue(columns
                      .indexOf(sortByCol)))
                    return -1;
            }
            return 0;
        }

        public TupleComp(ArrayList<String> sortBy) {
            this.sortBy = sortBy;
        }
    }

    @Override
    public ArrayList<String> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        return "ExternalSort[" + sortName + "]";
    }
}