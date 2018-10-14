package Project2.Tuple;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import static Project2.DBCatalog.getPAGE_SIZE;

/**
 * Writes the tuple to a file
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class TupleWriter {
   private FileOutputStream outputStream;
   private FileChannel fileChannel;
   protected ByteBuffer buffer;
   protected int tupleCounter;
   protected int pageSize;

   /**
    * Empty constructor to inherit for testing purposes
    */
   protected TupleWriter() {
      pageSize = getPAGE_SIZE();
   }

   /**
    * Creates a new TupleWriter to an output file with the given file name
    *
    * @param fileName the file name to output to
    * @throws IOException if there is an issue writing to the output file
    */
   public TupleWriter(String fileName) throws IOException {
      outputStream = new FileOutputStream(fileName);
      fileChannel = outputStream.getChannel();
      pageSize = getPAGE_SIZE();

      buffer = ByteBuffer.allocate(pageSize);
      buffer.position(8);
      tupleCounter = 0;
   }

   /**
    * Creates a new TupleWriter to an output file with the given file name
    *
    * @param fileName the file name to output to
    * @throws IOException if there is an issue writing to the output file
    */
   public TupleWriter(String fileName, int i) throws IOException {
      FileOutputStream outputStream = new FileOutputStream(fileName + i);
      fileChannel = outputStream.getChannel();
      buffer = ByteBuffer.allocate(pageSize);
      buffer.position(8);
      tupleCounter = 0;
   }

   /**
    * Writes the tuple to the output file. If it is the end of the page, endPage is called and a
    * new page is started
    *
    * @param tuple an arraylist of the tuple's contents to write to the file
    * @throws IOException if there is an issue writing to the output file
    */
   public void write(ArrayList<Integer> tuple) throws IOException {
      if (buffer.position() + tuple.size() * 4 > pageSize) {
         endPage(tuple.size());

         buffer.clear();
         buffer.position(8);
         tupleCounter = 0;
      }

      for (int x : tuple) {
         buffer.putInt(x);
      }
      tupleCounter ++;
   }

   /**
    * Writes the tuple to the output file. If it is the end of the page, endPage is called and a
    * new page is started
    *
    * @param tuples an arraylist of the tuple's contents to write to the file
    * @throws IOException if there is an issue writing to the output file
    */
   public void writeAll(ArrayList<Tuple> tuples) throws IOException {
      for (Tuple tuple : tuples) {
         write(tuple.getAllCols());
      }
   }

   /**
    * Ends the current page by filling any remaining space with zeros and filling in the number
    * of columns and number of tuples at the beginning
    *
    * @param colNum the number of columns for a tuple
    * @throws IOException if there is an issue writing to the output file
    */
   public void endPage(int colNum) throws IOException {
      if (tupleCounter != 0) {
         byte[] bytes = new byte[pageSize - buffer.position()];
         buffer.put(bytes);

         buffer.putInt(0, colNum);
         buffer.putInt(4, tupleCounter);
         buffer.flip();
         fileChannel.write(buffer);
      }
   }

   public void end(int colSize) throws IOException {
      endPage(colSize);
      if (outputStream != null) {
         outputStream.close();
      }
   }
}
