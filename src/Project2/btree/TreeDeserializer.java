package Project2.btree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static Project2.DBCatalog.getPAGE_SIZE;

public class TreeDeserializer {

    private FileInputStream inputStream;    // The file input stream for reader
    private FileChannel fileChannel;        // The file channel for reader
    private ByteBuffer buffer;              // The byte buffer
    private int rootId;                     // the address of the root
    private int numOfLeaves;                // The number of leaves in the tree
    private int order;                      // The order of the tree
    private Integer lowKey;                 // The low key of the search range
    private Integer highKey;                // The high key of the search range
    private LeafNode currLeaf;              // Current leaf node
    private int currLeafPageAddress;        // Current leaf page address
    private int dataEntryPointer;           // the pointer for dataEntry
    private int ridPointer;                 // The pointer for record id
    private boolean finished;               // is finished for get next rid

    private int firstLeafPageAddress;       // the first leaf node page address for the lowKey
    private int firstDataEntryPointer;      // the pointer for dataEntry for the first entry

    private static final int BUFFER_SIZE = getPAGE_SIZE();      // the size of the buffer page
    private static final int HEADER_ID = 0;                     // the the page id of header page.
    private static final int FIRST_LEAF = 1;                    // the id of the first leaf page.


    /**
     * Constructs the deserializer by using the given indexFile
     *
     * @param indexFile storing the serialized tree index
     * @throws FileNotFoundException
     */
    public TreeDeserializer(File indexFile) throws IOException {
        File file = indexFile;
        inputStream = new FileInputStream(file);
        fileChannel = inputStream.getChannel();
        buffer = ByteBuffer.allocate(BUFFER_SIZE);
        fetchPage(HEADER_ID);
        rootId = buffer.getInt();
        numOfLeaves = buffer.getInt();
        order = buffer.getInt();
        clearBuffer();
    }


    /**
     * Constructs the the deserializer using the given indexFile and the given
     * range specified by the lowKey and highKey.
     *
     * @param indexFile which stores the serialized tree index.
     * @param lowKey the lower bound (inclusive), no limit if set to null.
     * @param highKey the higher bound (exclusive), no limit if set to null.
     * @throws FileNotFoundException
     */
    public TreeDeserializer(File indexFile, Integer lowKey, Integer highKey) throws IOException {
        this(indexFile);
        this.lowKey = lowKey;
        this.highKey = highKey;
        dataEntryPointer = 0;
        ridPointer = 0;
        currLeafPageAddress = FIRST_LEAF;
        finished = false;
        moveToStartLeafPage();
    }

    /**
     * locate to the starting leaf page using the given low key
     */
    private void moveToStartLeafPage() throws IOException{
        if (lowKey == null) {
            currLeaf = (LeafNode) dsNode(currLeafPageAddress);
        } else {
            currLeaf = traverseToStartLeaf();
            while (currLeaf != null
                    && currLeaf.dataEntries.get(dataEntryPointer).key < lowKey) {
                dataEntryPointer++;
                if (dataEntryPointer >= currLeaf.dataEntries.size()) {
                    currLeaf = getNextLeafNode();
                    dataEntryPointer = 0;
                }
            }

        }

        firstLeafPageAddress = currLeafPageAddress;
        firstDataEntryPointer = dataEntryPointer;
    }

    /**
     * let the deserializer to traverse to the start leaf
     * @return the start leaf
     */
    private LeafNode traverseToStartLeaf() throws IOException {
        if (lowKey == null) {
            throw new IllegalArgumentException();
        }
        fetchPage(rootId);
        IndexNode root = dsIndexNode();
        TreeNode curr = root;
        int pageAddress = FIRST_LEAF;
        while (curr instanceof IndexNode) {
            IndexNode currIndexNode = (IndexNode) curr;
            int pageIndex = Collections.binarySearch(currIndexNode.keys, lowKey + 1);
            pageIndex = pageIndex >= 0 ? pageIndex : -(pageIndex + 1);
            pageAddress = currIndexNode.address.get(pageIndex);
            curr = dsNode(pageAddress);
        }
        currLeafPageAddress = pageAddress;
        return (LeafNode) curr;
    }

    /**
     *  deserialize the node of the given address
     */
    private TreeNode dsNode(int address) throws IOException {
        fetchPage(address);
        if (address > 0 && address <= numOfLeaves)
            return dsLeafNode();
        else
            return dsIndexNode();
    }

    /**
     *  deserialize the leaf node
     */
    private LeafNode dsLeafNode() {
        int flag = buffer.getInt();
        int numOfDEntries = buffer.getInt();
        List<DataEntry> dataEntries = new ArrayList<>();
        for (int i = 0; i < numOfDEntries; i++) {
            int key = buffer.getInt();
            int ridSize = buffer.getInt();
            List<rid> rids = new ArrayList<>();
            for (int j = 0; j < ridSize; j++) {
                int pageId = buffer.getInt();
                int tupleId = buffer.getInt();
                rids.add(new rid(pageId, tupleId));
            }
            dataEntries.add(new DataEntry(key, rids));
        }
        return new LeafNode(order, dataEntries);
    }

    /**
     *  deserialize the index node
     */
    private IndexNode dsIndexNode() {
        int flag = buffer.getInt();
        int numOfKeys = buffer.getInt();
        List<Integer> keys = new ArrayList<>();
        for (int i = 0; i < numOfKeys; i++) {
            keys.add(buffer.getInt());
        }
        List<Integer> addresses = new ArrayList<>();
        for (int i = 0; i < numOfKeys + 1; i++) {
            addresses.add(buffer.getInt());
        }
        return new IndexNode(order, keys, null, addresses);
    }

    /**
     *  get the next leaf node
     */
    private LeafNode getNextLeafNode() throws IOException {
        currLeafPageAddress++;
        if (currLeafPageAddress > 0 && currLeafPageAddress <= numOfLeaves)
            return (LeafNode) dsNode(currLeafPageAddress);
        else
            return null;
    }

    /**
     * fetch page of pageId to buffer
     */
    private void fetchPage(int pageId) throws IOException {
        clearBuffer();
        long position = BUFFER_SIZE * (long) pageId;
        fileChannel.position(position);
        fileChannel.read(buffer);
        buffer.flip();
    }

    /**
     * Returns the next record id in the specified range, null if no more
     * record id to return, null if reached the end of the range
     * @return rid the record id
     *
     * @throws IOException
     */
    public rid getNextRid() throws IOException {
        // check if deserialization is finished or current leaf is just null
        if (finished || currLeaf == null) return null;
        // we need to go to the next data entry and the next leaf page
        if (ridPointer >= currLeaf.dataEntries.get(dataEntryPointer).RidList.size()) {
            dataEntryPointer++;
            ridPointer = 0;
        }

        if (dataEntryPointer >= currLeaf.dataEntries.size()) {
            currLeaf = getNextLeafNode();
            // at the end of the leaf page
            if (currLeaf == null) {
                finished = true;
                return null;
            }
            dataEntryPointer = 0;
        }

        // dataEntryPointer is valid and ridPointer is valid at present
        // check if it is within the right bound
        if (highKey != null && currLeaf.dataEntries.get(dataEntryPointer).key >= highKey) {
            finished = true;
            return null;
        }

        return currLeaf.dataEntries.get(dataEntryPointer).RidList.get(ridPointer++);
    }

    /**
     * Resets to the start leaf page
     *
     * @throws IOException
     */
    public void reset() throws IOException {
        fetchPage(firstLeafPageAddress);
        currLeaf = dsLeafNode();
        dataEntryPointer = firstDataEntryPointer;

        finished = false;
    }

    /**
     * Helper function to clear the buffer
     */
    private void clearBuffer() {
        buffer.clear();
        buffer.put(new byte[BUFFER_SIZE]);
        buffer.clear();
    }

    public void close() throws IOException {
        inputStream.close();
    }
}
