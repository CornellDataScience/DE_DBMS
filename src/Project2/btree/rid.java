package Project2.btree;

public class rid {
    /**
     * Rid class contains page id and tuple id for
     * a specifc record id in leafnode
     * @author Rong Tan rt389
     */
    int pageId;
    int tupleId;

    public rid(int pageId, int tupleId){
        this.pageId = pageId;
        this.tupleId  = tupleId;
    }

    /**
     * @return the page id of the rid
     */
    public int getPageId() {
        return pageId;
    }

    /**
     * @return the tuple id of the rid
     */
    public int getTupleid() {
        return tupleId;
    }

    /**
     * @return the string representation of this record id.
     */
    @Override
    public String toString() {
        return "(" + pageId + "," + tupleId + ")";
    }

}
