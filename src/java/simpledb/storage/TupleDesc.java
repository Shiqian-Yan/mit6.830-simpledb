package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private List<TDItem> tupleDescList = new ArrayList<>();
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String getFieldName() {
            return fieldName;
        }

        public Type getFieldType() {
            return fieldType;
        }


        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tupleDescList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if(typeAr.length == fieldAr.length) {
            for (int i = 0; i < typeAr.length; i++) {
                tupleDescList.add(new TDItem(typeAr[i],fieldAr[i]));
            }
        }
    }
    public TupleDesc(List<TDItem> tupleDescList){
        this.tupleDescList = tupleDescList;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        for (int i = 0; i < typeAr.length; i++) {
            tupleDescList.add(new TDItem(typeAr[i],null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tupleDescList.size();
    }

    public List<TDItem> getTupleDescList() {
        return tupleDescList;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0 || i>=tupleDescList.size()){
            throw new NoSuchElementException();
        }
        return tupleDescList.get(i).getFieldName();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0 || i>=tupleDescList.size()){
            throw new NoSuchElementException();
        }
        return tupleDescList.get(i).getFieldType();
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if(name == null){
            throw new NoSuchElementException();
        }
        for (int i = 0; i < tupleDescList.size(); i++) {
            String tmp = tupleDescList.get(i).getFieldName();
            if(tmp == null){
                    continue;
            }
            if (tmp.equals(name)) {
                    return i;
            }
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int res = 0;
        for (int i = 0; i < tupleDescList.size(); i++) {
            TDItem item = tupleDescList.get(i);
            res = res +item.getFieldType().getLen();
        }
        return res;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<TDItem> tupleDescList1 = td1.tupleDescList;
        List<TDItem> tupleDescList2 = td2.tupleDescList;
        List<TDItem> res = new ArrayList<>(tupleDescList1);
        for (int i = 0; i < tupleDescList2.size(); i++) {
            res.add(tupleDescList2.get(i));
        }
        return new TupleDesc(res);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(o instanceof TupleDesc){
            List<TDItem> tmp = ((TupleDesc) o).tupleDescList;
            if(tmp.size() == tupleDescList.size()){
                for (int i = 0; i < tupleDescList.size(); i++) {
                    if(!tupleDescList.get(i).getFieldType().equals(tmp.get(i).getFieldType())){
                          return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        return "TupleDesc{" +
                "tupleDescList=" + tupleDescList +
                '}';
    }
}
