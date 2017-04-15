import clojure.lang.ASeq;
import clojure.lang.Seqable;
import clojure.lang.ISeq;
import clojure.lang.IPersistentMap;
import clojure.lang.Obj;
import clojure.lang.PersistentList;
import clojure.lang.Murmur3;
import java.util.Arrays;

class IdListSeq extends ASeq {

    public final IdList idList;
    public final int idx;

    public IdListSeq(IdList idList, int idx) {
        this.idList = idList;
        this.idx = idx;
    }

    public Object first() {
        return this.idList.get(this.idx);
    }

    @Override
    public int count() {
        return this.idList.size() - this.idx;
    }

    public ISeq next() {
        if (this.count() > 1) {
            return new IdListSeq(this.idList, this.idx+1);
        } else {
            return null;
        }
    }

    public Obj withMeta(IPersistentMap meta) {
        throw new UnsupportedOperationException();
    }
    
}

public class IdList implements Seqable {

    public final long[] ids;
    //it's a ring buffer
    public int startIdx;
    public int endIdx;


    public IdList(int size) {
        this.ids = new long[size];
        this.startIdx = 0;
        this.endIdx = 0;
    }

    public IdList(int maxSize, java.util.List<Long> data) {
        int size = data.size();
        if (size >= maxSize-1) {
            throw new IllegalArgumentException("constructor input is larger than size");
        }
        this.ids = new long[maxSize];
        this.startIdx = 0;
        int idx = 0;
        for (long id : data) {
            this.ids[idx] = id;
            idx++;
        }
        this.endIdx = idx;
    }

    public IdList(IdList src) {
        this.ids = src.copyTo(this);
    }
 
    public int hashCode() {
        int idx = this.startIdx;
        int res = 31;
        while (idx != this.endIdx) {
            res ^= this.ids[idx];
            idx++;
            if (idx >= this.ids.length) {
                idx = 0;
            }
        }
        return Murmur3.hashInt(res);
    } 

    public synchronized long[] copyTo(IdList target) {
        target.startIdx = this.startIdx;
        target.endIdx = this.endIdx;
        return Arrays.copyOf(this.ids, this.ids.length);
    }

    public synchronized long get(int idx) {
        if (idx >= this.ids.length || idx < 0 || this.size() < 1) {
            throw new IndexOutOfBoundsException();
        }
        return this.ids[(this.startIdx + idx) % this.ids.length];
    }

    public synchronized int size() {
        if (this.endIdx == this.startIdx) {
            return 0;
        } else if (this.endIdx > this.startIdx) {
            return this.endIdx - this.startIdx;
        } else {
            return (this.ids.length - this.startIdx) + this.endIdx;
        }
    }

    public synchronized void put(long value) {
        this.ids[this.endIdx] = value;
        this.endIdx++;
        if (this.endIdx >= this.ids.length) {
            this.endIdx = 0;
        }
        if (this.endIdx <= this.startIdx) {
            this.startIdx++;
        }
        if (this.startIdx >= this.ids.length) {
            this.startIdx = 0;
        }
    }

    public ISeq seq() {
        if (this.ids.length > 0 && this.size() > 0) {
            return new IdListSeq(new IdList(this), 0);
        } else {
            return PersistentList.EMPTY;
        }
    }

}
