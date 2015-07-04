package consistenthashing;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author: carlisle lee
 * @date: 2014-05-25
 */
public class DynamicAllocate {

    public List<RNode> nodes = new ArrayList<RNode>();
    public String lengthrule = null;
    public MD5NodeLocator MD5Nodelocator = null;
    public LengthNodeLocator lengthNodelocator = null;
    public HashCodeNodeLocator hashCodeNodelocator = null;
    public int nodeCopies = 160;

    public void setNodes(List<RNode> aurl) {
        if (!aurl.isEmpty()) {
            for (RNode node : aurl) {
                nodes.add(node);
            }
        }
    }
    
    public void setLengthrule(String lengthrule){
        this.lengthrule = lengthrule;
    }

    public MD5NodeLocator getMD5NodeLocator() {
        MD5Nodelocator = new MD5NodeLocator(nodes, MD5HashAlgorithm.KETAMA_HASH, nodeCopies);
        return MD5Nodelocator;
    }

    public HashCodeNodeLocator getHashCodeNodeLocator() {
        hashCodeNodelocator = new HashCodeNodeLocator(nodes);
        return hashCodeNodelocator;
    }
    
    public LengthNodeLocator getLengthNodeLocator() {
        lengthNodelocator = new LengthNodeLocator(nodes , lengthrule);
        return lengthNodelocator;
    }
}
