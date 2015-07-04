/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package consistenthashing;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author: carlisle lee
 * @date: 2014-05-25
 */
public class HashCodeNodeLocator implements NodeLocator{
    private List<RNode> nodes = new ArrayList<RNode>();
    
    public HashCodeNodeLocator(List<RNode> nodes) {
        this.nodes = (ArrayList<RNode>) nodes;
    }

    @Override
    public RNode getPrimary(String k) {
        int hash = Math.abs(k.hashCode());
        int i = hash%nodes.size();
        for(RNode n: nodes){
            if(n.getName().equals(""+ i)){
                return n;
            }
        }
        return null;
    }


    @Override
    public int getNodesNum() {
        return nodes.size();
    }
    
}
