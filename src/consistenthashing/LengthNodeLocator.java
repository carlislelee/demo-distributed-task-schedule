/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package consistenthashing;

import java.util.List;

/**
 *
 * @author evan
 */
public class LengthNodeLocator implements NodeLocator {

    private List<RNode> nodes = null;
    private String lengthrule = null;
    private int[] lengths = null;

    public LengthNodeLocator(List<RNode> nodes, String lengthrule) {
        this.nodes = nodes;
        this.lengthrule = lengthrule;
        String[] l = lengthrule.split("\\|");
        lengths = new int[l.length];
        for (int i = 0; i < l.length; i++) {
            lengths[i] = Integer.parseInt(l[i]);
        }
    }

    @Override
    public RNode getPrimary(String k) {
        int len = k.length();
        if (lengths.length == 0) {
            return nodes.get(0);
        } else {
            int ans = getIndex(len, 0, lengths.length - 1);
            return nodes.get(ans);
        }
    }

    @Override
    public int getNodesNum() {
        return nodes.size();
    }

    private int getIndex(int len, int start, int end) {
        if (start == end) {
            if (lengths[start] < len) {
                return start + 1;
            } else {
                return start;
            }
        }
        int mid = (start + end) / 2;
        if (lengths[mid] > len) {
            return getIndex(len, start, mid);
        } else if (lengths[mid] < len) {
            return getIndex(len, mid + 1, end);
        } else {
            return mid;
        }
    }
}
