/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package consistenthashing;

/**
 *
 * @author: carlisle lee
 * @date: 2014-05-25
 */
public interface NodeLocator {
    public RNode getPrimary(final String k);
    public int getNodesNum();
}
