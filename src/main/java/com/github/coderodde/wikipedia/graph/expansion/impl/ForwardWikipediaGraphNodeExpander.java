package com.github.coderodde.wikipedia.graph.expansion.impl;

import com.github.coderodde.wikipedia.graph.expansion.AbstractWikipediaGraphNodeExpander;
import java.util.List;

/**
 * This class implements a forward node expander in the Wikipedia article graph.
 * If article <tt>A</tt> has a link to <tt>B</tt>, this expander will generate
 * <tt>B</tt> whenever asked to process <tt>A</tt>. We can say that this 
 * expander traverses each directed arc from tail to head.
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Aug 6, 2016)
 */
public class ForwardWikipediaGraphNodeExpander 
extends AbstractWikipediaGraphNodeExpander {

    public ForwardWikipediaGraphNodeExpander(final String wikipediaUrl) {
        super(wikipediaUrl);
    }
    
    @Override
    public List<String> generateSuccessors(String node) {
        return baseGetNeighbors(node, true);
    }
}
