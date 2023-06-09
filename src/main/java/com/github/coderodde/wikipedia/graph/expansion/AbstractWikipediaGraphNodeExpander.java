package com.github.coderodde.wikipedia.graph.expansion;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import com.github.coderodde.graph.pathfinding.uniform.delayed.AbstractNodeExpander;
import org.apache.commons.io.IOUtils;

/**
 * This abstract class specifies the facilities shared by both forward and 
 * backward node expanders.
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Aug 6, 2016)
 */
public abstract class AbstractWikipediaGraphNodeExpander
extends AbstractNodeExpander<String> {
   
    // TODO: Find a better way.
    protected static final Map<Character, String> ENCODING_MAP = 
            new HashMap<>();
    
    static {
        ENCODING_MAP.put(' ', "_");
        ENCODING_MAP.put('"', "%22");
        ENCODING_MAP.put(';', "%3B");
        ENCODING_MAP.put('<', "%3C");
        ENCODING_MAP.put('>', "%3E");

        ENCODING_MAP.put('?', "%3F");
        ENCODING_MAP.put('[', "%5B");
        ENCODING_MAP.put(']', "%5D");
        ENCODING_MAP.put('{', "%7B");
        ENCODING_MAP.put('|', "%7C");

        ENCODING_MAP.put('}', "%7D");
        ENCODING_MAP.put('?', "%3F");
    }

    /**
     * The script URL template for expanding forward.
     */
    private static final String FORWARD_REQUEST_API_URL_SUFFIX = 
            "?action=query" +
            "&titles=%s" + 
            "&prop=links" + 
            "&pllimit=max" + 
            "&format=json"; 
    
    /**
     * The script URL template for expanding backwards.
     */
    private static final String BACKWARD_REQUEST_API_URL_SUFFIX = 
            "?action=query" +
            "&list=backlinks" +
            "&bltitle=%s" + 
            "&bllimit=max" + 
            "&format=json";
    
    /**
     * The pattern for Wikipedia URLs.
     */
    private static final Pattern WIKIPEDIA_URL_PATTERN = 
            Pattern.compile("^(https://|http://)?..+\\.wikipedia.org/wiki/.+$");
    
    /**
     * The HTTPS protocol prefix.
     */
    private static final String SECURE_HTTP_PROTOCOL_PREFIX = "https://";
    
    /**
     * The HTTP protocol prefix.
     */
    private static final String HTTP_PROTOCOL_PREFIX = "http://";
    
    /**
     * The <tt>wiki</tt> directory token.
     */
    private static final String WIKI_DIR_TOKEN = "/wiki/";
    
    /**
     * The API script path.
     */
    private static final String API_SCRIPT_NAME = "/w/api.php";
    
    /**
     * Caches the basic Wikipedia article URL. For example, the basic URL of 
     * <tt>https://en.wikipedia.org/wiki/Disc_jockey</tt> is
     * <tt>en.wikipedia.org</tt>.
     */
    protected final String basicUrl;
    
    /**
     * Caches the textual representation of the URL pointing to the 
     * <a href="https://www.mediawiki.org/wiki/API:Main_page">Wikipedia API</a>.
     */
    private final String apiUrl;

    /**
     * Constructs a graph node expander for the language subgraph specified in
     * the input URL.
     * 
     * @param wikipediaUrl the entire Wikipedia article URL.
     */
    protected AbstractWikipediaGraphNodeExpander(String wikipediaUrl) {
        final String originalWikipediaUrl = wikipediaUrl;
        
        Objects.requireNonNull(wikipediaUrl,
                               "The input Wikipedia article is null.");
        
        if (!WIKIPEDIA_URL_PATTERN.matcher(wikipediaUrl).matches()) {
            throw new IllegalArgumentException(
                    "[INPUT ERROR] The input URL is not a valid Wikipedia " +
                    "article URL: \"" + originalWikipediaUrl + "\".");
        }
        
        wikipediaUrl = removeProtocolPrefix(wikipediaUrl);
        
        if (wikipediaUrl.contains("://")) {
            throw new IllegalArgumentException(
                    "[INPUT ERROR] The input URL specifies unknown protocol: " +
                    "\"" + originalWikipediaUrl + "\".");
        }
        
        this.apiUrl   = constructAPIURL(wikipediaUrl);
        this.basicUrl = wikipediaUrl.
                        substring(0, wikipediaUrl.indexOf(WIKI_DIR_TOKEN));
    }
    
    /**
     * Returns the raw Wikipedia URL. For example, for 
     * <tt>https://en.wikipedia.org/wiki/Disc_jockey</tt>, this method will 
     * return <tt>en.wikipedia.org</tt>. This is used for making sure that a 
     * particular language (<tt>en</tt> in the example above) is selected.
     * 
     * @return the basic Wikipedia URL.
     */
    public String getBasicUrl() {
        return basicUrl;
    }
    
    /**
     * {@inheritDoc }
     */
    @Override
    public boolean isValidNode(final String node) {
        return !generateSuccessors(node).isEmpty();
    }
    
    /**
     * Constructs a Wikipedia API URL from the raw {@code wikipediaUrl}. The
     * input {@code wikipediaUrl} is of the form <tt>en.wikipedia.org</tt>. The
     * idea here is that the search may be applied to article subgraphs with 
     * different languages.
     * 
     * @param wikipediaUrl the Wikipedia URL.
     * @return full URL to Wikipedia API.
     */
    private String constructAPIURL(final String wikipediaUrl) {
        return SECURE_HTTP_PROTOCOL_PREFIX +
               wikipediaUrl.substring(0, wikipediaUrl.indexOf(WIKI_DIR_TOKEN)) +
               API_SCRIPT_NAME;
    }
    
    /**
     * If the input string {@code url} has a prefix "http://" or "https://", 
     * removes it from the URL and returns the URL.
     * 
     * @param url the URL to process.
     * @return the URL without the protocol selector.
     */
    private String removeProtocolPrefix(final String url) {
        if (url.startsWith(SECURE_HTTP_PROTOCOL_PREFIX)) {
            return url.substring(SECURE_HTTP_PROTOCOL_PREFIX.length());
        }
        
        if (url.startsWith(HTTP_PROTOCOL_PREFIX)) {
            return url.substring(HTTP_PROTOCOL_PREFIX.length());
        }
        
        return url;
    }
    
    /**
     * The actual implementation of the method producing the neighbors of a 
     * graph node.
     * 
     * @param node    the node to expand.
     * @param forward specifies the direction of the node expansion operation.
     *                if {@code forward} is {@code true}, generates the child
     *                nodes of {@code node}. Otherwise, generates the parent 
     *                nodes of {@code node}.
     * @return 
     */
    protected List<String> baseGetNeighbors(final String node,
                                            final boolean forward) {
        String jsonDataUrl;
        
        try {
            jsonDataUrl = 
                    apiUrl + String.format(forward ?
                                                FORWARD_REQUEST_API_URL_SUFFIX :
                                                BACKWARD_REQUEST_API_URL_SUFFIX,
                                           URLEncoder.encode(node,
                                                             "UTF-8"));
        } catch (final UnsupportedEncodingException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
        
        String jsonText;
        
        try {
            jsonText = IOUtils.toString(new URL(jsonDataUrl),
                                        Charset.forName("UTF-8"));
        } catch (final IOException ex) {
            throw new IllegalStateException(
                    "[I/O ERROR] Failed loading the JSON data from the " +
                    "Wikipedia API: " + ex.getMessage(), ex);
        }
        
        return forward ?
                extractForwardLinkTitles(jsonText) :
                extractBackwardLinkTitles(jsonText);
    }
    
    /**
     * Returns all the Wikipedia article titles that the current article links 
     * to.
     * 
     * @param jsonText the data in JSON format.
     * @return a list of Wikipedia article titles parsed from {@code jsonText}.
     */
    private static List<String> extractForwardLinkTitles(String jsonText) {
        List<String> linkNameList = new ArrayList<>();
        JsonArray linkNameArray;

        try {
            JsonObject root = new JsonParser().parse(jsonText).getAsJsonObject();
            JsonObject queryObject = root.get("query").getAsJsonObject();
            JsonObject pagesObject = queryObject.get("pages").getAsJsonObject();
            JsonObject mainObject  = pagesObject.entrySet()
                                                .iterator()
                                                .next()
                                                .getValue()
                                                .getAsJsonObject();

            linkNameArray = mainObject.get("links").getAsJsonArray();
        } catch (NullPointerException ex) {
            return linkNameList;
        }

        linkNameArray.forEach((element) -> {
            int namespace = element.getAsJsonObject().get("ns").getAsInt();

            if (namespace == 0) {
                String title = element.getAsJsonObject()
                                      .get("title")
                                      .getAsString();

                linkNameList.add(encodeWikipediaStyle(title));
            }
        });

        return linkNameList;
    }

    /**
     * Returns all the Wikipedia article titles that link to the current
     * article.
     * 
     * @param jsonText the data in JSON format.
     * @return a list of Wikipedia article titles parsed from {@code jsonText}.
     */
    private static List<String> extractBackwardLinkTitles(String jsonText) {
        List<String> linkNameList = new ArrayList<>();
        JsonArray backLinkArray;

        try {
            JsonObject root = new JsonParser().parse(jsonText).getAsJsonObject();
            JsonObject queryObject = root.get("query").getAsJsonObject();
            backLinkArray = queryObject.get("backlinks").getAsJsonArray();
        } catch (NullPointerException ex) {
            return linkNameList;
        }

        backLinkArray.forEach((element) -> {
            int namespace = element.getAsJsonObject()
                                   .get("ns")
                                   .getAsInt();

            if (namespace == 0) {
                String title = element.getAsJsonObject()
                                      .get("title")
                                      .getAsString();

                linkNameList.add(encodeWikipediaStyle(title));
            }
        });

        return linkNameList;
    }
    
    /**
     * Encodes some special characters using percent encoding.
     * 
     * @param s the string to encode.
     * @return the encoded version of {@code s}.
     */
    private static String encodeWikipediaStyle(final String s) {
        final StringBuilder sb = new StringBuilder();

        for (final char c : s.toCharArray()) {
            String encoder = ENCODING_MAP.get(c);

            if (encoder != null) {
                sb.append(encoder);
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }
}
