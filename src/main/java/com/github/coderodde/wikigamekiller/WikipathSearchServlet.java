package com.github.coderodde.wikigamekiller;

import java.io.IOException;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import com.github.coderodde.graph.pathfinding.uniform.delayed.AbstractDelayedGraphPathFinder;
import com.github.coderodde.graph.pathfinding.uniform.delayed.impl.ThreadPoolBidirectionalPathFinder;
import com.github.coderodde.wikipedia.graph.expansion.AbstractWikipediaGraphNodeExpander;
import com.github.coderodde.wikipedia.graph.expansion.impl.BackwardWikipediaGraphNodeExpander;
import com.github.coderodde.wikipedia.graph.expansion.impl.ForwardWikipediaGraphNodeExpander;

@WebServlet(name = "WikipediaPathSearchServlet", urlPatterns = {"search"})
public class WikipathSearchServlet extends HttpServlet {
    
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        
        final String sourceUrlText = request.getParameter("from_url").trim();
        final String targetUrlText = request.getParameter("to_url")  .trim();
        final String threadsText   = request.getParameter("threads");
        
        int requestedNumberOfThreads = Integer.parseInt(threadsText);
        
        final String sourceTitle = sourceUrlText
                .substring(sourceUrlText.lastIndexOf('/') + 1);
        
        final String targetTitle = targetUrlText.
                substring(targetUrlText.lastIndexOf('/') + 1);
        
        
        // When deployed to Heroku, prints to the application log file.
        System.out.println("[WIKIPATH_QUERY] " + sourceUrlText + 
                           " -> " + targetUrlText);
        
        try {
            final AbstractDelayedGraphPathFinder<String> finder =
                    new ThreadPoolBidirectionalPathFinder<>(
                            requestedNumberOfThreads);

            final AbstractWikipediaGraphNodeExpander forwardSearchExpander = 
                    new ForwardWikipediaGraphNodeExpander(sourceUrlText);

            final AbstractWikipediaGraphNodeExpander backwardSearchExpander = 
                    new BackwardWikipediaGraphNodeExpander(targetUrlText);

            if (!forwardSearchExpander
                    .getBasicUrl()
                    .equals(backwardSearchExpander.getBasicUrl())) {
                request.setAttribute("error_msg",
                                     "The two given Wikipedia articles seem " +
                                     "to belong to different languages.");
                request.getRequestDispatcher("show.jsp")
                       .forward(request, response);
                return;
            }

            final List<String> path = 
                    finder.search(sourceTitle, 
                                  targetTitle, 
                                  forwardSearchExpander, 
                                  backwardSearchExpander,
                                  null, 
                                  null, 
                                  null);

            request.setAttribute(
                    "result_msg",
                    String.format("The search took %.3f seconds, expanding " +
                                  "%d nodes.",
                                  finder.getDuration() / 1e3,
                                  finder.getNumberOfExpandedNodes()));

            for (int i = 0; i < path.size(); ++i) {
                path.set(i, 
                         "https://" + forwardSearchExpander.getBasicUrl() +
                         "/wiki/" + path.get(i));
            }
            
            request.setAttribute("solution", path);
            request.setAttribute("threads", requestedNumberOfThreads);
        } catch (final IOException | ServletException ex) {
            request.setAttribute("error_msg", "ERROR: " + ex.getMessage());
            request.setAttribute("threads", requestedNumberOfThreads);
        }
        
        request.getRequestDispatcher("show.jsp").forward(request, response);
    }
}
