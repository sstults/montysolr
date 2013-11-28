package org.apache.solr.handler.batch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;

import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldCache.DocTerms;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.CitationLRUCache;

/**
 * This dumps a CITEDBY data structure to disk
 *
 */
public class BatchProviderDumpCitationCache extends BatchProvider {
	
	
	public void run(SolrQueryRequest req, BatchHandlerRequestQueue queue) throws Exception {
		
		SolrParams params = req.getParams();
	  String jobid = params.get("jobid");
	  String workDir = params.get("#workdir");
	  String uniqueField = params.get("unique_field", "bibcode");
	  String refField = params.get("ref_field", "reference");
	  String cacheName = params.get("cache_name", "citations-cache");
	  boolean returnDocids = params.getBool("return_docids", false);
	  
	  assert jobid != null && new File(workDir).canWrite();
	  
	  String[] idFields = uniqueField.split(",");
	  
	  File jobFile = new File(workDir + "/" + jobid);
		final BufferedWriter out = new BufferedWriter(new FileWriter(jobFile), 1024*256);
		
		CitationLRUCache<Object, Integer> cache = (CitationLRUCache<Object, Integer>) req.getSearcher().getCache(cacheName);
    
		if (cache == null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Cannot find cache: " + cacheName);
    }
		
	  
	  

	  BytesRef ret = new BytesRef();
	  boolean first = true;
	  
	  Iterator<int[][]> it = cache.getCitationsIterator();
	  
	  if (!returnDocids) {
	    DocTerms uniqueValueCache = FieldCache.DEFAULT.getTerms(req.getSearcher().getAtomicReader(), uniqueField);
	    int paperid = 0;
	    while (it.hasNext()) {
	      int[][] data = it.next();
	      int[] references = data[0];
		  	if (references != null && references.length > 0) {
		  		uniqueValueCache.getTerm(paperid, ret);
		  		out.write(ret.utf8ToString());
		  		out.write("\t");
		  		first=true;
		  		for (int luceneDocId: references) {
			  		ret = uniqueValueCache.getTerm(luceneDocId, ret);
					  if (ret.length > 0) {
					  	if (!first) {
					  		out.write("\t");
					  	}
					    out.write(ret.utf8ToString());
					    first = false;
					  }
		  		}
		  		out.write("\n");
		  	}
		  	paperid++;
		  }
	  }
	  else {
	    int paperid = 0;
	    while (it.hasNext()) {
	      int[][] data = it.next();
        int[] references = data[0];
        
		  	if (references != null && references.length > 0) {
		  		out.write(Integer.toString(paperid));
		  		out.write("\t");
		  		first=true;
		  		for (int luceneDocId: references) {
				  	if (!first) {
				  		out.write("\t");
				  	}
				    out.write(Integer.toString(luceneDocId));
				    first = false;
		  		}
		  		out.write("\n");
		  	}
		  	paperid++;
		  }
	  }
	  
	  out.close();
	}

	@Override
  public String getDescription() {
	  return "Dumps citation network structure to disk";
  }

}
