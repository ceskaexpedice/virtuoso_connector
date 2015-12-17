package cz.knav.virtuoso.trippi;

import java.net.URI;
import java.net.URISyntaxException;

import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trippi.RDFUtil;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;

public class VirtuosoTripleIterator extends TripleIterator {

	private static final Logger logger = LoggerFactory.getLogger(VirtuosoTripleIterator.class.getName());

    private GraphQueryResult result;
    private RepositoryConnection con;
    
    private RDFUtil rdfUtil = new RDFUtil();

	public VirtuosoTripleIterator(Repository rep, String lang, String queryText)
			throws TrippiException {
		//logger.error("THIS IS NOT error! constructor VirtuosoTripleIterator - begin");
		if (lang.equalsIgnoreCase(QueryLanguage.SPARQL.getName())) {
			try {
				con = rep.getConnection();
				result = con.prepareGraphQuery(QueryLanguage.SPARQL, queryText).evaluate();
			} catch (Exception e) {
				throwTrippiException(e);
			}
		} else {
			throw new TrippiException("Unrecognized query language: " + lang);
		}
		//logger.error("THIS IS NOT error! constructor VirtuosoTripleIterator - end");
    }

	public Triple next() throws TrippiException {
		Triple ret = null;
		try {
			if (result.hasNext()) {
				Statement statement = result.next();
				ret = createTriple(statement.getSubject(), statement.getPredicate(), statement.getObject());
			}
		} catch (Exception e) {
			throwTrippiException(e);
		}
		//logger.error("THIS IS NOT error! public Triple next():" + ret);
		return ret;
	}

    private SubjectNode subjectNode(org.openrdf.model.Resource subject) 
            throws GraphElementFactoryException,
                   URISyntaxException {
    	if (subject == null) {
    		return null;
    	} else if (subject instanceof org.openrdf.model.URI) {
            return rdfUtil.createResource(new URI(subject.toString()));
        } else {
            return rdfUtil.createResource(((org.openrdf.model.BNode) subject).getID().hashCode());
        }
    }

    private PredicateNode predicateNode(org.openrdf.model.URI predicate)
            throws GraphElementFactoryException,
                   URISyntaxException {
    	if (predicate == null) {
    		return null;
    	} else {
            return rdfUtil.createResource( new URI(predicate.toString()) );
    	}
    }

    private ObjectNode objectNode(org.openrdf.model.Value object)
            throws GraphElementFactoryException,
                   URISyntaxException {
    	if (object == null) {
    		return null;
    	} else {
            if (object instanceof org.openrdf.model.URI) {
                return rdfUtil.createResource( new URI(object.toString()) );
            } else if (object instanceof  org.openrdf.model.Literal) {
                org.openrdf.model.Literal lit = (org.openrdf.model.Literal) object;
                org.openrdf.model.URI uri = lit.getDatatype();
                String lang = lit.getLanguage();
                if (uri != null) {
                    // typed 
                    return rdfUtil.createLiteral(lit.getLabel(), new URI(uri.toString()));
                } else if (lang != null && !lang.equals("")) {
                    // local
                    return rdfUtil.createLiteral(lit.getLabel(), lang);
                } else {
                    // plain
                    return rdfUtil.createLiteral(lit.getLabel());
                }
            } else {
                return rdfUtil.createResource(((org.openrdf.model.BNode) object).getID().hashCode());
            }
    	}
    }

    private Triple createTriple(org.openrdf.model.Resource subject, 
                       org.openrdf.model.URI predicate, 
                       org.openrdf.model.Value object) throws Exception {
        Triple triple = rdfUtil.createTriple( subjectNode(subject),
                                      predicateNode(predicate),
                                      objectNode(object));
        return triple;
    }

    public boolean hasNext() throws TrippiException {
    	boolean ret = true;
        try {
			ret = result.hasNext();
		} catch (QueryEvaluationException e) {
			throwTrippiException(e);
		}
        return ret;
    }
    
	public void close() throws TrippiException {
		try {
			con.close();
			result.close();
		} catch (Exception e) {
			throwTrippiException(e);
		}
	}
	
	private void throwTrippiException(Exception e) throws TrippiException {
		logger.error(e.getMessage(), e);
		throw new TrippiException(e.getMessage(), e);
	}
    
}
