package cz.knav.virtuoso.trippi;

import java.util.Set;

import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;
import org.trippi.impl.base.TriplestoreSession;

import virtuoso.sesame2.driver.VirtuosoRepository;

public class VirtuosoSession implements TriplestoreSession {
	
	private static final Logger logger = LoggerFactory.getLogger(VirtuosoSession.class.getName());
	
    private static final String SPARQL  = QueryLanguage.SPARQL.getName();
    private static final String UNSUPPORTED = "unsupported";

    public static final String[] TRIPLE_LANGUAGES = new String[] { SPARQL };
    public static final String[] TUPLE_LANGUAGES  = new String[] { UNSUPPORTED };

	private Repository repository;
	
	public VirtuosoSession(String connectString, String user, String password) {
		logger.error("THIS IS NOT error! constructor VirtuosoSession - begin");
		repository = new VirtuosoRepository(connectString, user, password);
		try {
			repository.initialize();
		} catch (RepositoryException e) {
			throwRuntimeException(e);
		}
		logger.error("THIS IS NOT error! constructor VirtuosoSession - end");
	}

	public void add(Set<Triple> triples) throws UnsupportedOperationException,
			TrippiException {
		doTriples(triples, true);
	}

	public void delete(Set<Triple> triples) throws UnsupportedOperationException,
			TrippiException {
		doTriples(triples, false);
	}

	public void close() throws TrippiException {
		try {
			repository.shutDown();
		} catch (RepositoryException e) {
			throwTrippiException(e);
		}
	}
	
	private void throwTrippiException(Exception e) throws TrippiException {
		logger.error(e.getMessage(), e);
		throw new TrippiException(e.getMessage(), e);
	}
	private void throwRuntimeException(Exception e) {
		logger.error(e.getMessage(), e);
		throw new RuntimeException(e.getMessage(), e);
	}

	private void doTriples(Set<Triple> triples, boolean add) throws TrippiException {
		//logger.error("THIS IS NOT error! doTriples - begin");
	    if (triples == null || triples.size() == 0) {
	        return;
	    }
		RepositoryConnection con = null;
		try {
			try {
				con = repository.getConnection();
			} catch (RepositoryException e) {
				throwTrippiException(e);
			}
			for (Triple triple : triples) {
				try {
					//logger.error("THIS IS NOT error! " + triple.getSubject()
							//+ " X " + triple.getPredicate()+ " X " + triple.getObject());
					if (add) {
						con.add(triple.getSubject(), triple.getPredicate(), triple.getObject());
					} else {
						con.remove(triple.getSubject(), triple.getPredicate(), triple.getObject());
					}
				} catch (RepositoryException e) {
					throwTrippiException(e);
				}
			}
		} finally { 
			try {
				con.close();
			} catch (RepositoryException e) {
				throwTrippiException(e);
			}
		}
		//logger.error("THIS IS NOT error! doTriples - end");
	}

	public TripleIterator findTriples(String lang, String queryText)
			throws TrippiException {
		//logger.error("THIS IS NOT error! findTriples(String lang, String queryText) - begin");
		return new VirtuosoTripleIterator(repository, lang, queryText);
	}

	public TripleIterator findTriples(SubjectNode subject,
			PredicateNode predicate, ObjectNode object) throws TrippiException {
		//logger.error("THIS IS NOT error! findTriples(SubjectNode subject, - begin");
		return findTriples(SPARQL,
				null //TODO: For inserting triples, deleting triples and
				     //SPARQL CONSTRUCT queries I didn't need this function
				     //findTriples(SubjectNode subject,	PredicateNode predicate, ObjectNode object)
				);
	}
	
    // Implements TriplestoreSession.listTripleLanguages()
    public String[] listTripleLanguages() { return TRIPLE_LANGUAGES; }
           
    // Implements TriplestoreSession.listTupleLanguages()
    public String[] listTupleLanguages()  { return TUPLE_LANGUAGES; }

    // Implements TriplestoreSession.query(String, String)
    public TupleIterator query(String query,
                               String lang) throws TrippiException {
        // TODO: I didn't need this function, so I implemented it the same way as it
    	// is implemented in Trippi for MPTstore.
        throw new TrippiException("Unsupported tuple query language: " + lang);
    }

}
