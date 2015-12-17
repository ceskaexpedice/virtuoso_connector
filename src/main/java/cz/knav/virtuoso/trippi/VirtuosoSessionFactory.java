package cz.knav.virtuoso.trippi;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.trippi.TrippiException;
import org.trippi.impl.base.TriplestoreSession;
import org.trippi.impl.base.TriplestoreSessionFactory;

public class VirtuosoSessionFactory implements TriplestoreSessionFactory {

	//private static final Logger logger = LoggerFactory.getLogger(VirtuosoSessionFactory.class.getName());

	private String connectString;
	private String user;
	private String password;
	
    public VirtuosoSessionFactory(String connectString, String user, String password) {
    	this.connectString = connectString;
    	this.user = user;
    	this.password = password;
    }

    // Implements TriplestoreSessionFactory.newSession()
    public TriplestoreSession newSession() throws TrippiException {
        return new VirtuosoSession(connectString, user, password);
    }

    // Implements TriplestoreSessionFactory.listTripleLanguages()
    public String[] listTripleLanguages() {
		//logger.error("THIS IS NOT error! listTripleLanguages");
        return VirtuosoSession.TRIPLE_LANGUAGES;
    }

    // Implements TriplestoreSessionFactory.listTupleLanguages()
    public String[] listTupleLanguages() {
        return VirtuosoSession.TUPLE_LANGUAGES;
    }

    // Implements TriplestoreSessionFactory.close()
    public void close() throws TrippiException {
    }

}
