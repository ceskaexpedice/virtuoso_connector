package cz.knav.virtuoso.trippi;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jrdf.graph.GraphElementFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trippi.RDFUtil;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.config.ConfigUtils;
import org.trippi.impl.base.AliasManager;
import org.trippi.impl.base.ConcurrentTriplestoreWriter;
import org.trippi.impl.base.MemUpdateBuffer;
import org.trippi.impl.base.SingleSessionPool;
import org.trippi.impl.base.TriplestoreSession;
import org.trippi.impl.base.TriplestoreSessionFactory;
import org.trippi.impl.base.TriplestoreSessionPool;
import org.trippi.impl.base.UpdateBuffer;

/*
Implementation of Trippi for „Virtuoso Open-Source Edition“

This implementation of Trippi for „Virtuoso Open-Source Edition“ (hereafter VOS)
I created to test if it is possible to use VOS as Fedora resource index (hereafter RI).

This implementation of Trippi for VOS consists of these files:

VirtuosoConnector.java, VirtuosoSessionFactory.java, 
VirtuosoSession.java, VirtuosoTripleIterator.java and TriplestoreConnector.xml

TriplestoreConnector.xml is my version (for VOS) of standard Fedora configuration file 
C:\fedora\server\config\spring\TriplestoreConnector.xml. How to use it is described 
in C:\fedora\server\config\spring\README.txt.

VOS has two Java APIs for RDF - „Virtuoso Sesame Provider“ and „Virtuoso Jena Provider“. 
In implementation of Trippi for VOS I used „Virtuoso Sesame Provider“.

This implementation of Trippi for VOS I created this way:
From Fedora Commons Repository on https://github.com/fcrepo I took several pieces of code 
from existing implementations of Trippi and also from older implementations of Trippi and 
than I changed and programmed things, which were necessary to change or to programm. Useful 
was for example also old (stopped - several years not maintained) implementation of Trippi 
for Sesame.

In source code I also left commented out my logging statements, which I was using this way:

		//logger.error("THIS IS NOT error! constructor VirtuosoSession - begin");

I changed or programmed only things, which were necessary for testing, if it is possible 
to use VOS as RI. First three things, which may be improved are – see TODO in next pieces 
of code, which I copied from this implementation of Trippi – they are edited here to fit 
here better:

TriplestoreSessionPool sessionPool = 
    //TODO Create something better than SingleSessionPool
    new SingleSessionPool(m_updateSession, 
        m_updateSession.listTupleLanguages(),
        m_updateSession.listTripleLanguages());

Next thing, which may be improved, is – see TODO:

public TripleIterator findTriples(String lang, String queryText)
        throws TrippiException {
    //logger.error(
       //"THIS IS NOT error! findTriples(String lang, String queryText) - begin");
    return new VirtuosoTripleIterator(repository, lang, queryText);
}

public TripleIterator findTriples(SubjectNode subject,
        PredicateNode predicate, ObjectNode object) throws TrippiException {
    //logger.error("THIS IS NOT error! findTriples(SubjectNode subject, - begin");
    return findTriples(SPARQL,
        null //TODO: For inserting triples, deleting triples and
             //SPARQL CONSTRUCT queries I didn't need this function
             //findTriples(SubjectNode subject,	PredicateNode predicate,
             //ObjectNode object)
        );
}

Third thing, which may be improved, is:

// Implements TriplestoreSession.query(String, String)
public TupleIterator query(String query,
                           String lang) throws TrippiException {
    // TODO: I didn't need this function, so I implemented it the same way as it
	// is implemented in Trippi for MPTstore.
    throw new TrippiException("Unsupported tuple query language: " + lang);
}

This implementation of Trippi for VOS I successfully tested on notebook (4 GB RAM, 
Intel Core i5-2450M CPU 2,5 GHz, Windows 7 64bit) with VOS 6.1.6 (it was latest version - today 
2013-08-23 VOS 7.0.0 released 2013-08-05 is latest) and Fedora Commons 3.5 and Java 1.6.

I successfully tried following with it:

•	Storing objects into RI VOS via user interface of Fedora C:\fedora\client\bin\fedora-ingest.bat
•	Deleting objects from RI VOS via user interface of Fedora C:\fedora\client\bin\fedora-admin.bat
•	SPARQL CONSTRUCT queries on data from RI VOS via user interface of Fedora 
	http://localhost:8080/fedora/risearch
	
Problem with „Virtuoso JDBC 4 Driver“:
When I was implementing this Trippi for VOS, there were two versions 
of „Virtuoso Sesame Provider“ - „Virtuoso Sesame 2 Provider“ (virt_sesame2.jar) and 
„Virtuoso Sesame 3 Provider“ (virt_sesame3.jar). When I wanted to use „Virtuoso JDBC 4 Driver“ 
(virtjdbc4.jar), that is type 4 JDBC driver, I must use „Virtuoso Sesame 3 Provider“ with it. 
But there were an incompatibility issue between „Virtuoso Sesame 3 Provider“ and Sesame 3 library 
„openrdf-sesame-3.0-alpha1-onejar.jar“ (it was latest, but to old version of Sesame 3 
library – there were newer Sesame 2 libraries). So it was impossible to use 
„Virtuoso JDBC 4 Driver“ – so I used type 3 JDBC driver „Virtuoso JDBC 3 Driver“ (virtjdbc3.jar) and 
„Virtuoso Sesame 2 Provider“ (virt_sesame2.jar) and „openrdf-sesame-2.6.5-onejar.jar“. I also 
think, that because of bad state of Sesame 3, it wouldn't be a good idea to use 
„Virtuoso Sesame 3 Provider“.

Today (2013-08-23) on 
http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VOSDownload there is no 
„Virtuoso Sesame 3 Provider“ – there are two versions 
of „Virtuoso Sesame Provider“ - „Virtuoso Sesame 2.6.x Provider“ and 
„Virtuoso Sesame 2.7.x Provider“. May be, that now it is possible to use „Virtuoso JDBC 4 Driver“ 
with one or both of them.
 */
public class VirtuosoConnector extends TriplestoreConnector {
	
	private static final Logger logger =
        LoggerFactory.getLogger(VirtuosoConnector.class.getName());

	private Map<String,String> m_config;
    private TriplestoreWriter m_readerWriter;
    private GraphElementFactory m_elementFactory = new RDFUtil();
    
    // where writes will occur, if this connector is writable
    private TriplestoreSession m_updateSession = null;
    
    private TriplestoreSessionFactory m_sessionFactory = null;
    
    //private TripleIteratorFactory m_iteratorFactory = null;
    
    private boolean m_isClosed = false;
    
	/**
	 * @see org.trippi.TriplestoreConnector#close() 
	 */
	@Override
	public void close() throws TrippiException {
		if (!m_isClosed) {
            logger.info("Connector closing...");
            if (m_readerWriter != null) {
                m_readerWriter.close();
            }
            if (m_sessionFactory != null) {
            	m_sessionFactory.close();
            }
            if (m_updateSession != null) {
                m_updateSession.close();
            }
            m_isClosed = true;
        }
	}

	/**
	 * @see org.trippi.TriplestoreConnector#getElementFactory() 
	 */
	@Override
	public GraphElementFactory getElementFactory() {
		return m_elementFactory;
	}

	/**
	 * @see org.trippi.TriplestoreConnector#getReader() 
	 */
	@Override
	public TriplestoreReader getReader() {
		return getWriter();
	}

	/**
	 * @see org.trippi.TriplestoreConnector#getWriter() 
	 */
	@Override
	public TriplestoreWriter getWriter() {
		if (m_readerWriter == null){
			try{
				open();
			}
			catch(TrippiException e){
				throwRuntimeException(e);
			}
		}
		return m_readerWriter;
	}

	/**
	 * @see org.trippi.TriplestoreConnector#init(Map) 
	 */
	@Deprecated
	@Override
	public void init(Map<String, String> config) throws TrippiException {
		setConfiguration(config);
	}

	/**
	 * @see org.trippi.TriplestoreConnector#setConfiguration(Map) 
	 */
	@Override
	public void setConfiguration(Map<String, String> config) throws TrippiException {
		Map<String,String> validated = new HashMap<String,String>(config);

		validated.put("connectString",ConfigUtils.getRequired(config, "connectString"));
        validated.put("user", ConfigUtils.getRequired(config, "user"));
        validated.put("password", ConfigUtils.getRequired(config, "password"));
    	
        validated.put("autoFlushDormantSeconds", Integer.toString(ConfigUtils.getRequiredNNInt(config, "autoFlushDormantSeconds")));
        
        int autoFlushBufferSize = ConfigUtils.getRequiredPosInt(config, "autoFlushBufferSize");
        validated.put("autoFlushBufferSize", Integer.toString(autoFlushBufferSize));
        
        int bufferSafeCapacity = ConfigUtils.getRequiredInt(config, "bufferSafeCapacity");
        if (bufferSafeCapacity < autoFlushBufferSize + 1) {
            throw new TrippiException("bufferSafeCapacity must be less than or equal to autoFlushBufferSize.");
        }
        validated.put("bufferSafeCapacity",Integer.toString(bufferSafeCapacity));

        int bufferFlushBatchSize = ConfigUtils.getRequiredPosInt(config, "bufferFlushBatchSize");
        if (bufferFlushBatchSize > autoFlushBufferSize) {
            throw new TrippiException("bufferFlushBatchSize must be less than or equal to autoFlushBufferSize.");
        }
        validated.put("bufferFlushBatchSize", Integer.toString(bufferFlushBatchSize));
        
        /*
        int poolInitialSize = ConfigUtils.getRequiredInt(config, "poolInitialSize");
        if (poolInitialSize > 0) {
            int poolMaxGrowth = ConfigUtils.getRequiredInt(config, "poolMaxGrowth");
            validated.put("poolMaxGrowth", Integer.toString(poolMaxGrowth));
            String temp = config.get("poolSpareSessions");
            int poolSpareSessions = (temp == null) ? 0 : Integer.parseInt(temp);
            validated.put("poolSpareSessions", Integer.toString(poolSpareSessions));
        }
        validated.put("poolInitialSize", Integer.toString(poolInitialSize));
        
        boolean autoCreate = false, autoTextIndex = false;

        if (!readOnly) {
        	autoCreate = ConfigUtils.getRequiredBoolean(config, "autoCreate");
            
            autoTextIndex = ConfigUtils.getRequiredBoolean(config, "autoTextIndex");
            
            if (autoTextIndex) {
              validated.put("textModelName", modelName + "-fullText");
            }
        }
        validated.put("autoCreate", Boolean.toString(autoCreate));
        validated.put("autoTextIndex", Boolean.toString(autoTextIndex));
        */
        
        m_config = validated;
    }
	
	/*
	@Override
	public void setTripleIteratorFactory(TripleIteratorFactory factory) {
	    this.m_iteratorFactory = factory;
	}
	*/
	
    /**
     * @see org.trippi.TriplestoreConnector#getConfiguration()
     */
    @Override
	public Map<String,String> getConfiguration(){
		return m_config;
	}
	
    /**
     * @see org.trippi.TriplestoreConnector#open()
     */
    @Override
    public void open() throws TrippiException {    
    	if (m_config == null){
    		throw new TrippiException("Cannot open " + getClass().getName() + " without valid configuration");
    	}
    	
        String connectString = m_config.get("connectString");
        String user = m_config.get("user");
        String password = m_config.get("password");
    	
    	/*
    	if (m_iteratorFactory == null) {
    	    m_iteratorFactory = TripleIteratorFactory.defaultInstance();
    	}
    	*/
    	
    	/*
		AliasManager aliasManager = new AliasManager(new HashMap<String, String>());

		boolean readOnly = Boolean.valueOf(m_config.get("readOnly"));
        //Mulgara location properties
		boolean remote = Boolean.valueOf(m_config.get("remote"));
        String serverName = m_config.get("serverName");
        String modelName = m_config.get("modelName");
        String textModelName = m_config.get("textModelName"); // will be null when autoTextIndex == false
        
        // connection pool configuration
        int poolInitialSize = Integer.parseInt(m_config.get("poolInitialSize"));
        int poolMaxGrowth = Integer.parseInt(m_config.get("poolMaxGrowth"));
        int poolSpareSessions = Integer.parseInt(m_config.get("poolSpareSessions"));
        boolean autoCreate = Boolean.valueOf(m_config.get("autoCreate"));
        */
        
        // buffer configuration
        int autoFlushBufferSize = Integer.parseInt(m_config.get("autoFlushBufferSize"));
        int bufferFlushBatchSize = Integer.parseInt(m_config.get("bufferFlushBatchSize"));
        int bufferSafeCapacity = Integer.parseInt(m_config.get("bufferSafeCapacity"));
        int autoFlushDormantSeconds = Integer.parseInt(m_config.get("autoFlushDormantSeconds"));
        
        VirtuosoSessionFactory sessionFactory = new VirtuosoSessionFactory(connectString, user, password);
        // construct the _updateSession, which is managed outside the pool
        m_updateSession = sessionFactory.newSession();
        // construct the TriplestoreSessionPool
        TriplestoreSessionPool sessionPool = //TODO Create something better than SingleSessionPool
        		new SingleSessionPool(m_updateSession, 
        				m_updateSession.listTupleLanguages(),
        				m_updateSession.listTripleLanguages());
        // construct the UpdateBuffer
        UpdateBuffer updateBuffer = new MemUpdateBuffer(bufferSafeCapacity,
                                                        bufferFlushBatchSize);
        // construct the TriplestoreWriter
        try {
			m_readerWriter = new ConcurrentTriplestoreWriter(sessionPool,
			                                          new AliasManager(new HashMap<String, String>()),
			                                          m_updateSession,
			                                          updateBuffer,
			                                          autoFlushBufferSize,
			                                          autoFlushDormantSeconds);
		} catch (IOException e) {
			throwTrippiException(e);
		}
        /*
        SynchronizedTriplestoreSession synchSession = new SynchronizedTriplestoreSession(
        		new VirtuosoSession());
        m_readerWriter = new SynchronizedTriplestoreWriter(synchSession,
                aliasManager,
                //m_iteratorFactory,
                15000);
        */
	}
	
	protected TriplestoreSessionFactory getSessionFactory() {
	    return m_sessionFactory;
	}

	private void throwTrippiException(Exception e) throws TrippiException {
		logger.error(e.getMessage(), e);
		throw new TrippiException(e.getMessage(), e);
	}
	private void throwRuntimeException(Exception e) {
		logger.error(e.getMessage(), e);
		throw new RuntimeException(e.getMessage(), e);
	}
	
}
