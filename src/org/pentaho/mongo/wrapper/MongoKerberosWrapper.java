package org.pentaho.mongo.wrapper;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Set;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.KettleKerberosHelper;
import org.pentaho.mongo.wrapper.collection.KerberosMongoCollectionWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;
import org.pentaho.mongo.wrapper.field.MongoField;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoCredential;

public class MongoKerberosWrapper extends MongoUsernamePasswordWrapper {
  private static Class<?> PKG = MongoKerberosWrapper.class;
  private final AuthContext authContext;

  public MongoKerberosWrapper( MongoDbMeta meta, VariableSpace vars, LogChannelInterface log ) throws KettleException {
    super( meta, vars, log );
    authContext = new AuthContext( KettleKerberosHelper.login( vars, getUser() ) );
  }

  @Override
  protected MongoCredential getCredential( MongoDbMeta meta, VariableSpace vars ) {
    return MongoCredential.createGSSAPICredential( vars.environmentSubstitute( meta.getAuthenticationUser() ) );
  }

  @Override
  protected void authenticateWithDb( DB db ) throws KettleException {
    // noop
  }

  @Override
  public Set<String> getCollectionsNames( final String dB ) throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<Set<String>>() {

        @Override
        public Set<String> run() throws Exception {
          return MongoKerberosWrapper.super.getCollectionsNames( dB );
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.Message.Error.RetrieveCollectionNames", dB ), e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @Override
  public List<String> getIndexInfo( final String dbName, final String collection ) throws Exception {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<List<String>>() {

        @Override
        public List<String> run() throws Exception {
          return MongoKerberosWrapper.super.getIndexInfo( dbName, collection );
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.Error.RetrieveIndexInfo" ), //$NON-NLS-1$
            e.getCause() );
      }
    }
  }

  @Override
  public List<MongoField> discoverFields( final String db, final String collection, final String query,
      final String fields, final boolean isPipeline, final int docsToSample ) throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<List<MongoField>>() {

        @Override
        public List<MongoField> run() throws Exception {
          return MongoKerberosWrapper.super.discoverFields( db, collection, query, fields, isPipeline, docsToSample );
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.ErrorMessage.UnableToDiscoverFields" ), e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @Override
  public List<String> getDatabaseNames() throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<List<String>>() {

        @Override
        public List<String> run() throws Exception {
          return MongoKerberosWrapper.super.getDatabaseNames();
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.Error.RetrieveDbNames" ), e //$NON-NLS-1$
            .getCause() );
      }
    }
  }

  @Override
  public List<String> getAllTags() throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<List<String>>() {

        @Override
        public List<String> run() throws Exception {
          return MongoKerberosWrapper.super.getAllTags();
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "MongoKerberosWrapper.Message.Error.UnableToRetrieveTags" ), e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @Override
  public List<String> getReplicaSetMembersThatSatisfyTagSets( final List<DBObject> tagSets ) throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<List<String>>() {

        @Override
        public List<String> run() throws Exception {
          return MongoKerberosWrapper.super.getReplicaSetMembersThatSatisfyTagSets( tagSets );
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.ErrorMessage.UnableToGetReplicaSetMembers" ), e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @Override
  public List<String> getLastErrorModes() throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<List<String>>() {

        @Override
        public List<String> run() throws Exception {
          return MongoKerberosWrapper.super.getLastErrorModes();
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "MongoKerberosWrapper.Message.Error.UnableToRetrieveLastErrorModes" ), e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @Override
  public MongoCollectionWrapper createCollection( final String db, final String name ) throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<MongoCollectionWrapper>() {

        @Override
        public MongoCollectionWrapper run() throws Exception {
          return MongoKerberosWrapper.super.createCollection( db, name );
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "MongoKerberosWrapper.Message.Error.UnableToCreateCollection" ), e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @Override
  public MongoCollectionWrapper getCollection( final String db, final String name ) throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<MongoCollectionWrapper>() {

        @Override
        public MongoCollectionWrapper run() throws Exception {
          return MongoKerberosWrapper.super.getCollection( db, name );
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "MongoKerberosWrapper.Message.Error.UnableToRetrieveCollection" ), e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @Override
  protected MongoCollectionWrapper wrap( DBCollection collection ) {
    return new KerberosMongoCollectionWrapper( collection, authContext );
  }
}
