package org.pentaho.mongo.wrapper.collection;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.wrapper.cursor.KerberosMongoCursorWrapper;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

public class KerberosMongoCollectionWrapper extends DefaultMongoCollectionWrapper {
  private static Class<?> PKG = KerberosMongoCollectionWrapper.class;
  private final AuthContext authContext;

  public KerberosMongoCollectionWrapper( DBCollection collection, AuthContext authContext ) {
    super( collection );
    this.authContext = authContext;
  }

  @Override
  public MongoCursorWrapper find( final DBObject dbObject, final DBObject dbObject2 ) throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<MongoCursorWrapper>() {

        @Override
        public MongoCursorWrapper run() throws Exception {
          return KerberosMongoCollectionWrapper.super.find( dbObject, dbObject2 );
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "KerberosMongoCollectionWrapper.Message.Error.UnableToRetrieveCollection" ), e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @Override
  public AggregationOutput aggregate( final DBObject firstP, final DBObject[] remainder ) throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<AggregationOutput>() {

        @Override
        public AggregationOutput run() throws Exception {
          return KerberosMongoCollectionWrapper.super.aggregate( firstP, remainder );
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "KerberosMongoCollectionWrapper.Message.Error.UnableToAggregateCollection" ), e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @Override
  public MongoCursorWrapper find() throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<MongoCursorWrapper>() {

        @Override
        public MongoCursorWrapper run() throws Exception {
          return KerberosMongoCollectionWrapper.super.find();
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "KerberosMongoCollectionWrapper.Message.Error.UnableToFindCollection" ), e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @Override
  public void drop() throws KettleException {
    try {
      authContext.doAs( new PrivilegedExceptionAction<Void>() {

        @Override
        public Void run() throws Exception {
          KerberosMongoCollectionWrapper.super.drop();
          return null;
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "KerberosMongoCollectionWrapper.Message.Error.UnableToDropCollection" ), e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @Override
  public WriteResult update( final DBObject updateQuery, final DBObject insertUpdate, final boolean upsert,
      final boolean multi ) throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<WriteResult>() {

        @Override
        public WriteResult run() throws Exception {
          return KerberosMongoCollectionWrapper.super.update( updateQuery, insertUpdate, upsert, multi );
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "KerberosMongoCollectionWrapper.Message.Error.UnableToUpdateCollection" ), e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @Override
  public WriteResult insert( final List<DBObject> m_batch ) throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<WriteResult>() {

        @Override
        public WriteResult run() throws Exception {
          return KerberosMongoCollectionWrapper.super.insert( m_batch );
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( "Unable to insert collection from MongoDB", e.getCause() );
      }
    }
  }

  @Override
  public MongoCursorWrapper find( final DBObject query ) throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<MongoCursorWrapper>() {

        @Override
        public MongoCursorWrapper run() throws Exception {
          return KerberosMongoCollectionWrapper.super.find( query );
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( "Unable to retrieve collection from MongoDB", e.getCause() );
      }
    }
  }

  @Override
  public void dropIndex( final BasicDBObject mongoIndex ) throws KettleException {
    try {
      authContext.doAs( new PrivilegedExceptionAction<Void>() {

        @Override
        public Void run() throws Exception {
          KerberosMongoCollectionWrapper.super.dropIndex( mongoIndex );
          return null;
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( "Unable to retrieve collection from MongoDB", e.getCause() );
      }
    }
  }

  @Override
  public void createIndex( final BasicDBObject mongoIndex ) throws KettleException {
    try {
      authContext.doAs( new PrivilegedExceptionAction<Void>() {

        @Override
        public Void run() throws Exception {
          KerberosMongoCollectionWrapper.super.createIndex( mongoIndex );
          return null;
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( "Unable to retrieve collection from MongoDB", e.getCause() );
      }
    }
  }

  @Override
  protected MongoCursorWrapper wrap( DBCursor cursor ) {
    return new KerberosMongoCursorWrapper( cursor, authContext );
  }
}
