package org.pentaho.mongo.wrapper.cursor;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.mongo.AuthContext;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.ServerAddress;

public class KerberosMongoCursorWrapper extends DefaultCursorWrapper {
  private final AuthContext authContext;

  public KerberosMongoCursorWrapper( DBCursor cursor, AuthContext authContext ) {
    super( cursor );
    this.authContext = authContext;
  }

  @Override
  public boolean hasNext() throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<Boolean>() {

        @Override
        public Boolean run() throws Exception {
          return KerberosMongoCursorWrapper.super.hasNext();
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
  public DBObject next() throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<DBObject>() {

        @Override
        public DBObject run() throws Exception {
          return KerberosMongoCursorWrapper.super.next();
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
  public ServerAddress getServerAddress() throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<ServerAddress>() {

        @Override
        public ServerAddress run() throws Exception {
          return KerberosMongoCursorWrapper.super.getServerAddress();
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
  public void close() throws KettleException {
    // TODO Auto-generated method stub
    super.close();
  }

  @Override
  public MongoCursorWrapper limit( final int i ) throws KettleException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<MongoCursorWrapper>() {

        @Override
        public MongoCursorWrapper run() throws Exception {
          return KerberosMongoCursorWrapper.super.limit( i );
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
