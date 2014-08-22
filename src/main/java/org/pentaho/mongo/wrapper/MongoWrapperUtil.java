package org.pentaho.mongo.wrapper;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;

/**
 * Created by bryan on 8/7/14.
 */
public class MongoWrapperUtil {
  private static MongoWrapperClientFactory mongoWrapperClientFactory = new MongoWrapperClientFactory() {
    @Override public MongoClientWrapper createMongoClientWrapper( MongoProperties props, MongoUtilLogger log )
      throws MongoDbException {
      return MongoClientWrapperFactory.createMongoClientWrapper( props, log );
    }
  };

  protected static void setMongoWrapperClientFactory( MongoWrapperClientFactory mongoWrapperClientFactory ) {
    MongoWrapperUtil.mongoWrapperClientFactory = mongoWrapperClientFactory;
  }

  protected static MongoWrapperClientFactory getMongoWrapperClientFactory() {
    return mongoWrapperClientFactory;
  }

  public static MongoClientWrapper createMongoClientWrapper( MongoDbMeta mongoDbMeta, VariableSpace vars,
                                                             LogChannelInterface log ) throws MongoDbException {
    MongoProperties.Builder propertiesBuilder = new MongoProperties.Builder();
    setIfNotNull( propertiesBuilder, MongoProp.HOST, vars.environmentSubstitute( mongoDbMeta.getHostnames() ) );
    setIfNotNull( propertiesBuilder, MongoProp.PORT, vars.environmentSubstitute( mongoDbMeta.getPort() ) );
    setIfNotNull( propertiesBuilder, MongoProp.DBNAME, vars.environmentSubstitute( mongoDbMeta.getDbName() ) );
    setIfNotNull( propertiesBuilder, MongoProp.connectTimeout, mongoDbMeta.getConnectTimeout() );
    setIfNotNull( propertiesBuilder, MongoProp.socketTimeout, mongoDbMeta.getSocketTimeout() );
    setIfNotNull( propertiesBuilder, MongoProp.readPreference, mongoDbMeta.getReadPreference() );
    setIfNotNull( propertiesBuilder, MongoProp.writeConcern, mongoDbMeta.getWriteConcern() );
    setIfNotNull( propertiesBuilder, MongoProp.wTimeout, mongoDbMeta.getWTimeout() );
    setIfNotNull( propertiesBuilder, MongoProp.JOURNALED, Boolean.toString( mongoDbMeta.getJournal() ) );
    setIfNotNull( propertiesBuilder, MongoProp.USE_ALL_REPLICA_SET_MEMBERS,
      Boolean.toString( mongoDbMeta.getUseAllReplicaSetMembers() ) );
    setIfNotNull( propertiesBuilder, MongoProp.USERNAME, mongoDbMeta.getAuthenticationUser() );
    setIfNotNull( propertiesBuilder, MongoProp.PASSWORD, mongoDbMeta.getAuthenticationPassword() );
    setIfNotNull( propertiesBuilder, MongoProp.USE_KERBEROS,
      Boolean.toString( mongoDbMeta.getUseKerberosAuthentication() ) );
    if ( mongoDbMeta.getReadPrefTagSets() != null ) {
      StringBuilder tagSet = new StringBuilder();
      for ( String tag : mongoDbMeta.getReadPrefTagSets() ) {
        tagSet.append( tag );
        tagSet.append( "," );
      }
      // Remove trailing comma
      if ( tagSet.length() > 0 ) {
        tagSet.setLength( tagSet.length() - 1 );
      }
      setIfNotNull( propertiesBuilder, MongoProp.tagSet, tagSet.toString() );
    }
    return mongoWrapperClientFactory
      .createMongoClientWrapper( propertiesBuilder.build(), new KettleMongoUtilLogger( log ) );
  }

  private static void setIfNotNull( MongoProperties.Builder builder, MongoProp prop, String value ) {
    if ( value != null ) {
      builder.set( prop, value );
    }
  }
}
