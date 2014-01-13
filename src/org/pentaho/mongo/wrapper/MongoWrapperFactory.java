package org.pentaho.mongo.wrapper;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;

public class MongoWrapperFactory {
  public static MongoWrapper createMongoWrapper( MongoDbMeta meta, VariableSpace vars, LogChannelInterface log )
    throws KettleException {
    if ( meta.getUseKerberosAuthentication() ) {
      return new MongoKerberosWrapper( meta, vars, log );
    } else if ( !Const.isEmpty( meta.getAuthenticationUser() ) || !Const.isEmpty( meta.getAuthenticationPassword() ) ) {
      return new MongoUsernamePasswordWrapper( meta, vars, log );
    } else {
      return new MongoNoAuthWrapper( meta, vars, log );
    }
  }
}
