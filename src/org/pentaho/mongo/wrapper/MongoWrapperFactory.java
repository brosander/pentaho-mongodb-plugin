package org.pentaho.mongo.wrapper;

import java.lang.reflect.Proxy;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;

public class MongoWrapperFactory {
  public static MongoWrapper createMongoWrapper( MongoDbMeta meta, VariableSpace vars, LogChannelInterface log )
    throws KettleException {
    if ( meta.getUseKerberosAuthentication() ) {
      MongoKerberosWrapper wrapper = new MongoKerberosWrapper( meta, vars, log );
      return (MongoWrapper) Proxy.newProxyInstance( wrapper.getClass().getClassLoader(),
          new Class<?>[] { MongoWrapper.class }, new KerberosInvocationHandler( wrapper.getAuthContext(), wrapper ) );
    } else if ( !Const.isEmpty( meta.getAuthenticationUser() ) || !Const.isEmpty( meta.getAuthenticationPassword() ) ) {
      return new MongoUsernamePasswordWrapper( meta, vars, log );
    } else {
      return new MongoNoAuthWrapper( meta, vars, log );
    }
  }
}
