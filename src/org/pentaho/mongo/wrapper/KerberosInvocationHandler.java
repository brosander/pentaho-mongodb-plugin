package org.pentaho.mongo.wrapper;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.wrapper.collection.KerberosMongoCollectionWrapper;

public class KerberosInvocationHandler implements InvocationHandler {
  private final AuthContext authContext;
  private final Object delegate;

  public KerberosInvocationHandler( AuthContext authContext, Object delegate ) {
    this.authContext = authContext;
    this.delegate = delegate;
  }

  @Override
  public Object invoke( Object proxy, final Method method, final Object[] args ) throws Throwable {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<Object>() {

        @Override
        public Object run() throws Exception {
          return method.invoke( delegate, args );
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof KettleException ) {
        throw (KettleException) e.getCause();
      } else {
        throw new KettleException( e.getCause() ); //$NON-NLS-1$
      }
    }
  }

  @SuppressWarnings( "unchecked" )
  public static <T> T wrap( Class<T> iface, AuthContext authContext, Object delegate ) {
    return (T) Proxy
        .newProxyInstance( KerberosInvocationHandler.class.getClassLoader(),
            new Class<?>[] { KerberosMongoCollectionWrapper.class }, new KerberosInvocationHandler( authContext,
                delegate ) );
  }
}
