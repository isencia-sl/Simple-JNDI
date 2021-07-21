package org.osjava.datasource;

import java.sql.Connection;
import java.sql.ConnectionBuilder;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ShardingKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.text.StrSubstitutor;

public class SJConnectionBuilder implements ConnectionBuilder {
  private String url;
  private String username;
  private String password;
  private Properties properties;
  private ShardingKey shardingKey;
  private ShardingKey superShardingKey;
  private Map<String,String> poolUrls = null;
  
  public SJConnectionBuilder(SJDataSource dataSource) {
    this.url = dataSource.getUrl();
    this.properties = dataSource.getProperties();
  }

  @Override
  public ConnectionBuilder user(String username) {
    if (username != null)
      this.username = username;
    return this;
  }

  @Override
  public ConnectionBuilder password(String password) {
    if (password != null)
      this.password = password;
    return this;
  }

  @Override
  public ConnectionBuilder shardingKey(ShardingKey shardingKey) {
    this.shardingKey = shardingKey;
    return this;
  }

  @Override
  public ConnectionBuilder superShardingKey(ShardingKey superShardingKey) {
    this.superShardingKey = superShardingKey;
    return this;
  }

  @Override
  public Connection build() throws SQLException {
    String url = this.url;
    String pool = properties.getProperty("pool");

    // replace shardingKey in URL if it is referenced
    if (shardingKey != null) {
      Map<String,String> substitutes = new HashMap<>();
      substitutes.put("shardingKey",shardingKey.toString());
      String substitutedUrl = StrSubstitutor.replace(url,substitutes);
      if (substitutedUrl != url && !substitutedUrl.equals(url)) {
    	  url = substitutedUrl;
          // if we have a unique URL per shardingKey and pooling is requested, use that key as the poolName 
    	  if (pool != null)
    		  pool = shardingKey.toString();
      }
    }

    if (pool != null) {  // we want a connection name named like the pool property
        if (poolUrls == null)
        	poolUrls = new HashMap<>();
    	String poolUrl = poolUrls.get(pool);
        synchronized (SJDataSource.class) {
        	if (poolUrl == null) {  // we didn't create a connection pool already, so do it now
                PoolSetup.setupConnection(pool, url, username, password, properties);
                poolUrls.put(pool,poolUrl = PoolSetup.getUrl(pool));
            }
        }
        url = poolUrl;  // url is now a pooling link
    }

    if (username == null || password == null) 
        return DriverManager.getConnection(url);
    
    return DriverManager.getConnection(url, username, password);
  }
}
