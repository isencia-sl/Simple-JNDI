package org.osjava.datasource;

import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.ShardingKey;
import java.sql.ShardingKeyBuilder;

public class SJShardingKeyBuilder implements ShardingKeyBuilder {

  private Object subkey;
  private SQLType subkeyType;
  
  public SJShardingKeyBuilder() {
  }

  @Override
  public ShardingKeyBuilder subkey(Object subkey, SQLType subkeyType) {
    this.subkey = subkey;
    this.subkeyType = subkeyType;
    return this;
  }

  @Override
  public ShardingKey build() throws SQLException {
    return new SJShardingKey(subkey,subkeyType);
  }

}
