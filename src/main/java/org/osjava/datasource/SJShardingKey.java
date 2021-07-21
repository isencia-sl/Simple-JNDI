package org.osjava.datasource;

import java.sql.SQLType;
import java.sql.ShardingKey;

public class SJShardingKey implements ShardingKey {
  private Object subkey;
  private SQLType subkeyType;
  
  public SJShardingKey(Object subkey,SQLType subkeyType) {
    this.subkey = subkey;
    this.subkeyType = subkeyType;
  }

  public Object getSubkey() {
    return subkey;
  }

  public SQLType getSubkeyType() {
    return subkeyType;
  }
  
  @Override
  public String toString() {
    return subkey.toString();
  }
}
