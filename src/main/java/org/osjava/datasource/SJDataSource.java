/*
 * Copyright (c) 2005, Henri Yandell
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or 
 * without modification, are permitted provided that the 
 * following conditions are met:
 * 
 * + Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer.
 * 
 * + Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution.
 * 
 * + Neither the name of Simple-JNDI nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software 
 *   without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */

package org.osjava.datasource;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.ShardingKeyBuilder;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;

/**
 * A basic implementation of a DataSource with optional connection pooling.
 */
public class SJDataSource extends BasicDataSource {
    private Class<? extends ConnectionBuilder> connectionBuilderClass;
    private Class<? extends ShardingKeyBuilder> shardingKeyBuilderClass;
    
    public SJDataSource(BasicDataSource source) throws Exception {
    	// call all setters used in BasicDataSoureFactory.createDataSource()
    	setDefaultAutoCommit(source.getDefaultAutoCommit());
    	setDefaultReadOnly(source.getDefaultReadOnly());
    	setDefaultTransactionIsolation(source.getDefaultTransactionIsolation());
    	setDefaultCatalog(source.getDefaultCatalog());
    	setDefaultSchema(source.getDefaultSchema());
    	setCacheState(source.getCacheState());
    	setDriverClassName(source.getDriverClassName());
    	setLifo(source.getLifo());
    	setMaxTotal(source.getMaxTotal());
    	setMaxIdle(source.getMaxIdle());
    	setMinIdle(source.getMinIdle());
    	setInitialSize(source.getInitialSize());
    	setMaxWaitMillis(source.getMaxWaitMillis());
    	setTestOnCreate(source.getTestOnCreate());
    	setTestOnBorrow(source.getTestOnBorrow());
    	setTestOnReturn(source.getTestOnReturn());
    	setTimeBetweenEvictionRunsMillis(source.getTimeBetweenEvictionRunsMillis());
    	setNumTestsPerEvictionRun(source.getNumTestsPerEvictionRun());
    	setMinEvictableIdleTimeMillis(source.getMinEvictableIdleTimeMillis());
    	setSoftMinEvictableIdleTimeMillis(source.getSoftMinEvictableIdleTimeMillis());
    	setEvictionPolicyClassName(source.getEvictionPolicyClassName());
    	setTestWhileIdle(source.getTestWhileIdle());
    	setPassword(source.getPassword());
    	setUrl(source.getUrl());
    	setUsername(source.getUsername());
    	setValidationQuery(source.getValidationQuery());
    	setValidationQueryTimeout(source.getValidationQueryTimeout());
    	setAccessToUnderlyingConnectionAllowed(source.isAccessToUnderlyingConnectionAllowed());
    	setRemoveAbandonedOnBorrow(source.getRemoveAbandonedOnBorrow());
    	setRemoveAbandonedOnMaintenance(source.getRemoveAbandonedOnMaintenance());
    	setRemoveAbandonedTimeout(source.getRemoveAbandonedTimeout());
    	setLogAbandoned(source.getLogAbandoned());
    	setAbandonedUsageTracking(source.getAbandonedUsageTracking());
    	setPoolPreparedStatements(source.getAbandonedUsageTracking());
    	setClearStatementPoolOnReturn(source.isClearStatementPoolOnReturn());
    	setMaxOpenPreparedStatements(source.getDefaultTransactionIsolation());
    	setConnectionInitSqls(source.getConnectionInitSqls());
    	
    	// connectionProperties are not publicly accessible in BasicDataSource
    	Properties connectionProperties = null;
    	try {
    		Field propertiesField = BasicDataSource.class.getDeclaredField("connectionProperties");
    		propertiesField.trySetAccessible();
    		connectionProperties = (Properties)propertiesField.get(source);
    	} catch (Exception e) {
    		// ignore failure to access field (should not occur)
    	}
    	if (connectionProperties != null)
    		for (Entry<Object,Object> entry : connectionProperties.entrySet())
    			addConnectionProperty((String)entry.getKey(),(String)entry.getValue());
    	
    	setMaxConnLifetimeMillis(source.getMaxConnLifetimeMillis());
    	setLogExpiredConnections(source.getLogExpiredConnections());
    	setJmxName(source.getJmxName());
    	setAutoCommitOnReturn(source.getAutoCommitOnReturn());
    	setRollbackOnReturn(source.getRollbackOnReturn());
    	setDefaultQueryTimeout(source.getDefaultQueryTimeout());
    	setFastFailValidation(source.getFastFailValidation());
    	setDisconnectionSqlCodes(source.getDisconnectionSqlCodes());
    	setConnectionFactoryClassName(source.getConnectionFactoryClassName());

    	// make sure that initialSize connections are created
    	if (getInitialSize() > 0)
    		createDataSource();
	}
    
    @Override
    public ConnectionBuilder createConnectionBuilder() throws SQLException {
    	if (connectionBuilderClass != null) {
			Constructor<? extends ConnectionBuilder> constructor;
			try {
				constructor = connectionBuilderClass.getConstructor(SJDataSource.class);
				return constructor.newInstance(this);
			} catch (Exception e) {
				throw new SQLException("Unable to create ConnectionBuilder for '"+connectionBuilderClass.getName()+"': "+e.getMessage(),e);
			}
    	}
    		
    	return new SJConnectionBuilder(this).user(getUsername()).password(getPassword());
    }
    
    @Override
    public ShardingKeyBuilder createShardingKeyBuilder() throws SQLException {
    	if (shardingKeyBuilderClass != null) {
			Constructor<? extends ShardingKeyBuilder> constructor;
			try {
				constructor = shardingKeyBuilderClass.getConstructor();
				return constructor.newInstance();
			} catch (Exception e) {
				throw new SQLException("Unable to create ShardingKeyBuilder for '"+shardingKeyBuilderClass.getName()+"': "+e.getMessage(),e);
			}
    	}
    	
    	return new SJShardingKeyBuilder();
    }
    
    public void setConnectionBuilderClass(Class<? extends ConnectionBuilder> connectionBuilderClass) {
		this.connectionBuilderClass = connectionBuilderClass;
	}
    
    public void setShardingKeyBuilderClass(Class<? extends ShardingKeyBuilder> shardingKeyBuilderClass) {
		this.shardingKeyBuilderClass = shardingKeyBuilderClass;
	}
}

