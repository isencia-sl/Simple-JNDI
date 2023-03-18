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

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.ShardingKeyBuilder;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * A basic implementation of a DataSource with optional connection pooling.
 */
public class SJDataSource implements DataSource {
	private Properties properties;
	private String url;
	private String poolname;
	private boolean useSharding = false;
	private Map<String,DataSource> poolDataSources = new ConcurrentHashMap<>(5,0.9F,1);
    private volatile PrintWriter logWriter = new PrintWriter(new OutputStreamWriter(System.out,StandardCharsets.UTF_8));
    
    public SJDataSource(Properties properties) {
    	this.properties = properties;
    	
    	url = properties.getProperty("url");
    	if (url == null || url.isEmpty())
    		throw new IllegalArgumentException("No valid url is defined in the JNDI connection properties !");
    	poolname = properties.getProperty("poolname");
    	if (poolname == null || poolname.isEmpty())
    		throw new IllegalArgumentException("No valid poolname is defined in the JNDI connection properties !");
	
    	String driverClassName = properties.getProperty("driverClassName");
    	if (driverClassName == null || driverClassName.isEmpty())
    		throw new IllegalArgumentException("No valid driverClassName is defined in the JNDI connection properties !");
    
    	String useSharding = properties.getProperty("useSharding");
    	if (useSharding != null)
    		this.useSharding = useSharding.equalsIgnoreCase("true");
    }
    
    @Override
    public ConnectionBuilder createConnectionBuilder() throws SQLException {
   		return new SJConnectionBuilder(this);
    }
    
    @Override
    public ShardingKeyBuilder createShardingKeyBuilder() throws SQLException {
    	if (!useSharding)
    		throw new SQLFeatureNotSupportedException("This datasource does not use sharding !");
    	return new SJShardingKeyBuilder();
    }
    
    @Override
	public Connection getConnection() throws SQLException {
		return createConnectionBuilder().build();
	}
    
    @Override
	public Connection getConnection(String username, String password) throws SQLException {
        throw new UnsupportedOperationException("Not supported by SJDataSource");
	}
    
    public Map<String, DataSource> getPoolDataSources() {
		return poolDataSources;
	}

	@Override
	public int getLoginTimeout() throws SQLException {
        throw new UnsupportedOperationException("Not supported by SJDataSource");
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException {
		return logWriter;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
	}
	
	public String getPoolname() {
		return poolname;
	}
	
	public Properties getProperties() {
		return properties;
	}
	
	public String getUrl() {
		return url;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException("Not supported by SJDataSource");
	}

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {
		this.logWriter = logWriter;
		
		for (DataSource dataSource : poolDataSources.values())
			dataSource.setLogWriter(out);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("SJDataSource is not a wrapper.");
	}

	@Override
	public String toString() {
		return "SJDataSource{" +
				"url='" + url + '\'' +
				", poolname='" + poolname + '\'' +
				", useSharding=" + useSharding +
				'}';
	}
}

