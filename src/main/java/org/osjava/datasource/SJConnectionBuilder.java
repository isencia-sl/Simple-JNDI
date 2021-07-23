package org.osjava.datasource;

import java.sql.Connection;
import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.ShardingKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.lang.text.StrSubstitutor;

public class SJConnectionBuilder implements ConnectionBuilder {
	private SJDataSource dataSource;
	private String username = null;
	private String password = null;
	private ShardingKey shardingKey = null;

	public SJConnectionBuilder(SJDataSource dataSource) {
		this.dataSource = dataSource;
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
		return this;
	}

	@Override
	public Connection build() throws SQLException {
		String url = dataSource.getUrl();
		String poolname = dataSource.getPoolname();
		
		// replace shardingKey in URL if it is referenced
		if (shardingKey != null) {
			Map<String, String> substitutes = new HashMap<>();
			substitutes.put("shardingKey", shardingKey.toString());
			String substitutedUrl = StrSubstitutor.replace(url, substitutes);
			if (substitutedUrl != url && !substitutedUrl.equals(url)) {
				url = substitutedUrl;
				// if we have a unique URL per shardingKey, use that key as the poolname
				poolname = shardingKey.toString();
			}
		}

		// lookup (or create) the BasicDataSource for the resolved poolname 
		Map<String,DataSource> poolDataSources = dataSource.getPoolDataSources();
		DataSource poolDataSource = poolDataSources.get(poolname);
		if (poolDataSource == null) {
			Properties properties = new Properties(dataSource.getProperties());
			properties.put("url",url);
			if (username != null)
				properties.put("username",username);
			if (password != null)
				properties.put("password",password);
			try {
				poolDataSource = BasicDataSourceFactory.createDataSource(dataSource.getProperties());
			} catch (Exception e) {
				throw new SQLException("Unable to create DataSource for '"+poolname+"': "+e.getMessage(),e);
			}
			poolDataSource.setLogWriter(dataSource.getLogWriter());
			poolDataSources.put(poolname,poolDataSource);
		}

		return poolDataSource.getConnection();
	}
}
