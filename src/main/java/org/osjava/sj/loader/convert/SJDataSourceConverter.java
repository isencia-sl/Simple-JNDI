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

package org.osjava.sj.loader.convert;

import org.osjava.datasource.SJDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SJDataSourceConverter implements ConverterIF {

    private static Logger LOGGER = LoggerFactory.getLogger(SJDataSourceConverter.class);

    public Object convert(Properties properties, String type) {
        String driverName = properties.getProperty("driver");
        String url = properties.getProperty("url");
        String user = properties.getProperty("user");
        String password = properties.getProperty("password");

        if (driverName == null) {
            LOGGER.error("Incomplete arguments provided: properties={} type={}", properties, type);
            throw new IllegalArgumentException("Required subelement 'driver'");
        }
        if (url == null) {
            LOGGER.error("Incomplete arguments provided: properties={} type={}", properties, type);
            throw new IllegalArgumentException("Required subelement 'url'");
        }
        if (user == null) {
            LOGGER.error("Incomplete arguments provided: properties={} type={}", properties, type);
            throw new IllegalArgumentException("Required subelement 'user'");
        }
        if (password == null) {
            LOGGER.error("Incomplete arguments provided: properties={} type={}", properties, type);
            throw new IllegalArgumentException("Required subelement 'password'");
        }
        // IMPROVE Make Simple-JNDI independant from org.osjava.datasource
        return new SJDataSource(driverName, url, user, password, properties);
    }

}
