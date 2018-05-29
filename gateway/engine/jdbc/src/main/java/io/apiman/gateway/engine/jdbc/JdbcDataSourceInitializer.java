package io.apiman.gateway.engine.jdbc;

import org.apache.commons.dbcp.BasicDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

public class JdbcDataSourceInitializer {
    private Map<String, String> config;
    private DataSource ds;
    private static JdbcDataSourceInitializer jdsi;
    private JdbcDataSourceInitializer(Map<String, String> config) {
        this.config = config;
    }

    /**
     * The JdbcDataSourceInstance is Singleton
     * @param config configurations of dbcp
     */
    public static JdbcDataSourceInitializer getInstance(Map<String, String> config){
        if (jdsi == null) {
            jdsi = new JdbcDataSourceInitializer(config);
        }
        return jdsi;
    }

    public synchronized DataSource getDatasource() {
        if (ds == null) {
            BasicDataSource ds = new BasicDataSource();
            if (config != null && config.containsKey("driverClassName")
                    && config.containsKey("url") && config.containsKey("username") && config.containsKey("password")) {
//                Iterator<Map.Entry<String, String>> iter = config.entrySet().iterator();
//                Map.Entry<String, String> tempEntry;
//                while (iter.hasNext()) {
//                    tempEntry = iter.next();
//                    System.out.println("k:" + tempEntry.getKey() + ",v:" + tempEntry.getValue());
//                    ds.addConnectionProperty(tempEntry.getKey(), tempEntry.getValue());
//                }
//                ds.setDriverClassName("com.mysql.jdbc.Driver");
//                ds.setUrl("jdbc:mysql://10.200.10.40:3306/apiman_gw");
//                ds.setUsername("apiman_gw");
//                ds.setPassword("!@#123qwe");
//                ds.setValidationQuery("select 1");
                ds.setDriverClassName(config.get("driverClassName"));
                ds.setUrl(config.get("url"));
                ds.setUsername(config.get("username"));
                ds.setPassword(config.get("password"));

                jdsi.setProperties(ds, config);

                try {
                    ds.getConnection();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                this.ds = ds;
            } else {
                throw new RuntimeException("Required config parameters:driverClassName,url,username,password.");
            }
        }
        return ds;
    }

    private void setProperties(BasicDataSource ds, Map<String,String> config) {
        if (config.containsKey("accessToUnderlyingConnectionAllowed")){
            ds.setAccessToUnderlyingConnectionAllowed(Boolean.parseBoolean(config.get("accessToUnderlyingConnectionAllowed")));
        }
        if (config.containsKey("connectionProperties")){
            ds.setConnectionProperties(config.get("connectionProperties"));
        }
        if (config.containsKey("defaultAutoCommit")){
            ds.setDefaultAutoCommit(Boolean.parseBoolean(config.get("defaultAutoCommit")));
        }
        if (config.containsKey("defaultCatalog")){
            ds.setDefaultCatalog(config.get("defaultCatalog"));
        }
        if (config.containsKey("defaultReadOnly")){
            ds.setDefaultReadOnly(Boolean.parseBoolean(config.get("defaultReadOnly")));
        }
        if (config.containsKey("defaultTransactionIsolation")){
            ds.setDefaultTransactionIsolation(Integer.parseInt(config.get("defaultTransactionIsolation")));
        }
        if (config.containsKey("initialSize")){
            ds.setInitialSize(Integer.parseInt(config.get("initialSize")));
        }
        if (config.containsKey("logAbandoned")){
            ds.setLogAbandoned(Boolean.parseBoolean(config.get("logAbandoned")));
        }
        if (config.containsKey("loginTimeout")){
            try {
                ds.setLoginTimeout(Integer.parseInt(config.get("loginTimeout")));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (config.containsKey("maxActive")){
            ds.setMaxActive(Integer.parseInt(config.get("maxActive")));
        }
        if (config.containsKey("maxIdle")){
            ds.setMaxIdle(Integer.parseInt(config.get("maxIdle")));
        }
        if (config.containsKey("maxOpenStatements")){
            ds.setMaxOpenPreparedStatements(Integer.parseInt(config.get("maxOpenStatements")));
        }
        if (config.containsKey("maxWait")){
            ds.setMaxWait(Long.parseLong(config.get("maxWait")));
        }
        if (config.containsKey("minEvictableIdleTimeMillis")){
            ds.setMinEvictableIdleTimeMillis(Long.parseLong(config.get("minEvictableIdleTimeMillis")));
        }
        if (config.containsKey("minIdle")){
            ds.setMinIdle(Integer.parseInt(config.get("minIdle")));
        }
        if (config.containsKey("numTestsPerEvictionRun")){
            ds.setNumTestsPerEvictionRun(Integer.parseInt(config.get("numTestsPerEvictionRun")));
        }
        if (config.containsKey("poolingStatements")){
            ds.setPoolPreparedStatements(Boolean.parseBoolean(config.get("poolingStatements")));
        }
        if (config.containsKey("removeAbandoned")){
            ds.setRemoveAbandoned(Boolean.parseBoolean(config.get("removeAbandoned")));
        }
        if (config.containsKey("removeAbandonedTimeout")){
            ds.setRemoveAbandonedTimeout(Integer.parseInt(config.get("removeAbandonedTimeout")));
        }
        if (config.containsKey("testOnBorrow")){
            ds.setTestOnBorrow(Boolean.parseBoolean(config.get("testOnBorrow")));
        }
        if (config.containsKey("testOnReturn")){
            ds.setTestOnReturn(Boolean.parseBoolean(config.get("testOnReturn")));
        }
        if (config.containsKey("testWhileIdle")){
            ds.setTestWhileIdle(Boolean.parseBoolean(config.get("testWhileIdle")));
        }
        if (config.containsKey("timeBetweenEvictionRunsMillis")){
            ds.setTimeBetweenEvictionRunsMillis(Long.parseLong(config.get("timeBetweenEvictionRunsMillis")));
        }
        if (config.containsKey("validationQuery")){
            ds.setValidationQuery(config.get("validationQuery"));
//            System.out.println("Thread ID:" + Thread.currentThread().getId() + ",query:" + config.get("validationQuery"));
        }
        if (config.containsKey("validationQueryTimeout")){
            ds.setValidationQueryTimeout(Integer.parseInt(config.get("validationQueryTimeout")));
        }
    }
}
