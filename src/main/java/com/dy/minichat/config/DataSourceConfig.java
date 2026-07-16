package com.dy.minichat.config;

import com.dy.minichat.component.replica.MasterReplicaKeyRouter;
import com.dy.minichat.component.replica.RoutingDataSource;
import com.dy.minichat.property.DataSourceProperty;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
@Configuration
public class DataSourceConfig {

    private final DataSourceProperty dataSourceProperty;

    // 로그 찍기용 createDataSource
    public DataSource createDataSource(String key, String url, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(20);
        config.setMinimumIdle(20);
        config.setInitializationFailTimeout(-1); // DB 준비될 때까지 기다림
        return new HikariDataSource(config);
    }

    @Bean
    public DataSource dataSource() {
        // System.out.println("---> ReplicaDataSourceConfig dataSource()");

        Map<Object, Object> dataSourceMap = new HashMap<>();
        MasterReplicaKeyRouter masterReplicaKeyRouter = new MasterReplicaKeyRouter();

        DataSource masterDataSource = createDataSource(
                dataSourceProperty.getMaster().getKey(),
                dataSourceProperty.getMaster().getUrl(),
                dataSourceProperty.getUsername(),
                dataSourceProperty.getPassword());
        dataSourceMap.put(dataSourceProperty.getMaster().getKey(), masterDataSource);
        masterReplicaKeyRouter.setMasterKey(dataSourceProperty.getMaster().getKey());

        dataSourceProperty.getReplicas().forEach(databaseInfo -> {
            DataSource replicaDataSource = createDataSource(
                    databaseInfo.getKey(),
                    databaseInfo.getUrl(),
                    dataSourceProperty.getUsername(),
                    dataSourceProperty.getPassword());
            dataSourceMap.put(databaseInfo.getKey(), replicaDataSource);
            masterReplicaKeyRouter.addReplicaKey(databaseInfo.getKey());
        });

        // return new RoutingDataSource(dataSourceMap, masterSlaveKeyRouter);
        return new LazyConnectionDataSourceProxy(new RoutingDataSource(dataSourceMap, masterReplicaKeyRouter));
    }
}