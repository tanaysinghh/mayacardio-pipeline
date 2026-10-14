package com.mayacardio.pipeline.cloud;

import com.mayacardio.pipeline.cloud.store.CloudStore;
import com.mayacardio.pipeline.cloud.store.JdbcCloudStore;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.Statement;

@EnabledIfEnvironmentVariable(named = "PG_TEST_URL", matches = ".+")
class PostgresCloudPipelineTest extends CloudPipelineTest {

    @Override
    protected CloudStore newStore() {
        String[] jdbc = CloudConfig.toJdbc(System.getenv("PG_TEST_URL"), "", "");
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(jdbc[0]);
        ds.setUser(jdbc[1]);
        ds.setPassword(jdbc[2]);
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS cloud_segments, cloud_losses, cloud_results, cloud_sessions");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        JdbcCloudStore store = new JdbcCloudStore(ds, JdbcCloudStore.Dialect.POSTGRES);
        store.init();
        return store;
    }
}
