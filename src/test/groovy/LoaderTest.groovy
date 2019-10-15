import groovy.transform.CompileStatic
import groovy.sql.GroovyResultSet
import groovy.sql.Sql
import org.hsqldb.jdbc.JDBCDataSource


@CompileStatic
class DBConfigTest extends GroovyTestCase {
    Map<String, String> env1 = [A1_URL: 'jdbc:hsqldb:mem:A1', A1_DRIVER: 'org.hsqldb.jdbc.JDBCDriver', A1_USER: 'SA', A1_PASSWORD: '']
    void 'test DBConfig happy path' () {
        def config = DBConfig.fromEnv('A1', { String it -> env1[it] })
        Sql.withInstance(config.url, config.username, config.password.value, config.driver) { Sql sql ->
            sql.eachRow("select 1 as c from (values('x'))") { GroovyResultSet row ->
                assert row.getAt('c') == 1
            }
        }
    }

    void 'test DBConfig misspelled env var' () {
        def env1 = [A1_URL: 'URL1', A1_DRIVER: 'java.sql.DriverManager', A1_USERNAME: 'U1', A1_PASSWORD: 'sekret']
        shouldFail IllegalStateException, { -> DBConfig.fromEnv('A1', { String it -> env1[it] }) }
    }

    void 'test missing driver' () {
        def env1 = [A1_URL: 'URL1', A1_DRIVER: 'sqlserver.Driver.Thingy', A1_USERNAME: 'U1', A1_PASSWORD: 'sekret']
        shouldFail ClassNotFoundException, { -> DBConfig.fromEnv('A1', { String it -> env1[it] }) }
    }

    void 'test Loader runScript' () {
        def config = DBConfig.fromEnv('A1', { String it -> env1[it] })
        Sql.withInstance(config.url, config.username, config.password.value, config.driver) { Sql sql ->
            def input = getClass().getResource('naaccr_tables.sql')
            def loader = new Loader(sql)
            loader.runScript(input)
        }
    }
}
