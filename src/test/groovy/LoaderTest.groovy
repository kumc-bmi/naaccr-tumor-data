import groovy.json.JsonOutput
import groovy.sql.GroovyResultSet
import groovy.sql.Sql
import groovy.transform.CompileStatic


@CompileStatic
class LoaderTest extends GroovyTestCase {
    Map<String, String> env1 = [A1_URL: 'jdbc:hsqldb:mem:A1', A1_DRIVER: 'org.hsqldb.jdbc.JDBCDriver', A1_USER: 'SA', A1_PASSWORD: '']

    void 'test DBConfig happy path'() {
        def config = Loader.DBConfig.fromEnv('A1', { String it -> env1[it] })
        Sql.withInstance(config.url, config.username, config.password.value, config.driver) { Sql sql ->
            sql.eachRow("select 1 as c from (values('x'))") { GroovyResultSet row ->
                assert row.getAt('c') == 1
            }
        }
    }

    void 'test DBConfig misspelled env var'() {
        def env1 = [A1_URL: 'URL1', A1_DRIVER: 'java.sql.DriverManager', A1_USERNAME: 'U1', A1_PASSWORD: 'sekret']
        shouldFail IllegalStateException, { -> Loader.DBConfig.fromEnv('A1', { String it -> env1[it] }) }
    }

    void 'test missing driver'() {
        def env1 = [A1_URL: 'URL1', A1_DRIVER: 'sqlserver.Driver.Thingy', A1_USERNAME: 'U1', A1_PASSWORD: 'sekret']
        shouldFail ClassNotFoundException, { -> Loader.DBConfig.fromEnv('A1', { String it -> env1[it] }) }
    }

    void 'test Loader runScript'() {
        def config = Loader.DBConfig.fromEnv('A1', { String it -> env1[it] })
        Sql.withInstance(config.url, config.username, config.password.value, config.driver) { Sql sql ->
            def input = getClass().getResource('naaccr_tables.sql')
            def loader = new Loader(sql)
            loader.runScript(input)
        }
    }

    void 'test Loader query'() {
        def config = Loader.DBConfig.fromEnv('A1', { String it -> env1[it] })
        Sql.withInstance(config.url, config.username, config.password.value, config.driver) { Sql sql ->
            def loader = new Loader(sql)
            loader.runScript(getClass().getResource('naaccr_tables.sql'))
            loader.runScript(getClass().getResource('naaccr_query_data.sql'))

            // query for each datatype
            def tumors = new StringWriter()
            tumors << loader.query('select tumor_id, dateOfDiagnosis, primarySite, ageAtDiagnosis from naaccr_tumors')
            assert tumors.toString() == '[{"TUMOR_ID":11,"DATEOFDIAGNOSIS":"2010-01-01T06:00:00+0000","PRIMARYSITE":"650","AGEATDIAGNOSIS":60}]'

            def stats = new StringWriter()
            stats << loader.query('select sectionId, dx_yr, naaccrId, mean from naaccr_export_stats')
            assert stats.toString() == '[{"SECTIONID":1,"DX_YR":2014,"NAACCRID":"tumorSizeClinical","MEAN":3.4000}]'
        }
    }

    void 'test Loader loading'() {
        def buf = new StringWriter()
        buf << JsonOutput.toJson([sql: "insert into naaccr_tumors (tumor_id, dateOfDiagnosis, primarySite) values (?, ?, ?)"]) << "\n"
        for (record in [[1, "2001-01-01", "C650"],
                        [2, "2002-02-02", "C650"],
                        [3, "2003-02-02", "C650"]]) {
            buf << JsonOutput.toJson(record) << "\n"
        }
        def input = new StringReader(buf.toString())

        def config = Loader.DBConfig.fromEnv('A1', { String it -> env1[it] })
        Sql.withInstance(config.url, config.username, config.password.value, config.driver) { Sql sql ->
            sql.execute "delete from naaccr_tumors"
            def loader = new Loader(sql)
            loader.load(input)

            def results = new StringWriter()
            results << loader.query('select sum(tumor_id) tot from naaccr_tumors')
            assert results.toString() == '[{"TOT":6}]'
        }
    }
}
