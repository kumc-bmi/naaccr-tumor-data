import groovy.json.JsonOutput
import groovy.sql.GroovyResultSet
import groovy.sql.Sql
import groovy.transform.CompileStatic
import junit.framework.TestCase

import static groovy.test.GroovyAssert.shouldFail

@CompileStatic
class LoaderTest extends TestCase {
    void 'inMemoryDB supports select'() {
        def account = DBConfig.inMemoryDB("A1")
        account.withSql { Sql sql ->
            sql.eachRow("select 1 as c from (values('x'))") { GroovyResultSet row ->
                assert row['c'] == 1
            }
        }
    }

    static final Map<String, String> env1 = [A1_URL: 'jdbc:h2:mem:A1;create=true', A1_DRIVER: 'org.h2.Driver',
                                             A1_USER: 'SA', A1_PASSWORD: '']

    void 'test DBConfig misspelled env var'() {
        def env1 = [A1_URL: 'URL1', A1_DRIVER: 'java.sql.DriverManager', A1_USERNAME: 'U1', A1_PASSWORD: 'sekret']
        shouldFail IllegalStateException, { -> DBConfig.fromEnv('A1', { String it -> env1[it] }) }
    }

    void 'test missing driver'() {
        def env1 = [A1_URL: 'URL1', A1_DRIVER: 'sqlserver.Driver.Thingy', A1_USERNAME: 'U1', A1_PASSWORD: 'sekret']
        shouldFail ClassNotFoundException, { -> DBConfig.fromEnv('A1', { String it -> env1[it] }) }
    }

    void 'test Loader runScript'() {
        def account = DBConfig.inMemoryDB("A1")
        account.withSql { Sql sql ->
            sql.execute('drop all objects')
            def input = getClass().getResource('naaccr_tables.sql')
            def loader = new Loader(sql)
            loader.runScript(input)
        }
    }

    void 'test Loader query'() {
        def account = DBConfig.inMemoryDB("A1")
        account.withSql { Sql sql ->
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

        def account = DBConfig.inMemoryDB("A1")
        account.withSql { Sql sql ->
            def loader = new Loader(sql)
            loader.runScript(getClass().getResource('naaccr_tables.sql'))
            sql.execute "delete from naaccr_tumors"
            loader.load(input)

            def results = new StringWriter()
            results << loader.query('select sum(tumor_id) tot from naaccr_tumors')
            assert results.toString() == '[{"TOT":6}]'
        }
    }
}
