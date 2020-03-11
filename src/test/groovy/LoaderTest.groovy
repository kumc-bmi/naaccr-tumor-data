import groovy.json.JsonOutput
import groovy.sql.GroovyResultSet
import groovy.sql.Sql
import groovy.transform.CompileStatic
import junit.framework.TestCase

import static groovy.test.GroovyAssert.shouldFail

@CompileStatic
class LoaderTest extends TestCase {
    @SuppressWarnings("GrMethodMayBeStatic")
    void 'inMemoryDB supports select'() {
        def account = DBConfig.inMemoryDB("A1")
        account.withSql { Sql sql ->
            sql.eachRow("select 1 as c from (values('x'))") { GroovyResultSet row ->
                assert row['c'] == 1
            }
        }
    }

    static final Properties dbProps1 = ({ ->
        Properties ps = new Properties()
        ps.putAll(["db.url" : 'jdbc:h2:mem:A1;create=true', "db.driver": 'org.h2.Driver',
                   "db.user": 'SA', "db.password": ''])
        ps
    })()

    void 'test DBConfig misspelled env var'() {
        def ps = new Properties()
        ps.putAll(["db.url": 'URL1', "db.driver": 'java.sql.DriverManager', "db.user": 'U1', "db.pssword": 'sekret'])
        shouldFail IllegalStateException, { -> DBConfig.jdbcProperties(ps) }
    }

    void 'test missing driver'() {
        def ps = new Properties()
        ps.putAll(["db.url": 'URL1', "db.driver": 'sqlserver.Driver.Thingy', "db.user": 'U1', "db.password": 'sekret'])
        shouldFail ClassNotFoundException, { -> DBConfig.jdbcProperties(ps) }
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

            // query for each datatype... except dates; due to ambient timezone access, it's not portable somehow
            def tumors = new StringWriter()
            tumors << loader.query("select tumor_id, to_char(dateOfDiagnosis, 'YYYY-MM-DD') as dateOfDiagnosis, primarySite, ageAtDiagnosis from naaccr_tumors")
            assert tumors.toString() == '[{"TUMOR_ID":11,"DATEOFDIAGNOSIS":"2010-01-01","PRIMARYSITE":"650","AGEATDIAGNOSIS":60}]'

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
