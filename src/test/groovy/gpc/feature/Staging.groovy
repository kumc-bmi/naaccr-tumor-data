package gpc.feature

import gpc.DBConfig
import gpc.TumorFile
import gpc.unit.TumorFileTest
import groovy.sql.Sql
import groovy.transform.CompileStatic
import junit.framework.TestCase
import org.junit.Ignore
import tech.tablesaw.api.Table

import java.nio.file.Paths

/**
 * CAUTION: ambient access to user.dir to write config file, DB.
 * TODO: use temp dir
 * TODO: consider renaming Staging to something about PCORNet tumor table
 */
@CompileStatic
class Staging extends TestCase {
    void "test discrete data on 100 records of test data with local disk h2 DB"() {
        def argv = ['tumor-table']
        final cli = cli1(argv, System.getProperty('user.dir'))

        TumorFile.main(argv as String[])
        cli.account().withSql { Sql sql ->
            final qty = sql.firstRow("select count(distinct PRIMARY_SITE_N400) from NAACCR_DISCRETE")[0]
            assert qty == 50
            final txt = sql.firstRow("select distinct task_id from naaccr_discrete")[0]
            assert txt == 'task123'
            final v = sql.firstRow("select distinct naaccr_record_version_n50 from naaccr_discrete")[0]
            assert v == '180'
        }
    }

    static final String v16_file = 'naaccr_xml_samples/valid_standard-file-1.txt'

    void "test a v16 flat file"() {
        def argv = ['tumor-table']
        final cli = cli1(argv, System.getProperty('user.dir'), v16_file)

        TumorFile.main(argv as String[])
        cli.account().withSql { Sql sql ->
            final qty = sql.firstRow("select count(distinct PRIMARY_SITE_N400) from NAACCR_DISCRETE")[0]
            assert qty == 1
            final v = sql.firstRow("select distinct naaccr_record_version_n50 from naaccr_discrete")[0]
            assert v == '160'
        }
    }

    void "test load multiple NAACCR files in a local disk h2 DB"() {
        def argv = ['tumor-files', 'tmp1', 'tmp2']
        final cli = cli1(argv, System.getProperty('user.dir'))

        ['tmp1', 'tmp2'].each { String n ->
            new File(n).withPrintWriter { w ->
                w.write(new File(TumorFileTest.testDataPath).text)
            }
        }
        TumorFile.main(argv as String[])
        cli.account().withSql { Sql sql ->
            final qty = sql.firstRow("select count(*) from NAACCR_DISCRETE")[0]
            assert qty == 200
        }
    }

    static DBConfig.CLI cli1(List<String> argv, String userDir,
                             String flat_file = null) {
        Properties ps = new Properties()
        if (!flat_file) {
            flat_file = TumorFileTest.testDataPath
        }
        ps.putAll(["db.url"            : "jdbc:h2:file:${userDir}/DB1;create=true".toString(),
                   "db.driver"         : 'org.h2.Driver',
                   "db.username"       : 'SA',
                   "db.password"       : '',
                   "naaccr.flat-file"  : flat_file,
                   "naaccr.tumor-table": "NAACCR_DISCRETE",
                   "naaccr.stats-table": "NAACCR_EXPORT_STATS",
        ])
        def cli = TumorFileTest.buildCLI(argv, ps)
        ps.store(new File(cli.arg("--db")).newWriter(), null)
        cli.account().withSql { Sql sql -> sql.execute('drop all objects') }
        cli
    }

    @Ignore("TODO: stats test. depends on tumors? move to ETL?")
    static class ToDo extends TestCase {
        void "test build ontology without luigi, python, Spark"() {

        }

        void "test TODO rename discrete-data command to tumor-table"() {
        }

        void "test complete check based on upload_id rather than task_id"() {

        }

        void "test that tumor table has patid varchar column"() {

        }

        void "test stats on 100 records of test data with local disk h2 DB"() {
            def argv = ['discrete-data', '--no-file']
            cli1(argv, System.getProperty('user.dir'))

            TumorFile.main(['load-records'] as String[])
            TumorFile.main(['discrete-data', '--no-file'] as String[])
            TumorFile.main(['summary', '--no-file'] as String[])
        }

        @Ignore("testStatsFromDB: requires live Oracle connection")
        void testStatsFromDB() {
            final cdw = DBConfig.inMemoryDB("TR", true)
            final String extract_table = "TR_DATA"
            final String stats_table = "TR_STATS"

            int cksum = -1
            cdw.withSql() { Sql sql ->
                DBConfig.Task work = new TumorFile.NAACCR_Summary(cdw, "task123",
                        [Paths.get(TumorFileTest.testDataPath).toUri().toURL()], extract_table, stats_table)
                work.run()
                sql.query("select * from ${stats_table}" as String) { results ->
                    Table stats = Table.read().db(results, "stats")
                    cksum = stats.longColumn('TUMOR_QTY').countUnique()
                }
            }
            assert cksum == 3
        }
    }
}
