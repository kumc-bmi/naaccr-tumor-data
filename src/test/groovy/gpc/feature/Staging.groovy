package gpc.feature

import gpc.DBConfig
import gpc.TumorFile
import gpc.unit.TumorFileTest
import groovy.sql.Sql
import groovy.transform.CompileStatic
import junit.framework.TestCase
import org.junit.Ignore

import java.nio.file.Files
import java.nio.file.Paths

/**
 * CAUTION: ambient access to user.dir to write config file, DB.
 * ISSUE: use temp dir?
 */
@CompileStatic
class Staging extends TestCase {
    void "test discrete data on 100 records of test data with local disk h2 DB"() {
        def argv = ['discrete-data']
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
        def argv = ['discrete-data']
        final cli = cli1(argv, System.getProperty('user.dir'), v16_file)

        TumorFile.main(argv as String[])
        cli.account().withSql { Sql sql ->
            final qty = sql.firstRow("select count(distinct PRIMARY_SITE_N400) from NAACCR_DISCRETE")[0]
            assert qty == 1
            final v = sql.firstRow("select distinct naaccr_record_version_n50 from naaccr_discrete")[0]
            assert v == '160'
        }
    }

    @Ignore("TODO: stats test. depends on tumors? move to ETL?")
    static class ToDo extends TestCase {
        void "test stats on 100 records of test data with local disk h2 DB"() {
            def argv = ['discrete-data', '--no-file']
            cli1(argv, System.getProperty('user.dir'))

            TumorFile.main(['load-records'] as String[])
            TumorFile.main(['discrete-data', '--no-file'] as String[])
            TumorFile.main(['summary', '--no-file'] as String[])
        }
    }

    void "test load multiple NAACCR files in a local disk h2 DB"() {
        def argv = ['load-files', 'tmp1', 'tmp2']
        cli1(argv, System.getProperty('user.dir'))

        ['tmp1', 'tmp2'].each { String n ->
            new File(n).withPrintWriter { w ->
                ['line1', 'line2', 'line3'].each { w.println(it) }
            }
        }
        TumorFile.main(argv as String[])
    }

    static DBConfig.CLI cli1(List<String> argv, String userDir,
                             String flat_file = null) {
        Properties ps = new Properties()
        if (!flat_file) {
            flat_file = TumorFileTest.testDataPath
        }
        ps.putAll(["db.url"              : "jdbc:h2:file:${userDir}/DB1;create=true".toString(),
                   "db.driver"           : 'org.h2.Driver',
                   "db.username"         : 'SA',
                   "db.password"         : '',
                   "naaccr.flat-file"    : flat_file,
                   "naaccr.records-table": "NAACCR_RECORDS",
                   "naaccr.extract-table": "NAACCR_DISCRETE",
                   "naaccr.stats-table"  : "NAACCR_EXPORT_STATS",
        ])
        def cli = TumorFileTest.buildCLI(argv, ps)
        ps.store(new File(cli.arg("--db")).newWriter(), null)
        cli.account().withSql { Sql sql -> sql.execute('drop all objects') }
        cli
    }
}
