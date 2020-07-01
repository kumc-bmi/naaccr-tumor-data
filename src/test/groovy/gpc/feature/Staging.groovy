package gpc.feature

import gpc.DBConfig
import gpc.TumorFile
import gpc.unit.TumorFileTest
import groovy.sql.Sql
import groovy.transform.CompileStatic
import junit.framework.TestCase

import java.nio.file.Paths
import java.nio.file.Files

/**
 * CAUTION: ambient access to user.dir to write config file, DB.
 * ISSUE: use temp dir?
 */
@CompileStatic
class Staging extends TestCase {
    void "test load-records: load NAACCR records from flat-file into a (CLOB) column of a DB table"() {
        def cli = cli1(['load-records'], System.getProperty('user.dir'))

        def path = Paths.get(TumorFileTest.testDataPath)
        def lineCount = Files.lines(path).count();

        TumorFile.main(['load-records'] as String[])

        def rowCount = cli.account().withSql { Sql sql ->
            sql.firstRow("select count(*) from NAACCR_RECORDS")[0]
        }
        assert rowCount == lineCount
        TumorFile.main(['query', "select count(*) from NAACCR_RECORDS"] as String[])
    }

    void "test discrete data on 100 records of test data with local disk h2 DB"() {
        def argv = ['discrete-data', '--no-file']
        cli1(argv, System.getProperty('user.dir'))

        TumorFile.main(['load-records'] as String[])
        TumorFile.main(argv as String[])
    }

    // TODO: stats test. depends on tumors? move to ETL?
    void "SKIP test stats on 100 records of test data with local disk h2 DB"() {
        def argv = ['discrete-data', '--no-file']
        cli1(argv, System.getProperty('user.dir'))

        TumorFile.main(['load-records'] as String[])
        TumorFile.main(['discrete-data', '--no-file'] as String[])
        TumorFile.main(['summary', '--no-file'] as String[])
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

    static DBConfig.CLI cli1(List<String> argv, String userDir) {
        Properties ps = new Properties()
        ps.putAll(["db.url"              : "jdbc:h2:file:${userDir}/DB1;create=true".toString(),
                   "db.driver"           : 'org.h2.Driver',
                   "db.username"         : 'SA',
                   "db.password"         : '',
                   "naaccr.flat-file"    : TumorFileTest.testDataPath,
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
