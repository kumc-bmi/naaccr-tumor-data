import groovy.sql.Sql
import groovy.transform.CompileStatic
import junit.framework.TestCase

import java.nio.file.Paths
import java.nio.file.Files

@CompileStatic
class StagingIntegrationTest  extends TestCase {

    /**
     * CAUTION: ambient access to user.dir to write config file, DB.
     * ISSUE: use temp dir?
     */
    void "test load-records sub-comomand"() {
        Properties config = db1()
        def cli = TumorFileTest.buildCLI(['load-records'], config)
        config.store(new File(cli.arg("--db")).newWriter(), null)
        cli.account().withSql { Sql sql -> sql.execute('drop all objects') }

        def path = Paths.get(config.getProperty("naaccr.flat-file"))
        def lineCount = Files.lines(path).count();

        TumorFile.main(['load-records'] as String[])

        def rowCount = cli.account().withSql { Sql sql ->
            sql.firstRow("select count(*) from NAACCR_RECORDS")[0]
        }
        assert rowCount == lineCount
        TumorFile.main(['query', "select count(*) from NAACCR_RECORDS"] as String[])
    }

    /**
     * CAUTION: ambient access to System.getProperty('user.dir')
     * @return config a la README
     */
    static Properties db1() {
        Properties ps = new Properties()
        ps.putAll(["db.url"              : "jdbc:h2:file:${System.getProperty('user.dir')}/DB1;create=true".toString(),
                   "db.driver"           : 'org.h2.Driver',
                   "db.username"         : 'SA',
                   "db.password"         : '',
                   "naaccr.flat-file"    : 'naaccr_xml_samples/naaccr-xml-sample-v180-incidence-100.txt',
                   "naaccr.records-table": "NAACCR_RECORDS",
                   "naaccr.extract-table": "NAACCR_DISCRETE",
                   "naaccr.stats-table"  : "NAACCR_EXPORT_STATS",
        ])
        ps
    }
}
