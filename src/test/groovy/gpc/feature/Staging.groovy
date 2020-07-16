package gpc.feature

import gpc.DBConfig
import gpc.TumorFile
import gpc.unit.TumorFileTest
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import junit.framework.TestCase
import org.junit.Ignore

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.DriverManager

/**
 * CAUTION: ambient access to temp dir to write config file, DB.
 * TODO: consider renaming Staging to something about PCORNet tumor table
 */
@CompileStatic
@Slf4j
class Staging extends TestCase {
    Path workDir

    void setUp() {
        workDir = Files.createTempDirectory('Staging')
    }

    void tearDown() {
        deleteFolder(workDir.toFile())
    }

    void "test tumor-table on 100 v18 records with local disk h2 DB"() {
        def argv = ['tumor-table']
        final cli = cli1(argv, workDir)

        TumorFile.run(cli)
        cli.account().withSql { Sql sql ->
            final qty = sql.firstRow("select count(distinct PRIMARY_SITE_N400) from TUMOR")[0]
            assert qty == 50
            final txt = sql.firstRow("select distinct task_id from TUMOR")[0]
            assert txt == 'task123'
            final v = sql.firstRow("select distinct naaccr_record_version_n50 from TUMOR")[0]
            assert v == '180'
        }
    }

    static final URL v16_file = TumorFileTest.getResource('naaccr_xml_samples/valid_standard-file-1.txt')

    void "test a v16 flat file"() {
        def argv = ['tumor-table']
        final cli = cli1(argv, workDir, v16_file)

        TumorFile.run(cli)
        cli.account().withSql { Sql sql ->
            final qty = sql.firstRow("select count(distinct PRIMARY_SITE_N400) from TUMOR")[0]
            assert qty == 1
            final v = sql.firstRow("select distinct naaccr_record_version_n50 from TUMOR")[0]
            assert v == '160'
        }
    }

    void "test load multiple NAACCR file versions in a local disk h2 DB"() {
        final installed = [TumorFileTest.sample100, v16_file].collect { installTestData(workDir, it) }
        def argv = ['tumor-files'] + installed.collect { it.toString() }
        final cli = cli1(argv, workDir)

        TumorFile.run(cli)
        cli.account().withSql { Sql sql ->
            final qty = sql.firstRow("select count(*) from TUMOR")[0]
            assert qty == 102
        }
    }

    void "test load layouts"() {
        def argv = ['load-layouts']
        final cli = cli1(argv, workDir)

        TumorFile.run(cli)
        cli.account().withSql { Sql sql ->
            final qty = sql.firstRow("select count(*) from LAYOUT")[0] as Integer
            assert qty > 300
        }
    }

    static DBConfig.CLI cli1(List<String> argv, Path workDir,
                             URL flat_file = null,
                             boolean reset = true) {
        final io = io1(workDir, flat_file)
        final cli = new DBConfig.CLI(TumorFile.docopt.parse(argv), io)
        if (reset) {
            File configFile = cli.pathArg("--db").toFile()
            Properties ps = io.fetchProperties('x')
            ps.store(configFile.newWriter(), null)
            cli.account().withSql { Sql sql -> sql.execute('drop all objects') }
        }
        cli
    }

    static DBConfig.IO io1(Path workDir,
                           URL testData = null,
                           Map<String, String> extraProperties = [:]) {
        Properties ps = new Properties()
        if (!testData) {
            testData = TumorFileTest.sample100
        }
        final flat_file = installTestData(workDir, testData)
        ps.putAll(["db.url"                    : "jdbc:h2:file:${workDir}/DB1;create=true".toString(),
                   "db.driver"                 : 'org.h2.Driver',
                   "db.username"               : 'SA',
                   "db.password"               : '',
                   "naaccr.flat-file"          : flat_file.toString(),
                   "naaccr.tumor-table"        : "TUMOR",
        ] + extraProperties)
        buildIO(ps, workDir)
    }

    static Path installTestData(Path workDir, URL testData) {
        final dest = workDir.resolve(Paths.get(testData.toURI()).fileName)
        dest.toFile().withPrintWriter { wr -> wr.write(testData.text) }
        dest
    }

    /**
     * CAUTION: Ambient access to DriverManager.getConnection
     */
    static DBConfig.IO buildIO(Properties config, Path workDir) {
        [
                fetchProperties: { String name -> config },
                exit           : { int it -> throw new RuntimeException('unexpected exit') },
                getConnection  : { String url, Properties ps ->
                    log.debug('ambient access to {}', url)
                    DriverManager.getConnection(url, ps)
                },
                resolve        : { String s ->
                    final p = workDir.resolve(s)
                    log.debug('resolved "{}" to path <{}>', s, p)
                    p
                },
        ] as DBConfig.IO
    }


    static <V> V withTempDir(String prefix, Closure<V> thunk) {
        Path path = Files.createTempDirectory(prefix)
        try {
            thunk(path)
        } finally {
            deleteFolder(path.toFile())
        }
    }

    // ack NCode Oct 2011 https://stackoverflow.com/a/7768086/7963
    static void deleteFolder(File folder) {
        final files = folder.listFiles()
        if (files != null) { //some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteFolder(f)
                } else {
                    f.delete()
                }
            }
        }
        folder.delete()
    }

    @Ignore("TODO: stats test. depends on tumors? move to ETL?")
    static class ToDo extends TestCase {
        void "test build ontology without luigi, python, Spark"() {

        }

        void "test complete check based on upload_id rather than task_id"() {

        }

        void "test that tumor table has patid varchar column"() {

        }

        void "test loading v16 and v18 in that order"() {

        }
    }
}
