package gpc.feature

import gpc.DBConfig
import gpc.TumorFile
import gpc.unit.TumorFileTest
import groovy.sql.Sql
import groovy.transform.CompileStatic
import junit.framework.TestCase
import org.junit.Ignore

import java.nio.file.Files
import java.nio.file.Path

/**
 * CAUTION: ambient access to temp dir to write config file, DB.
 * TODO: consider renaming Staging to something about PCORNet tumor table
 */
@CompileStatic
class Staging extends TestCase {
    void "test discrete data on 100 records of test data with local disk h2 DB"() {
        def argv = ['tumor-table']
        withTempDir('db1') { dbDir ->
            final cli = cli1(argv, dbDir.toString())

            TumorFile.main(argv as String[])
            cli.account().withSql { Sql sql ->
                final qty = sql.firstRow("select count(distinct PRIMARY_SITE_N400) from TUMOR")[0]
                assert qty == 50
                final txt = sql.firstRow("select distinct task_id from TUMOR")[0]
                assert txt == 'task123'
                final v = sql.firstRow("select distinct naaccr_record_version_n50 from TUMOR")[0]
                assert v == '180'
            }
        }
    }

    static final String v16_file = 'naaccr_xml_samples/valid_standard-file-1.txt'

    void "test a v16 flat file"() {
        def argv = ['tumor-table']
        withTempDir('db1') { dbDir ->
            final cli = cli1(argv, dbDir.toString(), v16_file)

            TumorFile.main(argv as String[])
            cli.account().withSql { Sql sql ->
                final qty = sql.firstRow("select count(distinct PRIMARY_SITE_N400) from TUMOR")[0]
                assert qty == 1
                final v = sql.firstRow("select distinct naaccr_record_version_n50 from TUMOR")[0]
                assert v == '160'
            }
        }
    }

    void "test load multiple NAACCR file versions in a local disk h2 DB"() {
        def argv = ['tumor-files', TumorFileTest.testDataPath, v16_file]
        withTempDir('db1') { dbDir ->
            final cli = cli1(argv, dbDir.toString())

            TumorFile.main(argv as String[])
            cli.account().withSql { Sql sql ->
                final qty = sql.firstRow("select count(*) from TUMOR")[0]
                assert qty == 102
            }
        }
    }

    void "test load layouts"() {
        def argv = ['load-layouts']
        withTempDir('db1') { dbDir ->
            final cli = cli1(argv, dbDir.toString())

            TumorFile.main(argv as String[])
            cli.account().withSql { Sql sql ->
                final qty = sql.firstRow("select count(*) from LAYOUT")[0] as Integer
                assert qty > 300
            }
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
                   "naaccr.tumor-table": "TUMOR",
                   "naaccr.stats-table": "NAACCR_EXPORT_STATS",
        ])
        def cli = TumorFileTest.buildCLI(argv, ps)
        ps.store(new File(cli.arg("--db")).newWriter(), null)
        cli.account().withSql { Sql sql -> sql.execute('drop all objects') }
        cli
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
