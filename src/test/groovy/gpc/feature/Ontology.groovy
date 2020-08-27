package gpc.feature

import gpc.TumorFile
import gpc.TumorOnt
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import junit.framework.TestCase
import org.junit.Ignore

import java.nio.file.Path
import java.nio.file.Paths

@CompileStatic
@Slf4j
class Ontology extends TestCase {
    Path workDir

    void setUp() {
        workDir = Files.createTempDirectory('Staging')
    }

    void tearDown() {
        Staging.deleteFolder(workDir.toFile())
    }

    void "test build NAACCR_ONTOLOGY in a local disk h2 DB"() {
        final cli = Staging.cli1(['ontology'], workDir)
        TumorFile.run(cli)

        cli.account().withSql { Sql sql ->
            final actual = sql.rows("""
            select c_hlevel, count(*) qty from NAACCR_ONTOLOGY
            group by c_hlevel order by c_hlevel
            """)
            assert actual == [
                    ['C_HLEVEL':1, 'QTY':1],
                    ['C_HLEVEL':2, 'QTY':19],
                    ['C_HLEVEL':3, 'QTY':987],
                    ['C_HLEVEL':4, 'QTY':2761],
                    ['C_HLEVEL':5, 'QTY':10545],
            ]
        }
    }

    void "test ICO-O-3 OncologyMeta"() {
        // final cache = Paths.get(',cache')
        final cache = Paths.get('/home/connolly/projects/naaccr-tumor-data/,cache') //@@@
        if (!cache.toFile().exists()) {
            log.warn('skipping OncologyMeta test. cache does not exist: ' + cache)
            return
        }

        final onc = TumorOnt.OncologyMeta.load(cache)

        assert onc.major()[0] == ['level': 3, 'code': '800', 'label': 'Neoplasms NOS']


        /*
        static String encoding = 'ISO-8859-1'

         */
    }


    @Ignore("TODO: metadata.json for 2017 ont")
    static class ToDo extends TestCase {
        void "test import 2017 ontology into local disk h2 DB"() {
            def argv = ['import', 'ONT_2017', 'heron_load/shrine_ont.naaccr_ontology.csv', 'heron_load/shrine_ont.naaccr_ontology-metadata.json']
            Staging.cli1(argv, null)
            TumorFile.main(argv as String[])
        }
    }
}

