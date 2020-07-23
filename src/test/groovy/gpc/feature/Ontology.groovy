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
    void "test build NAACCR_ONTOLOGY in a local disk h2 DB"() {
        Staging.withTempDir('db1') { Path dbDir ->
            final cli = Staging.cli1(['ontology'], dbDir)
            TumorFile.run(cli)
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

