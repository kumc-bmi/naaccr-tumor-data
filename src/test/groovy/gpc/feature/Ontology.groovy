package gpc.feature

import gpc.TumorFile
import groovy.transform.CompileStatic
import junit.framework.TestCase
import org.junit.Ignore

import java.nio.file.Path

@CompileStatic
class Ontology extends TestCase {
    void "test build NAACCR_ONTOLOGY in a local disk h2 DB"() {
        Staging.withTempDir('db1') { Path dbDir ->
            final cli = Staging.cli1(['ontology'], dbDir)
            TumorFile.run(cli)
        }
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

