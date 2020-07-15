package gpc.feature

import gpc.TumorFile
import groovy.transform.CompileStatic
import junit.framework.TestCase
import org.junit.Ignore

@CompileStatic
class Ontology extends TestCase {
    void "test build NAACCR_ONTOLOGY in a local disk h2 DB"() {
        Staging.withTempDir('db1') { dbDir ->
            Staging.cli1(['ontology'], dbDir.toString())
            TumorFile.main(['ontology'] as String[])
        }
    }

    @Ignore("TODO: metadata.json for 2017 ont")
    static class ToDo extends TestCase {
        void "test import 2017 ontology into local disk h2 DB"() {
            def argv = ['import', 'ONT_2017', 'heron_load/shrine_ont.naaccr_ontology.csv', 'heron_load/shrine_ont.naaccr_ontology-metadata.json']
            Staging.cli1(argv, System.getProperty('user.dir'))
            TumorFile.main(argv as String[])
        }
    }
}

