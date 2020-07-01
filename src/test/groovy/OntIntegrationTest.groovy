import groovy.transform.CompileStatic
import junit.framework.TestCase

@CompileStatic
class OntIntegrationTest extends TestCase {
    void "test build NAACCR_ONTOLOGY in a local disk h2 DB"() {
        StagingIntegrationTest.cli1(['ontology'], System.getProperty('user.dir'))
        TumorFile.main(['ontology'] as String[])
    }

    // TODO: metadata.json
    void "SKIP test import 2017 ontology into local disk h2 DB"() {
        def argv = ['import', 'ONT_2017', 'heron_load/shrine_ont.naaccr_ontology.csv', 'heron_load/shrine_ont.naaccr_ontology-metadata.json']
        StagingIntegrationTest.cli1(argv, System.getProperty('user.dir'))
        TumorFile.main(argv as String[])
    }
}
