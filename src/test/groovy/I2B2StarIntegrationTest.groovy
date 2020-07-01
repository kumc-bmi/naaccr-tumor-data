import junit.framework.TestCase

class I2B2StarIntegrationTest  extends TestCase{
    void "test visits from 100 records of test data with local disk h2 DB"() {
        StagingIntegrationTest.cli1(['tumors'], System.getProperty('user.dir'))
        TumorFile.main(['tumors'] as String[])
    }

    void "test facts from 100 records of test data with local disk h2 DB"() {
        StagingIntegrationTest.cli1(['facts'], System.getProperty('user.dir'))
        TumorFile.main(['facts'] as String[])
    }
}
