package gpc.feature


import gpc.TumorFile
import junit.framework.TestCase

class I2B2Star extends TestCase {
    void "test visits from 100 records of test data with local disk h2 DB"() {
        Staging.cli1(['tumors'], System.getProperty('user.dir'))
        TumorFile.main(['tumors'] as String[])
    }

    void "test facts from 100 records of test data with local disk h2 DB"() {
        Staging.cli1(['facts'], System.getProperty('user.dir'))
        TumorFile.main(['facts'] as String[])
    }
}
