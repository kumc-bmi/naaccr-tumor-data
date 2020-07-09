package gpc.feature

import gpc.TumorFile
import gpc.unit.TumorFileTest
import groovy.sql.Sql
import junit.framework.TestCase

class I2B2Star extends TestCase {
    void "test facts from 100 records of test data with local disk h2 DB"() {
        String patient_ide_source = 'SMS@kumed.com'

        final cli = Staging.cli1(['discrete-data'], System.getProperty('user.dir'))
        cli.account().withSql { Sql sql ->
            TumorFileTest.mockPatientMapping(sql, patient_ide_source, 100)
        }
        TumorFile.main(['discrete-data'] as String[])
        TumorFile.main(['facts', '--upload-id=111222'] as String[])
    }
}
