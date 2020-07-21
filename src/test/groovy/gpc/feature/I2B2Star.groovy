package gpc.feature

import gpc.DBConfig
import gpc.Loader
import gpc.TumorFile
import gpc.unit.TumorFileTest
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import junit.framework.TestCase
import org.junit.Ignore

import java.nio.file.Files
import java.nio.file.Path

/**
 * CAUTION: ambient access to temp dir for config, DB
 */
@CompileStatic
@Slf4j
class I2B2Star extends TestCase {
    Path workDir

    void setUp() {
        workDir = Files.createTempDirectory('I2B2Star')
    }

    void tearDown() {
        Staging.deleteFolder(workDir.toFile())
    }

    static String patientMapping = """
            select distinct tr.patient_id_number_n20 as MRN, pm.patient_num
            from patient_mapping pm
            join tumor tr on trim(leading '0' from tr.patient_id_number_n20) = pm.patient_ide
        """

    void "test facts from 100 records of test data with local disk h2 DB"() {
        String patient_ide_source = 'SMS@kumed.com'

        final io1 = Staging.io1(workDir, TumorFileTest.sample100,
                ["i2b2.patient-mapping-query": patientMapping])
        final cli = { List<String> args -> new DBConfig.CLI(TumorFile.docopt.parse(args), io1) }
        final cli1 = cli(['tumor-table'])
        TumorFile.run(cli1)

        cli1.account().withSql { Sql sql ->
            TumorFileTest.mockPatientMapping(sql, patient_ide_source, 100)
        }
        TumorFile.run(cli(['facts', '--upload-id=111222']))
    }

    void "test template table"() {
        String patient_ide_source = 'SMS@kumed.com'
        URL createFactTable = Loader.getResource('observation_fact.sql')

        final io1 = Staging.io1(workDir, TumorFileTest.sample100,
                [
                        "i2b2.patient-mapping-query": patientMapping,
                        "i2b2.template-fact-table": "OBSERVATION_FACT",
                ])
        final cli = { List<String> args -> new DBConfig.CLI(TumorFile.docopt.parse(args), io1) }
        final cli1 = cli(['tumor-table'])
        TumorFile.run(cli1)

        cli1.account().withSql { Sql sql ->
            new Loader(sql).runScript(createFactTable)
            TumorFileTest.mockPatientMapping(sql, patient_ide_source, 100)
        }
        TumorFile.run(cli(['facts', '--upload-id=111222']))
        cli1.account().withSql { Sql sql ->
            final actual = sql.firstRow('select * from observation_fact_111222')
            assert (actual as Map).keySet().contains('VALUEFLAG_CD')
        }
    }

    void "test run patient mapping script"() {
        String patient_ide_source = 'SMS@kumed.com'
        final updatePatientMapping = """
            update TUMOR tr
            set tr.patient_num = (
                    select pm.patient_num
            from patient_mapping pm
            where pm.patient_ide_source = 'SMS@kumed.com'
            and pm.patient_ide = tr.PATIENT_ID_NUMBER_N20
            )
            """

        final io1 = Staging.io1(workDir, TumorFileTest.sample100,
                ["i2b2.patient-mapping-query": patientMapping])
        final cli1 = new DBConfig.CLI(TumorFile.docopt.parse(['tumor-table']), io1)
        TumorFile.run(cli1)
        cli1.account().withSql { Sql sql ->
            TumorFileTest.mockPatientMapping(sql, patient_ide_source, 100)
        }
        final script = workDir.resolve('pmap.sql')
        script.toFile().withPrintWriter { wr ->
            wr.println(updatePatientMapping)
        }
        TumorFile.run(new DBConfig.CLI(TumorFile.docopt.parse(['run', 'pmap.sql']), io1))
        cli1.account().withSql { Sql sql ->
            final q = cli1.property("i2b2.patient-mapping-query")
            final toPatientNum = TumorFile.NAACCR_Facts.getPatientMapping(sql, q)
            assert toPatientNum.size() == 91
        }
    }

    @Ignore
    static class ToDo {
        void "test i2b2 facts from v16 flat file"() {

        }

        void "test bc_qa variables"() {

        }

        void "test seer site recode terms"() {
        }

        void "test site-specific factors csterms"() {

        }

        void "test icd_o_meta terms"() {

        }

        void "test mrnItem = 2300"() {

        }
    }
}
