package gpc.unit

import com.imsweb.layout.LayoutFactory
import com.imsweb.layout.LayoutInfo
import com.imsweb.layout.record.fixed.FixedColumnsLayout
import gpc.DBConfig
import gpc.DBConfig.Task
import gpc.TumorFile
import gpc.TumorOnt
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import junit.framework.TestCase
import org.docopt.Docopt
import tech.tablesaw.api.Row
import tech.tablesaw.api.Table

import java.nio.file.Paths
import java.sql.DriverManager
import java.time.LocalDate

@CompileStatic
@Slf4j
class TumorFileTest extends TestCase {
    static final String testDataPath = 'naaccr_xml_samples/naaccr-xml-sample-v180-incidence-100.txt'

    /**
     * CAUTION: Ambient access to DB Connection
     */
    static DBConfig.CLI buildCLI(List<String> args, Properties config) {
        final String doc = TumorFile.usage
        new DBConfig.CLI(new Docopt(doc).withExit(false).parse(args),
                { String name -> config },
                { int it -> throw new RuntimeException('unexpected exit') },
                { String url, Properties ps -> DriverManager.getConnection(url, ps) })
    }

    // NOTE: if you rename this, update CONTRIBUTING.md
    void testDocOpt() {
        final String doc = TumorFile.usage
        assert doc.startsWith('Usage:')
        final args = ['tumor-table']
        final actual = new Docopt(doc).withExit(false).parse(args)
        assert actual['--db'] == 'db.properties'
        assert actual['tumor-table'] == true
        assert actual['facts'] == false

        final both = new Docopt(doc).withExit(false).parse(
                ['tumor-table', '--task-id', 'abc123', '--db', 'deid.properties'])
        assert both['--db'] == 'deid.properties'
        assert both["--update-date"] == null

        def args2 = ['ontology', '--task-hash=1234', '--update-date=2002-02-02', '--who-cache=,cache']
        final more = new Docopt(doc).withExit(false).parse(
                args2)
        assert more['--task-hash'] == '1234'
        assert LocalDate.parse(more["--update-date"] as String) == LocalDate.of(2002, 2, 2)

        final Properties config = new Properties()
        config.putAll(LoaderTest.dbInfo1 + [('naaccr.flat-file'): testDataPath, ('naaccr.records-table'): 'T1'])
        DBConfig.CLI cli = new DBConfig.CLI(new Docopt(doc).withExit(false).parse(args2),
                { String ignored -> config }, null, null)
        assert cli.urlArg('--who-cache').toString().endsWith(',cache')
        assert cli.property("naaccr.records-table") == "T1"

        final tfiles = new Docopt(doc).withExit(false).parse(['tumor-files', 'F1', 'F2', 'F3'])
        assert tfiles['NAACCR_FILE'] == ['F1', 'F2', 'F3']
    }


    static void mockPatientMapping(Sql sql, String patient_ide_source,
                                   int qty = 100) {
        sql.execute("""
            create table patient_mapping (
                patient_num int,
                patient_ide_source varchar(50),
                patient_ide varchar(64)
            )""")
        sql.withBatch(
                256,
                'insert into patient_mapping(patient_num, patient_ide_source, patient_ide) values (:num, :src, :ide)'
        ) { ps ->
            (1..qty).collect {
                [num: it, src: patient_ide_source, ide: it.toString()]
            }.each { ps.addBatch(it) }
        }
    }

    void testExtractDiscrete() {
        final cdw = DBConfig.inMemoryDB("TR", true)
        final task_id = "task123"
        URL flat_file = Paths.get(testDataPath).toUri().toURL()
        Task extract = new TumorFile.NAACCR_Extract(cdw, task_id,
                [flat_file],
                "NAACCR_DISCRETE",
        )

        cdw.withSql { sql ->
            assert !extract.complete()
            extract.run()
            assert extract.complete()
        }
    }


    void testLayout() {
        List<LayoutInfo> possibleFormats = LayoutFactory.discoverFormat(new File(testDataPath))
        assert !possibleFormats.isEmpty()
        assert possibleFormats.first().layoutId == 'naaccr-18-incidence'

        assert LayoutFactory.LAYOUT_ID_NAACCR_18_INCIDENCE == 'naaccr-18-incidence'
        final FixedColumnsLayout v18 = LayoutFactory.getLayout('naaccr-18-incidence') as FixedColumnsLayout
        assert v18.getFieldByNaaccrItemNumber(400).start == 554
        assert v18.allFields.take(3).collect {
            [('long-label')     : it.longLabel,
             start              : it.start,
             ('naaccr-item-num'): it.naaccrItemNum,
             section            : it.section,
             grouped            : it.subFields != null && it.subFields.size() > 0
            ]
        } == [['long-label': 'Record Type', 'start': 1, 'naaccr-item-num': 10, 'section': 'Record ID', 'grouped': false],
              ['long-label': 'Registry Type', 'start': 2, 'naaccr-item-num': 30, 'section': 'Record ID', 'grouped': false],
              ['long-label': 'Reserved 00', 'start': 3, 'naaccr-item-num': 37, 'section': 'Record ID', 'grouped': false]]
    }

    void "test loading naaccr flat file"() {
        DBConfig.inMemoryDB("TR").withSql { Sql sql ->
            final claimed = TumorFile.NAACCR_Extract.loadFlatFile(
                    sql, new File(testDataPath), "TUMOR", "task1234", TumorOnt.pcornet_fields)
            assert claimed == 100
            final actual = sql.firstRow('select count(PRIMARY_SITE_N400) from TUMOR')[0]
            assert actual == claimed

            final rs = sql.connection.metaData.getColumns(null, null, 'TUMOR', null)
            def cols = []
            while (rs.next()) {
                cols << rs.getString('COLUMN_NAME')
            }
            assert cols.size() == 635

            final pcf = TumorOnt.pcornet_fields.stringColumn('FIELD_NAME').asList()
            assert pcf.size() == 640
            // 10 fields are missing due to:
            // WARN item not found in naaccr-18-incidence: 7320 PATH_DATE_SPEC_COLLECT1_N7320
            assert pcf - cols == [
                    'PATH_DATE_SPEC_COLLECT1_N7320', 'PATH_DATE_SPEC_COLLECT2_N7321', 'PATH_DATE_SPEC_COLLECT3_N7322',
                    'PATH_DATE_SPEC_COLLECT4_N7323', 'PATH_DATE_SPEC_COLLECT5_N7324',
                    'PATH_REPORT_TYPE1_N7480', 'PATH_REPORT_TYPE2_N7481', 'PATH_REPORT_TYPE3_N7482',
                    'PATH_REPORT_TYPE4_N7483', 'PATH_REPORT_TYPE5_N7484'
            ]
            // 5 extra are present:
            assert cols - pcf == ['SOURCE_CD', 'ENCOUNTER_NUM', 'PATIENT_NUM', 'TASK_ID', 'OBSERVATION_BLOB']
        }
    }


    /*****
     * Date parsing. Ugh.

     p. 97:
     "Below are the common formats to handle the situation where only
     certain components of date are known.
     YYYYMMDD - when complete date is known and valid
     YYYYMM - when year and month are known and valid, and day is unknown
     YYYY - when year is known and valid, and month and day are unknown
     Blank - when no known date applies"

     But we also see wierdness such as '    2009' and '19719999'; see
     test cases below.

     In Date of Last Contact, we've also seen 19919999
     */
    void testDates() {
        final actual = date_cases.stringColumn('text').iterator().collect { String it -> TumorFile.parseDate(it) }
        assert date_cases_ymd == actual
    }

    static final Table date_cases = TumorOnt.read_csv(TumorFileTest.getResource('date_cases.csv'))
    static final List<LocalDate> date_cases_ymd = date_cases.iterator().collect { Row it ->
        it.isMissing('year') ?
                null : LocalDate.of(
                it.getInt('year'),
                it.getInt('month'),
                it.getInt('day'))
    }

    void "test i2b2 fact table"() {
        final patient_ide_source = 'SMS@kumed.com'
        final sourcesystem_cd = 'my-naaccr-file'
        final upload_id = 123456 // TODO: transition from task_id to upload_id?
        final flat_file = new File(testDataPath)
        final mrnItem = 'patientIdNumber' // TODO: 2300 MRN
        String schema = null

        DBConfig.inMemoryDB("obs").withSql { Sql memdb ->
            Map<String, Integer> toPatientNum = (0..100).collectEntries { [String.format('%08d', it), it] }

            final upload = new TumorFile.I2B2Upload(schema, upload_id, sourcesystem_cd, patient_ide_source)
            final enc = TumorFile.makeTumorFacts(
                    flat_file, 2000,
                    memdb, mrnItem, toPatientNum,
                    upload)

            final actual = memdb.firstRow("""
                select count(*) records
                     , count(distinct encounter_num) encounters
                     , count(distinct patient_num) patients
                     , count(distinct concept_cd) concepts
                     , count(distinct start_date) dates
                     , count(distinct valtype_cd) types
                     , count(distinct tval_char) texts
                     , count(distinct nval_num) numbers
                from ${upload.factTable}
            """ as String)
            assert actual as Map == ['RECORDS': 6711, 'ENCOUNTERS': 97, 'PATIENTS': 91, 'CONCEPTS': 552,
                                     'DATES'  : 188, 'TYPES': 3, 'TEXTS': 0, 'NUMBERS': 18]
            assert enc == 2100
        }
    }

    void "test patient mapping"() {
        final cdw = DBConfig.inMemoryDB("TR", true)
        final task_id = "task123"
        URL flat_file = Paths.get(testDataPath).toUri().toURL()
        final patient_ide_source = 'SMS@kumed.com'
        final patient_ide_col = 'PATIENT_ID_NUMBER_N20'
        final patient_ide_expr = "trim(leading '0' from tr.${patient_ide_col})"
        final extract_table = "NAACCR_DISCRETE"
        final extract = new TumorFile.NAACCR_Extract(cdw, task_id,
                [flat_file],
                extract_table,
        )

        final upload = new TumorFile.I2B2Upload(null, 1, "NAACCR", patient_ide_source)

        cdw.withSql { Sql sql ->
            mockPatientMapping(sql, patient_ide_source, 100)
            extract.run()
            extract.updatePatientNum(sql, upload, patient_ide_expr)
            final toPatientNum = extract.getPatientMapping(sql, patient_ide_col)
            assert toPatientNum.size() == 91
        }
    }

    /**
     * _stable_hash uses a published algorithm (CRC32)
     */
    void testStableHash() {
        assert TumorFile._stable_hash("abc") == 891568578
    }

    void testTumorFields() {
        Table actual = TumorOnt.fields(false)
        assert actual.rowCount() == 640
        assert actual.rowCount() > 100
        assert actual.where(actual.stringColumn('FIELD_NAME').isEqualTo('RECORD_TYPE_N10')).rowCount() == 1

        Table pcornet_spec = TumorOnt.read_csv(TumorFileTest.getResource('tumor table.version1.2.csv')).select(
                'NAACCR Item', 'FLAG', 'FIELD_NAME'
        )
        assert pcornet_spec.rowCount() == 775
        pcornet_spec = pcornet_spec.where(pcornet_spec.stringColumn('FLAG').isNotEqualTo('PRIVATE'))
        assert pcornet_spec.rowCount() == 775 - 109

        actual.column('FIELD_NAME').setName('name_test')
        Table items = pcornet_spec.joinOn('NAACCR Item').fullOuter(actual, 'naaccrNum')
        // println(items.first(3))
        Table problems = items.first(0)
        for (Row item : items) {
            if (item.getString('FIELD_NAME') != item.getString('name_test')) {
                problems.addRow(item)
            }
        }
        def missingId = problems.where(problems.column('naaccrId').isMissing())
        assert missingId.rowCount() == 26
        def noMatch = problems.where(problems.column('name_test').isMissing())
        assert noMatch.rowCount() == 26
        def renamed = problems.where(problems.stringColumn('name_test').isNotIn(''))
        assert renamed.rowCount() == 23
    }

    void "test loading layout for id data extraction"() {
        final account = DBConfig.inMemoryDB("layout")
        account.withSql {
            final t1 = new TumorFile.LoadLayouts(account, "layout")
            assert !t1.complete()
            t1.run()
            assert t1.complete()
        }
    }
}
