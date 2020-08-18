package gpc.unit

import com.imsweb.layout.Field
import com.imsweb.layout.LayoutFactory
import com.imsweb.layout.LayoutInfo
import com.imsweb.layout.record.fixed.FixedColumnsField
import com.imsweb.layout.record.fixed.FixedColumnsLayout
import gpc.DBConfig
import gpc.DBConfig.Task
import gpc.Tabular
import gpc.TumorFile
import gpc.TumorFile.SEERRecode
import gpc.TumorFile.SEERRecode.Range
import gpc.TumorFile.SEERRecode.Ranges
import gpc.TumorOnt
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import junit.framework.TestCase
import tech.tablesaw.api.Row
import tech.tablesaw.api.Table

import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate

@CompileStatic
@Slf4j
class TumorFileTest extends TestCase {
    // CAUTION: ambient access to source code directory
    static final Path resolveSource(String other) {
        Paths.get('').resolve(other)
    }
    static final URL sample100 = TumorFileTest.getResource('naaccr_xml_samples/naaccr-xml-sample-v180-incidence-100.txt')

    // NOTE: if you rename this, update CONTRIBUTING.md
    void testDocOpt() {
        assert TumorFile.usageText.startsWith('Usage:')
        final args = ['tumor-table']
        final actual = TumorFile.docopt.parse(args)
        assert actual['--db'] == 'db.properties'
        assert actual['tumor-table'] == true
        assert actual['facts'] == false

        final both = TumorFile.docopt.parse(
                ['tumor-table', '--task-id', 'abc123', '--db', 'deid.properties'])
        assert both['--db'] == 'deid.properties'
        assert both["--update-date"] == null

        def args2 = ['ontology', '--task-hash=1234', '--update-date=2002-02-02', '--who-cache=,cache']
        final more = TumorFile.docopt.parse(
                args2)
        assert more['--task-hash'] == '1234'
        assert LocalDate.parse(more["--update-date"] as String) == LocalDate.of(2002, 2, 2)

        final Properties config = new Properties()
        config.putAll(LoaderTest.dbInfo1 + [('naaccr.flat-file'): sample100.toString(), ('naaccr.records-table'): 'T1'])
        DBConfig.CLI cli = new DBConfig.CLI(TumorFile.docopt.parse(args2),
                [
                        fetchProperties: { String ignored -> config },
                        resolve        : { String other -> Paths.get(other) }
                ] as DBConfig.IO)
        assert cli.pathArg('--who-cache').toString().endsWith(',cache')
        assert cli.property("naaccr.records-table") == "T1"

        final tfiles = TumorFile.docopt.parse(['tumor-files', 'F1', 'F2', 'F3'])
        assert tfiles['NAACCR_FILE'] == ['F1', 'F2', 'F3']
    }

    void 'test pathArg on missing SCRIPT arg'() {
        final cli = new DBConfig.CLI(TumorFile.docopt.parse('query', 'select 1'), [:] as DBConfig.IO)
        try {
            cli.pathArg("SCRIPT")
            assert 'should have thrown' == ''
        } catch (IllegalArgumentException ignored) {
            assert ignored != null
        }
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
        final flat_file = Paths.get(sample100.toURI())
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
        List<LayoutInfo> possibleFormats = LayoutFactory.discoverFormat(Paths.get(sample100.toURI()).toFile())
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

    static List<Map<String, Object>> recode_in_100 = [
            ['PRIMARY_SITE_N400': 'C001', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8070'],
            ['PRIMARY_SITE_N400': 'C019', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8070'],
            ['PRIMARY_SITE_N400': 'C059', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8070'],
            ['PRIMARY_SITE_N400': 'C079', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8430'],
            ['PRIMARY_SITE_N400': 'C099', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9680'],
            ['PRIMARY_SITE_N400': 'C160', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8140'],
            ['PRIMARY_SITE_N400': 'C165', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8140'],
            ['PRIMARY_SITE_N400': 'C169', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8010'],
            ['PRIMARY_SITE_N400': 'C180', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8261'],
            ['PRIMARY_SITE_N400': 'C182', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8140'],
            ['PRIMARY_SITE_N400': 'C183', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8261'],
            ['PRIMARY_SITE_N400': 'C185', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8140'],
            ['PRIMARY_SITE_N400': 'C187', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8010'],
            ['PRIMARY_SITE_N400': 'C187', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8140'],
            ['PRIMARY_SITE_N400': 'C187', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8261'],
            ['PRIMARY_SITE_N400': 'C199', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8140'],
            ['PRIMARY_SITE_N400': 'C209', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8140'],
            ['PRIMARY_SITE_N400': 'C211', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8070'],
            ['PRIMARY_SITE_N400': 'C220', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8170'],
            ['PRIMARY_SITE_N400': 'C320', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8070'],
            ['PRIMARY_SITE_N400': 'C321', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8070'],
            ['PRIMARY_SITE_N400': 'C340', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8020'],
            ['PRIMARY_SITE_N400': 'C341', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8046'],
            ['PRIMARY_SITE_N400': 'C341', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8071'],
            ['PRIMARY_SITE_N400': 'C341', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8140'],
            ['PRIMARY_SITE_N400': 'C343', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8041'],
            ['PRIMARY_SITE_N400': 'C343', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8070'],
            ['PRIMARY_SITE_N400': 'C349', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8010'],
            ['PRIMARY_SITE_N400': 'C349', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8012'],
            ['PRIMARY_SITE_N400': 'C349', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8070'],
            ['PRIMARY_SITE_N400': 'C402', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9180'],
            ['PRIMARY_SITE_N400': 'C421', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9800'],
            ['PRIMARY_SITE_N400': 'C421', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9823'],
            ['PRIMARY_SITE_N400': 'C421', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9989'],
            ['PRIMARY_SITE_N400': 'C443', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8742'],
            ['PRIMARY_SITE_N400': 'C445', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8743'],
            ['PRIMARY_SITE_N400': 'C446', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8720'],
            ['PRIMARY_SITE_N400': 'C502', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8500'],
            ['PRIMARY_SITE_N400': 'C502', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8520'],
            ['PRIMARY_SITE_N400': 'C504', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8010'],
            ['PRIMARY_SITE_N400': 'C504', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8500'],
            ['PRIMARY_SITE_N400': 'C504', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8503'],
            ['PRIMARY_SITE_N400': 'C508', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8500'],
            ['PRIMARY_SITE_N400': 'C508', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8522'],
            ['PRIMARY_SITE_N400': 'C509', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8140'],
            ['PRIMARY_SITE_N400': 'C509', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8500'],
            ['PRIMARY_SITE_N400': 'C539', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8010'],
            ['PRIMARY_SITE_N400': 'C540', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8140'],
            ['PRIMARY_SITE_N400': 'C549', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8260'],
            ['PRIMARY_SITE_N400': 'C569', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8140'],
            ['PRIMARY_SITE_N400': 'C619', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8140'],
            ['PRIMARY_SITE_N400': 'C629', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9070'],
            ['PRIMARY_SITE_N400': 'C649', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8310'],
            ['PRIMARY_SITE_N400': 'C649', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8312'],
            ['PRIMARY_SITE_N400': 'C673', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8120'],
            ['PRIMARY_SITE_N400': 'C679', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8050'],
            ['PRIMARY_SITE_N400': 'C679', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8120'],
            ['PRIMARY_SITE_N400': 'C679', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8130'],
            ['PRIMARY_SITE_N400': 'C700', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9537'],
            ['PRIMARY_SITE_N400': 'C713', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9440'],
            ['PRIMARY_SITE_N400': 'C715', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9391'],
            ['PRIMARY_SITE_N400': 'C719', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8000'],
            ['PRIMARY_SITE_N400': 'C719', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9401'],
            ['PRIMARY_SITE_N400': 'C770', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9695'],
            ['PRIMARY_SITE_N400': 'C772', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9690'],
            ['PRIMARY_SITE_N400': 'C778', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9699'],
            ['PRIMARY_SITE_N400': 'C779', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9590'],
            ['PRIMARY_SITE_N400': 'C779', 'HISTOLOGIC_TYPE_ICD_O3_N522': '9699'],
            ['PRIMARY_SITE_N400': 'C809', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8000'],
            ['PRIMARY_SITE_N400': 'C809', 'HISTOLOGIC_TYPE_ICD_O3_N522': '8070'],
    ] as List<Map<String, Object>>

    void "test loading naaccr flat file"() {
        DBConfig.inMemoryDB("TR").withSql { Sql sql ->
            final claimed = TumorFile.NAACCR_Extract.loadFlatFile(
                    sql, Paths.get(sample100.toURI()), "TUMOR", "task1234", TumorOnt.pcornet_fields)
            assert claimed == 100
            final actual = sql.firstRow('select count(PRIMARY_SITE_N400) from TUMOR')[0]
            assert actual == claimed

            final recode_in = sql.rows('select distinct primary_site_N400, HISTOLOGIC_TYPE_ICD_O3_N522 from TUMOR order by 1, 2')
            assert recode_in == recode_in_100

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
        final sourcesystem_cd = 'my-naaccr-file'
        final upload_id = 123456 // TODO: transition from task_id to upload_id?
        final flat_file = Paths.get(sample100.toURI())
        final mrnItem = 'patientIdNumber' // TODO: 2300 MRN
        String schema = null

        DBConfig.inMemoryDB("obs").withSql { Sql memdb ->
            Map<String, Integer> toPatientNum = (0..100).collectEntries { [String.format('%08d', it), it] }

            final upload = new TumorFile.I2B2Upload(schema, upload_id, sourcesystem_cd)
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
            assert actual as Map == ['RECORDS': 6808, 'ENCOUNTERS': 97, 'PATIENTS': 91, 'CONCEPTS': 583,
                                     'DATES'  : 188, 'TYPES': 3, 'TEXTS': 0, 'NUMBERS': 18]
            assert enc == 2100

            final byCode = memdb.rows("""
                select concept_cd, count(*) records
                from ${upload.factTable} group by concept_cd
            """ as String).collectEntries { [it.concept_cd, it.records] }
            assert byCode['SEER_SITE:26000'] == 17 // Breast
            assert byCode.findAll { (it.key as String).startsWith('NAACCR|400:C50') }.collect { it.value }.sum() == 17
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

    void "test SEER Recode"() {
        assert Ranges.parse('C530-C539', false) == [
                excl: false, bounds: [[lo: 'C530', hi: 'C539'] as Range]] as Ranges
        assert Ranges.parse('excluding 9590-9989, and sometimes 9050-9055, 9140') == [
                excl: true, bounds: [[lo: '9590', hi: '9989'] as Range, [lo: '9050', hi: '9055'] as Range, [lo: '9140', hi: null] as Range]] as Ranges
        assert Ranges.parse('excluding 9590-9989, and sometimes 9050-9055, 9140', false) == [
                excl: true, bounds: [[lo: '9590', hi: '9989'] as Range]] as Ranges
        assert Ranges.parse('All sites except C024, C098-C099, C111, C142, C379, C422, C770-C779') == [
                excl: true, bounds: [
                [lo: 'C024', hi: null] as Range, [lo: 'C098', hi: 'C099'] as Range, [lo: 'C111', hi: null] as Range,
                [lo: 'C142', hi: null] as Range, [lo: 'C379', hi: null] as Range, [lo: 'C422', hi: null] as Range, [lo: 'C770', hi: 'C779'] as Range]] as Ranges


        final recodeRules = SEERRecode.fromLines(SEERRecode.site_recode.text)

        for (tumor in recode_in_100) {
            final site = tumor.PRIMARY_SITE_N400 as String
            final histology = tumor.HISTOLOGIC_TYPE_ICD_O3_N522 as String
            final recode = SEERRecode.getRecode(recodeRules, site, histology)
            assert recode.length() == '20010'.length()
            assert recode.startsWith('2') || recode.startsWith('3') || recode == '99999'
        }
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

    void "test loading tumor data stats"() {
        final data = TumorFileTest.getResource("tumor_data_stats.csv")
        assert data != null

        final cdw = DBConfig.inMemoryDB("TR", true)
        cdw.withSql { Sql sql ->
            Tabular.importCSV(sql, "tumor_data_stats", data, Tabular.metadata(data))
            final actual = sql.firstRow("select sum(dx_yr) YR_SUM, count(distinct concept_cd) CD_QTY from tumor_data_stats")
            assert actual as Map == [YR_SUM: 54782529, CD_QTY: 13179]
        }
    }

    void "test synthesizing data"() {
        final stats = TumorFileTest.getResource("tumor_data_stats.csv")
        final dest = new File('synthetic1500.dat')
        final qty = 150
        final writing = false

        final cdw = DBConfig.inMemoryDB("TR", true)
        final rng = new Random(1)
        final layout = LayoutFactory.getLayout(LayoutFactory.LAYOUT_ID_NAACCR_18_INCIDENCE) as FixedColumnsLayout
        cdw.withSql { Sql sql ->
            Tabular.importCSV(sql, "stats", stats, Tabular.metadata(stats))
            final record = syntheticRecord(sql, layout, rng, 1)
            assert record.length() == layout.layoutLineLength
            assert record.substring(0, 1) == 'I'
            assert TumorFile.fieldValue(layout.getFieldByName('dateOfDiagnosis'), record) == '20150516'

            if (writing) {
                dest.withPrintWriter { out ->
                    (1..qty).each {
                        final txt = syntheticRecord(sql, layout, rng, it)
                        out.println(txt)
                    }
                }
            }
        }
    }

    // String.repeat is new in Java 11 but we target Java 8
    static String repeat(String s, int n) {
        new String(new char[n]).replace("\0", s)
    }

    static String syntheticRecord(Sql sql, FixedColumnsLayout layout, Random rng, int patientIdNumber) {
        String record = repeat(" ", layout.layoutLineLength)
        final dx_yrs = sql.rows("select distinct DX_YR from stats").collect { it.DX_YR as Integer }
        final dx_yr = dx_yrs.get(rng.nextInt(dx_yrs.size()))
        final dx_dt = LocalDate.of(dx_yr, rng.nextInt(12) + 1, rng.nextInt(28) + 1)
        final spliceField = { FixedColumnsField field ->
            { String it ->
                assert field.length >= it.length()
                final pad = repeat(field.padChar, field.length - it.length())
                assert field.align == Field.FieldAlignment.LEFT || field.align == Field.FieldAlignment.RIGHT
                final txt = field.align == Field.FieldAlignment.LEFT ? it + pad : pad + it
                record = record.substring(0, field.start - 1) + txt + record.substring(field.start - 1 + field.length)
            }
        }
        final patientIdFields = [20, 2300].collect { layout.getFieldByNaaccrItemNumber(it) }.findAll { it != null }
        patientIdFields.each { spliceField(it)(patientIdNumber.toString()) };
        sql.eachRow(
                "select distinct VALTYPE_CD, NAACCRNUM, PCT_PRESENT from stats where DX_YR = ${dx_yr} order by naaccrnum"
        ) { item ->
            if (item.getDouble('PCT_PRESENT') < rng.nextInt(100)) {
                return
            }
            final field = layout.getFieldByNaaccrItemNumber(item.getInt('NAACCRNUM'))
            final splice = spliceField(field)
            final valtype_cd = item.getString('VALTYPE_CD')
            //noinspection GroovyFallthrough
            switch (valtype_cd) {
                case '@':
                    final cut = rng.nextInt(100)
                    double acc = 0.0
                    final choices = sql.rows(
                            "select VALUE, PCT_FREQ from stats where dx_yr = ${dx_yr} and naaccrNum = ${item.getInt('NAACCRNUM')} order by pct_freq desc"
                    )
                    final choice = choices.find {
                        acc += it.PCT_FREQ as Double
                        acc > cut
                    }
                    splice(String.format("%" + field.length + "s", choice.VALUE as String))
                    break
                case 'D':
                    if (field.name == 'dateOfDiagnosis') {
                        splice(dx_dt.toString().replace('-', ''))
                        return
                    }
                case 'N':
                    final dist = sql.firstRow(
                            "select MEAN, SD from stats where dx_yr = ${dx_yr} and naaccrNum = ${item.getInt('NAACCRNUM')}"
                    )
                    Double mean = dist.MEAN as Double
                    Double sd = dist.SD as Double
                    assert mean != null && sd != null
                    final qty = (mean + rng.nextGaussian() * sd).round().abs()
                    if (valtype_cd == 'D') {
                        final when = dx_dt.plusDays(qty)
                        splice(when.toString().replace('-', ''))
                    } else {
                        String digits = qty.toString()
                        if (digits.length() > field.length) {
                            // oops... on rare occasions, normally distributed variables take on extreme values
                            digits = digits.substring(0, field.length)
                        }
                        splice(digits)
                    }
                    break
                default:
                    assert "@DN".contains(valtype_cd)
            }
        }
        record
    }

}
