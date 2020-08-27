package gpc.unit

import gpc.DBConfig
import gpc.Tabular
import gpc.Tabular.ColumnMeta
import gpc.TumorFile
import gpc.TumorOnt
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import junit.framework.TestCase
import tech.tablesaw.api.ColumnType
import tech.tablesaw.api.StringColumn
import tech.tablesaw.api.Table

import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate

@CompileStatic
@Slf4j
class TumorOntTest extends TestCase {
    void 'test import sections'() {
        final table_name = 'NAACCR_ONTOLOGY'

        DBConfig.inMemoryDB("sections", true).withSql { Sql sql ->
            final toTypeName = ColumnMeta.typeNames(sql.connection)
            sql.execute(ColumnMeta.createStatement(table_name, TumorOnt.metadataColumns, toTypeName))
            TumorOnt.insertTerms(sql, table_name, TumorOnt.sectionCSV, { Map s -> TumorOnt.makeSectionTerm(s) })
            final actual = sql.firstRow('select min(c_fullname) fn1, min(c_name) n1, count(*) qty from NAACCR_ONTOLOGY')
            assert actual == [
                    N1 : '01 Cancer Identification',
                    FN1: '\\i2b2\\naaccr\\S:1 Cancer Identification\\',
                    QTY: 17,
            ]
        }
    }

    void 'test write sections'() {
        final update_date = LocalDate.of(2020, 7, 17)
        final out = new StringWriter()
        TumorOnt.writeTerms(out, update_date, TumorOnt.sectionCSV, { Map s -> TumorOnt.makeSectionTerm(s) })
        final actual = out.toString().split('\r\n')
        assert actual.size() == 18
        assert actual[0].startsWith('C_HLEVEL,C_FULLNAME,C_NAME')
        assert actual[1].startsWith('2,\\i2b2\\naaccr\\S:1 Cancer Identification\\')
        assert actual[1].count(',') == TumorOnt.metadataColumns.size() - 1
    }

    void 'test import items'() {
        final table_name = 'NAACCR_ONTOLOGY'

        DBConfig.inMemoryDB("sections", true).withSql { Sql sql ->
            final toTypeName = ColumnMeta.typeNames(sql.connection)
            sql.execute(ColumnMeta.createStatement(table_name, TumorOnt.metadataColumns, toTypeName))
            TumorOnt.insertTerms(sql, table_name, TumorOnt.itemCSV, { Map s -> TumorOnt.makeItemTerm(s) })
            final actual = sql.firstRow('select min(c_fullname) fn1, min(c_name) n1, count(*) qty from NAACCR_ONTOLOGY')
            assert actual == [
                    'FN1':'\\i2b2\\naaccr\\S:1 Cancer Identification\\0380 Sequence Number--Central\\',
                    'N1':'0010 Record Type',
                    'QTY':732,
            ]
        }
    }

    void 'test SEER Recode terms'() {
        final recodeRules = TumorFile.SEERRecode.fromLines(TumorFile.SEERRecode.site_recode.text)

        final terms = recodeRules.collect { it.asTerm() }.unique()
        assert terms[2] == [hlevel: 1, path: 'Oral Cavity and Phar\\Tongue', name: 'Tongue', basecode: '20020', visualattributes: 'LA']

        final t2 = Tabular.allCSVRecords(TumorOnt.SEERRecode.seer_recode_terms)
        [t2, terms].transpose().each {
            final parts = it as List
            assert parts[0] == parts[1]
        }
        assert t2.size() == terms.size()
    }

    static final String cache = ',cache/'

    void testLoadCSV() {
        final Table per_section = TumorOnt.read_csv(TumorOnt.getResource('heron_load/section.csv'))
        assert per_section.columnTypes() as List == [ColumnType.INTEGER, ColumnType.STRING]
        assert per_section
                .where(per_section.stringColumn("section").isEqualTo("Record ID"))
                .row(0).getInt("sectionid") == 9
    }

    void "test loinc answer codes"() {
        def qty = 0
        TumorOnt.LOINC_NAACCR.eachAnswerTerm { Map it ->
            qty += 1
            assert (it.C_BASECODE as String).matches('NAACCR\\|\\d+:.+')
        }
        assert qty > 100
    }

    void testLoinc() {
        final Table answers = TumorOnt.LOINC_NAACCR.answer
        assert answers.columnNames().size() == 9
        assert answers.columnNames().first() == 'LOINC_NUMBER'
        assert answers.columnNames().last() == 'ANSWER_STRING'
    }

    void "test R code labels"() {
        TumorOnt.NAACCR_R.eachFieldScheme {
            println(it)
        }
        TumorOnt.NAACCR_R.eachCodeLabel {
            println(it)
        }
        TumorOnt.NAACCR_R.eachCodeTerm {
            println(it)
        }
    }

    void testR() {
        final scheme = 'peritonealCytology'
        final info = TumorOnt.NAACCR_R._code_labels.toURI().resolve(scheme + '.csv').toURL()
        Table codes = TumorOnt.read_csv(info, TumorOnt.NAACCR_R.field_info_schema.columnTypes())
        codes.addColumns(StringColumn.create('scheme', [scheme] * codes.rowCount()))
        assert codes.columnNames() == ['code', 'label', 'means_missing', 'description', 'scheme']
        final Table with_fields = codes.joinOn('scheme').inner(TumorOnt.NAACCR_R.field_code_scheme, 'scheme')
        Table item_name = TumorOnt.NAACCR_R.field_info.select('item', 'name')
        final Table with_field_info = with_fields.joinOn('name').inner(item_name, 'name')
        assert with_field_info.columnCount() == 7

        final Table labels = TumorOnt.NAACCR_R.code_labels()
        assert labels.columnCount() == 7
    }

    void testOncologyMeta() {
        Path cachePath = Paths.get(cache)
        if (!cachePath.toFile().exists()) {
            log.warn('skipping OncologyMeta test. cache does not exist: ' + cache)
            return
        }
        final meta = TumorOnt.OncologyMeta
        final morph = meta.read_table(cachePath, meta.morph3_info)
        assert morph.columnNames() == ["code", "label", "notes"]

        // test encoding
        final non_ascii = morph.where(morph.stringColumn("code").isEqualTo("M8950/3"))
        assert non_ascii.get(0, 1) == "M\u009Fllerian mixed tumour"

        final topo = meta.read_table(Paths.get(cache), meta.topo_info)
        assert topo.columnNames() == ["Kode", "Lvl", "Title"]

        final icd_o_topo = meta.icd_o_topo(topo)
        assert icd_o_topo.columnNames() == ['lvl', 'concept_cd', 'c_visualattributes', 'path', 'concept_name']
    }


    void testLoadTable() {
        Table aTable = TumorOnt.NAACCR_I2B2.tumor_item_type
        DBConfig.inMemoryDB("load", true).withSql { Sql sql ->
            TumorOnt.load_data_frame(sql, "tumor_item_type", aTable)

            Map rowMap = sql.firstRow("select * from tumor_item_type limit 1")
            assert rowMap['SECTION'] == 'Cancer Identification'
            assert sql.firstRow("select count(*) from tumor_item_type")[0] == 732
        }
    }

    void testTableSql() {
        Table aTable = TumorOnt.NAACCR_I2B2.tumor_item_type

        final create = TumorOnt.SqlScript.create_ddl("tumor_item_type", aTable.columns())
        assert create.startsWith("create table tumor_item_type (")
        assert create.contains("\"LENGTH\" INTEGER")
        assert create.endsWith("\"PHI_ID_KIND\" VARCHAR(1024))")

        final insert = TumorOnt.SqlScript.insert_dml("tumor_item_type", aTable.columns())
        assert insert.contains("(\"NAACCRNUM\", \"SECTIONID\"")
        assert insert.contains("\"PHI_ID_KIND\")")
        assert insert.contains("(?, ?, ?")

        def update_date = LocalDate.of(2000, 1, 1)
        def top = TumorOnt.NAACCR_I2B2.naaccr_top(update_date)
        final create_top = TumorOnt.SqlScript.create_ddl("top", top.columns())
        assert create_top.contains("\"C_HLEVEL\" INTEGER")
    }

    void testDDict() {
        final dd = TumorOnt.ddictDF()
        assert dd.rowCount() > 700

        final primarySite = dd.where(dd.intColumn("naaccrNum").isEqualTo(400))

        assert primarySite[0].getString("naaccrId") == "primarySite"
        assert primarySite[0].getString("naaccrName") == 'Primary Site'
        assert primarySite[0].getInt("startColumn") == 554
        assert primarySite[0].getInt("length") == 4
        assert primarySite[0].getString("parentXmlElement") == 'Tumor'
    }

    void testOnt() {
        def update_date = LocalDate.of(2000, 1, 1)
        def top = TumorOnt.NAACCR_I2B2.naaccr_top(update_date)
        assert top.get(0, 0) == 1

        DBConfig.inMemoryDB("ont", true).withSql { Sql sql ->
            final Table actual = TumorOnt.NAACCR_I2B2.ont_view_in(sql, "task123", update_date, Paths.get(cache))
            assert actual.columnCount() == 21
            assert actual.columnNames().contains("C_FULLNAME")
            // top concept
            assert actual.where(actual.intColumn("C_HLEVEL").isEqualTo(1)).rowCount() == 1

            // section concepts
            assert actual.where(actual.intColumn("C_HLEVEL").isEqualTo(2)
                    & actual.stringColumn("C_FULLNAME").startsWith("\\i2b2\\naaccr\\S:")).rowCount() == 17
            // item concepts
            assert actual.where(actual.intColumn("C_HLEVEL").isEqualTo(3)
                    & actual.stringColumn("C_FULLNAME").startsWith("\\i2b2\\naaccr\\S:")).rowCount() > 500

            // TODO: separate LOINC, R codes?
            // code concepts
            assert actual.where(actual.intColumn("C_HLEVEL").isEqualTo(4)
                    & actual.stringColumn("C_FULLNAME").startsWith("\\i2b2\\naaccr\\S:")).rowCount() > 5000

            // TODO: separate SEER site table method?
            // seer site
            assert actual.where(actual.stringColumn("C_FULLNAME").startsWith("\\i2b2\\naaccr\\SEER Site\\")).rowCount() == 103

            // TODO: separate site-specific factor method?
            // cancer staging site-specific terms
            assert actual.where(actual.stringColumn("C_FULLNAME").startsWith("\\i2b2\\naaccr\\csterms\\")).rowCount() > 10000

            assert actual.rowCount() > 100
        }
    }

    void testSqlDialect() {
        DBConfig.inMemoryDB("dialect").withSql { Sql sql ->
            assert sql.firstRow("select 1 as x from (values('X'))  ")[0] == 1
            assert sql.firstRow("select lpad(10, 4, '0') from (values(1))")[0] == "0010"
            assert sql.firstRow("select lpad(9, 2, '0') || ' xyz' from (values(1))")[0] == "09 xyz"
            assert sql.firstRow("select 1 from (values('X')) where not regexp_like('XXXX.9', '^[0-9].*')")[0] == 1
        }
    }

    void testTableExpr() {
        final records = [
                [c_facttablecolumn: "x", c_comment: "xx", c_totalnum: 1],
                [c_facttablecolumn: "CONCEPT_CD", c_comment: null, c_totalnum: null]
        ] as List<Map<String, Object>>
        final actual = TumorOnt.fromRecords(records)
        assert actual.columnTypes() == [ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER] as ColumnType[]
        assert actual.rowCount() == 2
    }

    void testSqlScript() {
        String sql = TumorOnt.resourceText('heron_load/naaccr_concepts_load.sql')
        assert sql.indexOf('select') > 0
        final script = TumorOnt.NAACCR_I2B2.ont_script
        assert script.objects.last().first == 'naaccr_ontology'
    }

}