import groovy.sql.Sql
import junit.framework.TestCase
import tech.tablesaw.api.ColumnType
import tech.tablesaw.api.Table

import java.nio.file.Paths
import java.time.LocalDate

class TumorOntTest extends TestCase {
    static final cache = ',cache/'

    void testLoadCSV() {
        final Table per_section = TumorOnt.read_csv(TumorOnt.getResource('heron_load/section.csv'))
        assert per_section.columnTypes() as List == [ColumnType.INTEGER, ColumnType.STRING]
        assert per_section
                .where(per_section.stringColumn("section").isEqualTo("Record ID"))
                .row(0).getInt("sectionid") == 9
    }

    void testOncologyMeta() {
        final meta = TumorOnt.OncologyMeta
        final morph = meta.read_table(Paths.get(cache), meta.morph3_info)
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
        Loader.DBConfig config = LoaderTest.config1
        Sql.withInstance(config.url, config.username, config.password.value, config.driver) { Sql sql ->
            // ack: https://stackoverflow.com/a/4991969
            sql.execute("DROP SCHEMA PUBLIC CASCADE")  // ISSUE: DB state shouldn't persist between tests
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
        assert create.contains("length INTEGER")
        assert create.endsWith("phi_id_kind VARCHAR(1024))")

        final insert = TumorOnt.SqlScript.insert_dml("tumor_item_type", aTable.columns())
        assert insert.contains("(naaccrNum, sectionId")
        assert insert.contains("phi_id_kind)")
        assert insert.contains("(?, ?, ?")
    }


    void testOnt() {
        Loader.DBConfig config = LoaderTest.config1
        Sql.withInstance(config.url, config.username, config.password.value, config.driver) { Sql sql ->
            final actual = TumorOnt.NAACCR_I2B2.ont_view_in(sql, "task123", LocalDate.of(2000, 1, 1), Paths.get(cache))
            println(actual)
            assert actual == "please print@@"
        }
    }

    void testSqlDialect() {
        Loader.DBConfig config = LoaderTest.config1
        Sql.withInstance(config.url, config.username, config.password.value, config.driver) { Sql sql ->
            assert sql.firstRow("select 1 as x from (values('X'))  ")[0] == 1
            sql.firstRow("select lpad('0', 4, cast(10 as varchar(4))) from (values(1))")[0] == "0010"
            final s1 = TumorOnt.NAACCR_I2B2.ont_script
            sql.execute(TumorOnt.SqlScript.find_ddl("zpad", s1.code))
            sql.firstRow("select zpad(4, 10) from (values(1))")[0] == "0010"
            sql.execute('drop function zpad')  // ISSUE: DB state shouldn't persist between tests.
        }
    }

    void testTableExpr() {
        final records = [
                [c_facttablecolumn: "x", c_comment: "xx", c_totalnum: 1],
                [c_facttablecolumn: "CONCEPT_CD", c_comment: null, c_totalnum: null]
        ]
        final actual = TumorOnt.fromRecords(records)
        assert actual.columnTypes() == [ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER] as ColumnType[]
        assert actual.rowCount() == 2
    }

    void testSqlScript() {
        // TODO: commit symlink from src/main/resources to heron_load
        URL url = getClass().getResource('heron_load/naaccr_concepts_load.sql')
        String sql = TumorOnt.resourceText(url)
        assert sql.indexOf('select') > 0
        final script = TumorOnt.NAACCR_I2B2.ont_script
        assert script.objects.last().first == 'naaccr_ontology'
    }

}