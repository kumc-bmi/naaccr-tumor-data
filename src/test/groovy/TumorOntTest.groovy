import junit.framework.TestCase

import java.nio.file.Paths
import java.time.LocalDate

class TumorOntTest extends TestCase {
    static final cache = ',cache/'

    void testOncologyMeta() {
        final meta = TumorOnt.OncologyMeta
        final morph = meta.read_table(Paths.get(cache), meta.morph3_info)
        assert morph.columnNames() == ["code", "label", "notes"]

        // test encoding
        final non_ascii = morph.where(morph.stringColumn("code").isEqualTo("M8950/3"))
        println(non_ascii)
        assert non_ascii.get(0, 1) == "M\u009Fllerian mixed tumour"

        final topo = meta.read_table(Paths.get(cache), meta.topo_info)
        println(topo.first(5))
        assert topo.columnNames() == ["Kode", "Lvl", "Title"]
    }

    void testTableExpr() {
        final obj = [
                [c_facttablecolumn: "x", c_comment: "xx", c_totalnum: 1],
                [c_facttablecolumn: "CONCEPT_CD", c_comment: null, c_totalnum: null]
        ]
        println(obj[0])
        final actual = TumorOnt.build(obj)
        println(actual)
    }

    void testSqlScript() {
        // TODO: commit symlink from src/main/resources to heron_load
        URL url = getClass().getResource('heron_load/naaccr_concepts_load.sql')
        String sql = TumorOnt.resourceText(url)
        assert sql.indexOf('select') > 0
        final script = TumorOnt.NAACCR_I2B2.ont_script
        assert script.objects.last().first == 'naaccr_ontology'
    }

    void testOnt() {
        final actual = TumorOnt.NAACCR_I2B2.ont_view_in("task123", LocalDate.of(2000, 1, 1), Paths.get(cache))
        for (Object x : actual) {
            println(x)
        }
    }
}