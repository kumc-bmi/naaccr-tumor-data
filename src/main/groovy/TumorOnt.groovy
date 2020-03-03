import groovy.transform.CompileStatic
import tech.tablesaw.api.*
import tech.tablesaw.columns.Column
import tech.tablesaw.io.csv.CsvReadOptions

import java.nio.charset.Charset
import java.nio.file.Path
import java.time.LocalDate
import java.util.zip.ZipFile

@CompileStatic
class TumorOnt {
    static class OncologyMeta {
        static Tuple morph3_info = new Tuple('ICD-O-2_CSV.zip', 'icd-o-3-morph.csv', ['code', 'label', 'notes'])
        static Tuple topo_info = new Tuple('ICD-O-2_CSV.zip', 'Topoenglish.txt', null)
        static String encoding = 'ISO-8859-1'

        static Table read_table(Path cache, Tuple info) {
            String zip = info[0] as String; String item = info[1] as String; List<String> names = info[2] as List<String>
            def archive = new ZipFile(cache.resolve(zip).toFile())
            def infp = new InputStreamReader(archive.getInputStream(archive.getEntry(item)), Charset.forName(encoding))
            CsvReadOptions options = CsvReadOptions.builder(infp)
                    .separator((item.endsWith(".csv") ? ',' : '\t') as char)
                    .header(names == null)
                    .build()
            Table data = Table.read().csv(options)
            if (names) {
                listZip(names, data.columns()) { String name, Column<?> col -> col.setName(name) }
            }
            data
        }
    }

    static List listZip(List a, List b, Closure f) {
        def result = []
        0.upto(Math.min(a.size(), b.size()) - 1) { ix -> result << f(a[ix], b[ix]) }
        result
    }

    static String top_folder = "\\i2b2\\naaccr\\"
    static String c_name = 'Cancer Cases (NAACCR Hierarchy)'
    static String sourcesystem_cd = 'heron-admin@kumc.edu'
    static class NAACCR_I2B2 {
        static def ont_view_in(String task_hash, LocalDate update_date, Path who_cache) {
            // TODO: make who_cache optional
            def info = OncologyMeta.topo_info
            def who_topo = OncologyMeta.read_table(who_cache, info)
            // TODO info = OncologyMeta.icd_o_info
            // def icd_o_topo = OncologyMeta.read_table(who_cache, info.zip, info.items, info.names)
            Table top = build([[c_hlevel :1,
                                c_fullname: top_folder,
                                c_name: c_name,
                                update_date: update_date,
                                sourcesystem_cd: sourcesystem_cd] as Map])
            def current_task = build([[task_hash: task_hash] as Map]).setName("current_task")
            [who_topo, top, current_task]
        }
    }

    static Table build(List<Map<String, Object>> obj) {
        Collection<Column<?>> cols = (obj[0].collect { k, v ->
            switch(v) {
                case String:
                    return StringColumn.create(k, v as String)
                case Integer:
                    return IntColumn.create(k, v as Integer)
                case LocalDate:
                    return DateColumn.create(k, v as LocalDate)
                default:
                    throw new IllegalArgumentException("Expected String or Int in 1st record, not:" + v)
            }
        }) as Collection<Column<?>>
        Table data = Table.create("t1", cols)
        obj.tail().each { Map<String, Object> m ->
            Row row = data.appendRow()
            for (it in m) {
                switch(it.value) {
                    case String:
                        row.setString(it.key, it.value as String)
                        break
                    case Integer:
                        row.setInt(it.key, it.value as Integer)
                        break
                    case LocalDate:
                        row.setDate(it.key, it.value as LocalDate)
                        break
                    case null:
                        row.setMissing(it.key)
                        break
                    default:
                        throw new IllegalArgumentException("not supported" + it.value)
                }
            }
        }
        data
    }
}