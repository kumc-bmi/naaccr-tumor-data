import tech.tablesaw.api.Table
import tech.tablesaw.io.csv.CsvReadOptions

import javax.annotation.Nullable
import java.nio.charset.Charset
import java.nio.file.Path
import java.util.zip.ZipFile

class TumorOnt {
    static class OncologyMeta {
        static def morph3_info = [zip:'ICD-O-2_CSV.zip', item: 'icd-o-3-morph.csv', names: ['code', 'label', 'notes']]
        static def topo_info = [zip: 'ICD-O-2_CSV.zip', item: 'Topoenglish.txt', names: null]
        static def encoding = 'ISO-8859-1'

        static Table read_table(Path cache, String zip, String item, @Nullable List<String> names) {
            def archive = new ZipFile(cache.resolve(zip).toFile())
            def infp = new InputStreamReader(archive.getInputStream(archive.getEntry(item)), Charset.forName(encoding))
            CsvReadOptions options = CsvReadOptions.builder(infp)
                    .separator((item.endsWith(".csv") ? ',' : '\t') as char)
                    .header(names == null)
                    .build()
            Table data = Table.read().csv(options)
            if (names) {
                listZip(names, data.columns()) { name, col -> col.setName(name) }
            }
            data
        }
    }

    static List listZip(List a, List b, Closure f) {
        def result = []
        0.upto(Math.min(a.size(), b.size()) - 1) { ix -> result << f(a[ix], b[ix]) }
        result
    }
}