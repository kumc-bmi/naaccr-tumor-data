/** tumor_load - use naaccr-xml to load tumor registry as XML
 *
 * tested with openjdk 1.8, apache-groovy-2.5.7
 * sha256 3d905dfe4f739c8c0d9dd181e6687ac816e451bf327a9ec0740da473cfebc9e0
 *
 * Output can be used a la:
 *
 * select line
 *      , to_date(extract(tumor,
 *                        '* / * / * / *[@naaccrId="dateOfDiagnosis"]/text()'),
 *               'YYYYMMDD') as dateOfDiagnosis
 * from tumors;
 */
import java.io.StringWriter

import groovy.sql.Sql

// "the [xpp3] library needs to be loaded by the system classloader"
// -- https://codeday.me/en/qa/20190306/5613.html
@GrabConfig(systemClassLoader= true)
@Grab('com.imsweb:naaccr-xml:6.1')

import com.imsweb.naaccrxml.PatientFlatReader
import com.imsweb.naaccrxml.PatientXmlWriter
import com.imsweb.naaccrxml.entity.Patient


class Loader {
    static String oraDriver = 'oracle.jdbc.OracleDriver'
    static String oraUrl = 'jdbc:oracle:thin:@localhost:1521:'
    static max = 20  // TODO: unimited tumors

    static void main(String[] args) {
        String flat_file_name = args[0]
        String database = args[1]
        String username = System.getenv('LOGNAME')
        String password = System.getenv("${username}_${database}".toUpperCase())
        String db_url = oraUrl + database

        Sql.withInstance(db_url, username, password, oraDriver) { sql ->
            sql.eachRow('select * from global_name') { row ->
                println("${username}@${row.global_name}: loading TUMORS ...")
            }
            def ld = new Loader(sql)
            new File(flat_file_name).withReader('utf-8') { reader ->
                def qty = ld.load(reader, max)
                println("... loaded ${qty}")
            }
        }
    }

    private final Sql sql

    Loader(Sql sql) {
        this.sql = sql
    }

    int load(Reader flatReader, int max) {
        def line = 0

        sql.execute 'delete from tumors' // ISSUE: purge?
        sql.execute 'commit'

        sql.withBatch(12, 'insert into tumors (line, tumor) values(?, ?)') {
            stmt ->
            def patReader = new PatientFlatReader(flatReader)
            def naaccrData = patReader.getRootData()
            while (line++ < max) {
                def p1 = patReader.readPatient()
                if (p1 == null) { break }
                def info = patientXML(p1, naaccrData)
                def xmlVal = sql.connection.createSQLXML()
                xmlVal.setString(info)
                stmt.addBatch(line, xmlVal)
            }
        }
        line - 1
    }

    /**
     * serailize Patient as XML format String.
     */
    static String patientXML(Patient p1, Object naaccrData) {
        def buf = new StringWriter()
        def writer = new PatientXmlWriter(buf, naaccrData)
        writer.writePatient(p1)
        writer.close()
        buf.toString()
    }
}
