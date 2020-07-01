package gpc

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.file.Paths
import java.sql.Connection
import java.sql.SQLException

@CompileStatic
@Slf4j
class Loader {
    private final Sql _sql

    Loader(Sql sql) {
        _sql = sql
    }

    void runScript(URL input) {
        assert input != null
        def edited = productSqlEdits(productName())

        input.withInputStream { stream ->
            new Scanner(stream).useDelimiter(";\n") each {
                String sql = edited(it)
                log.debug("executing $input: $sql")
                try {
                    _sql.execute sql
                } catch (SQLException oops) {
                    if (sql.contains("drop table")) {
                        /* trundle on ... */
                    } else {
                        throw oops
                    }
                }
            }
        }
    }

    static Closure<String> productSqlEdits(productName) {
        if (productName == "Oracle") {
            return { String sql -> sql.replaceAll("drop table if exists", "drop table") }
        } else {
            return { String sql -> sql }
        }
    }

    void setSessionDateFormat() {
        if (productName() == "Oracle") {
            _sql.execute("alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD'")
        }
    }

    String productName() {
        String productName = '???'
        _sql.cacheConnection { Connection conn ->
            productName = conn.metaData.databaseProductName
        }
        productName
    }

    static int batchSize = 100


    JsonBuilder query(String sql) {
        def results = _sql.rows(sql)
        new JsonBuilder(results)
    }

    int load(Reader input) {
        def scanner = new Scanner(input)
        def parser = new JsonSlurper()
        def header = parser.parseText(scanner.nextLine())
        String stmt = header['sql']
        log.info("batch insert: $stmt")

        setSessionDateFormat()

        def qty = 0
        _sql.withBatch(batchSize, stmt) { BatchingPreparedStatementWrapper ps ->
            List<Object> record
            while (scanner.hasNextLine()) {
                try {
                    record = parser.parseText(scanner.nextLine()) as List
                } catch (EOFException ignored) {
                    break
                }
                // logger.info("insert record: $record")
                ps.addBatch(record)
                qty += 1
            }
        }
        log.info("inserted $qty records")
        qty
    }


    static void run_cli(DBConfig.CLI cli) {
        DBConfig account = cli.account()

        account.withSql { Sql sql ->
            def loader = new Loader(sql)

            String script = cli.arg("SCRIPT")
            String query = cli.arg("SQL")
            if (script) {
                def cwd = Paths.get(".").toAbsolutePath().normalize().toString()
                loader.runScript(new URL(new URL("file://$cwd/"), script))
            } else if (query) {
                def json = loader.query(query)
                System.out.withWriter {
                    json.writeTo(it)
                }
                System.out.println()
            } else if (cli.flag("load")) {
                loader.load(new InputStreamReader(System.in))
            } else {
                throw new IllegalArgumentException(cli.opts.toString())
            }
            null
        }
    }
}
