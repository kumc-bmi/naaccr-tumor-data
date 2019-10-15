import java.util.logging.Logger
import groovy.sql.Sql
import groovy.sql.GroovyResultSet
import groovy.transform.CompileStatic
import groovy.transform.Immutable

@Immutable
class Password {
    final String value

    @Override
    String toString() {
        return "...${value.length()}..."
    }
}


@Immutable
class DBConfig {
    final String url
    final String driver
    final String username
    final Password password

    static Logger logger = Logger.getLogger("")

    static DBConfig fromEnv(String account, Closure<String> getenv) {
        logger.info("getting config for $account")
        Closure<String> config = {
            def name = "${account}_${it}"
            def val = getenv(name)
            if (val == null) {
                throw new IllegalStateException(name)
            }
            val
        }
        def driver = config("DRIVER")
        Class.forName(driver)
        new DBConfig(url: config("URL"), driver: driver,
                     username: config("USER"), password: new Password(value: config("PASSWORD")))
    }
}

@CompileStatic
class Loader {
    static Logger logger = Logger.getLogger("")

    private final Sql _sql

    Loader(Sql sql) {
        _sql = sql
    }

    void runScript(URL input) {
        def edited = productSqlEdits(productName())

        input.withInputStream { stream ->
            new Scanner(stream).useDelimiter(";\n") each {
                String sql = edited(it)
                logger.info("executing $input: $sql")
                try {
                    _sql.execute sql
                } catch (java.sql.SQLException oops) {
                    if (sql.contains("drop table")) {
                        /* trundle on ... */
                    } else {
                        throw oops
                    }
                }
            }
        }
    }

    Closure<String> productSqlEdits(productName) {
        if (productName == "Oracle") {
            return { String sql -> sql.replaceAll("drop table if exists", "drop table") }
        } else {
            return { String sql -> sql }
        }
    }

    String productName() {
        String productName
        _sql.cacheConnection { java.sql.Connection conn ->
            productName = conn.metaData.databaseProductName
        }
        productName
    }

    static void main(String[] args) {
        def arg = { String target ->
            int ix = args.findIndexOf({ it == target })
            if (ix < 0 || ix + 1 >= args.length) {
                return null
            }
            args[ix + 1]
        }
        def account = arg("--account")
        if (!account) {
            logger.warning("Usage: java -jar loader.jar --account A [--run abc.sql]")
            System.exit(1)
        }
        def config
        try {
            config = DBConfig.fromEnv(account, { String name -> System.getenv(name) })
        } catch (IllegalStateException oops) {
            logger.warning("Config missing from environment: $oops")
            System.exit(1)
        } catch (ClassNotFoundException oops) {
            logger.warning("driver not found (fix CLASSPATH?): $oops")
            System.exit(1)
        }
        logger.info("$account config: $config")
        Sql.withInstance(config.url, config.username, config.password.value, config.driver) { Sql sql ->
            def script = arg("--run")
            if (script) {
                def cwd = java.nio.file.Paths.get(".").toAbsolutePath().normalize().toString()
                new Loader(sql).runScript(new URL(new URL("file://$cwd/"), script))
            }
        }
    }
}
