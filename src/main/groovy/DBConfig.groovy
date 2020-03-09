import groovy.sql.Sql
import groovy.transform.CompileStatic

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.logging.Logger

@CompileStatic
class DBConfig {
    static Logger logger = Logger.getLogger("")

    final String url
    final Properties connectionProperties
    private final Closure<Connection> connect

    DBConfig(String url, Properties connectionProperties, Closure<Connection> connect) {
        this.url = url
        this.connectionProperties = connectionProperties
        this.connect = connect
    }

    def <V> V withSql(Closure<V> thunk) throws SQLException {
        Connection conn = connect(url, connectionProperties)
        Sql sql = new Sql(conn)
        try {
            thunk(sql)
        } finally {
            sql.close()
            conn.close()
        }
    }

    static Properties fromEnv(String account, Closure<String> getenv) {
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
        try {
            Class.forName(driver)
        } catch (Exception noDriver) {
            logger.exiting("cannot load driver", driver, noDriver)
            throw noDriver
        }
        Properties properties = new Properties()
        properties.setProperty('url', config("URL"))
        properties.setProperty('user', config("USER"))
        properties.setProperty('password', config('PASSWORD'))
        properties
    }

    static DBConfig inMemoryDB(String databaseName, boolean reset=false) {
        String url = "jdbc:h2:mem:${databaseName};create=true"
        DBConfig it = new DBConfig(url, new Properties(),
                { String ignored, Properties _ -> DriverManager.getConnection(url) })
        if (reset) {
            it.withSql({ Sql sql -> sql.execute('drop all objects') })
        }
        it
    }

    static class CLI {
        protected final String[] args
        private final Closure<String> getenv
        private final Closure exit
        private final Closure<Connection> getConnection

        CLI(String[] args, Closure<String> getenv, Closure exit, Closure<Connection> getConnection) {
            this.args = args
            this.getenv = getenv
            this.exit = exit
            this.getConnection = getConnection
        }

        int argIx(String target) {
            args.findIndexOf({ it == target })
        }

        String arg(String target, String fallback=null) {
            int ix = argIx(target)
            if (ix < 0 || ix + 1 >= args.length) {
                return fallback
            }
            args[ix + 1]
        }

        DBConfig account() {
            String account = arg("--account")
            if (!account) {
                logger.warning("Usage: java -jar JAR --account A [--run abc.sql]")
                exit(1)
            }
            Properties config = null
            try {
                config = fromEnv(account, getenv)
            } catch (IllegalStateException oops) {
                logger.warning("Config missing from environment: $oops")
                exit(1)
            } catch (ClassNotFoundException oops) {
                logger.warning("driver not found (fix CLASSPATH?): $oops")
                exit(1)
            }
            String url = config.getProperty('url')
            logger.info("$account: $url")
            new DBConfig(url, config, getConnection)
        }
    }
}