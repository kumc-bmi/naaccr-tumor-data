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

    static void main(String[] args) {
        if (args.length < 1) {
            logger.warning("Usage: java -jar loader.jar ACCOUNT")
            System.exit(1)
        }
        def account = args[0]
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
            sql.eachRow('select * from global_name') { GroovyResultSet row ->
                logger.info("${config.username}@${row.getAt('global_name')}: loading ...")
            }
        }
    }
}
