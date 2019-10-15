import java.util.logging.Logger
import groovy.sql.Sql
import groovy.sql.GroovyResultSet
import groovy.transform.CompileStatic

@CompileStatic
class Loader {
    static Logger logger = Logger.getLogger("")

    static void main(String[] args) {
        if (args.length < 1) {
            logger.warning("Usage: java -jar loader.jar ACCOUNT")
            System.exit(1)
        }
        def account = args[0]
        logger.info("getting config for $account")
        def config = {
            def name = "${account}_${it}"
            def val = System.getenv(name)
            if (val == null) {
                logger.warning("Missing environment: $name")
                System.exit(1)
            }
            val
        }
        def db_url = config("URL")
        def username = config("USER")
        def password = config("PASSWORD")
        def driver = config("DRIVER")
        logger.info("$account config: URL=$db_url; user=$username password=...${password.length()} driver=$driver")
        try {
            def driverClass = Class.forName(driver)
            logger.info("driver: $driverClass")
        } catch (ClassNotFoundException oops) {
            logger.warning("driver $driver not found (fix CLASSPATH?): $oops")
            System.exit(1)
        }
        Sql.withInstance(db_url, username, password, driver) { Sql sql ->
            sql.eachRow('select * from global_name') { GroovyResultSet row ->
                logger.info("${username}@${row.getAt('global_name')}: loading ...")
            }
        }
    }
}
