class HelloTest extends GroovyTestCase {
    void 'test Hello should return "Hello, World!"' () {
        assert new Loader().world == "Hello, World!"
    }
}
