import groovy.transform.Immutable

@Immutable
class Password {
    final String value

    @Override
    String toString() {
        return "...${value.length()}..."
    }
}