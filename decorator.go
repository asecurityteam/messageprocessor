package messageprocessor

// Decorator is a named type for any function that takes a RoundTripper and
// returns a RoundTripper.
type Decorator func(MessageProcessor) MessageProcessor

// Chain is an ordered collection of Decorators.
type Chain []Decorator

// Apply wraps the given RoundTripper with the Decorator chain.
func (c Chain) Apply(base MessageProcessor) MessageProcessor {
	for x := len(c) - 1; x >= 0; x = x - 1 {
		base = c[x](base)
	}
	return base
}
