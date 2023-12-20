package dictionaries

type Dictionary interface {
	Identifier() string
	Set(key string, val string) error
	Get(key string) (string, error)
	Delete(key string) error
}
