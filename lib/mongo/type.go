package mongo

type MongoDialSettings struct {
	Timeout int
	Ssl     bool
}

type MongoSessionSettings struct {
	SocketTimeout int `toml:"socket-timeout"`
	SyncTimeout   int `toml:"sync-timeout"`
}
