package types

type mongoDialSettings struct {
	Timeout int
	Ssl     bool
}

type mongoSessionSettings struct {
	SocketTimeout int `toml:"socket-timeout"`
	SyncTimeout   int `toml:"sync-timeout"`
}
