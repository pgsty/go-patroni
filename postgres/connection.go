package postgres

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const defaultConnectTimeout = 5 * time.Second

type TLSMode string

const (
	TLSVerifyFull TLSMode = "verify-full"
	TLSInsecure   TLSMode = "insecure"
	TLSDisable    TLSMode = "disable"
	TLSFromSource TLSMode = "source"
)

type ConnectionOptions struct {
	Host            string        `json:"-"`
	Port            uint16        `json:"-"`
	Database        string        `json:"-"`
	Username        string        `json:"-"`
	ApplicationName string        `json:"-"`
	ConnectTimeout  time.Duration `json:"-"`

	connectionString string
	password         string
	tlsMode          TLSMode
	tlsConfig        *tls.Config
}

func NewConnectionOptions(connectionString string) ConnectionOptions {
	return ConnectionOptions{connectionString: connectionString, tlsMode: TLSVerifyFull}
}

func (options ConnectionOptions) WithPassword(password string) ConnectionOptions {
	options.password = password
	return options
}

func (options ConnectionOptions) WithTLSMode(mode TLSMode) ConnectionOptions {
	options.tlsMode = mode
	return options
}

func (options ConnectionOptions) WithTLSConfig(configuration *tls.Config) ConnectionOptions {
	if configuration == nil {
		options.tlsConfig = nil
	} else {
		options.tlsConfig = configuration.Clone()
	}
	return options
}

func (options ConnectionOptions) TLSMode() TLSMode {
	if options.tlsMode == "" {
		return TLSVerifyFull
	}
	return options.tlsMode
}

func (options ConnectionOptions) String() string {
	return fmt.Sprintf("postgres.ConnectionOptions{connectionString:%t,host:%t,port:%t,database:%t,username:%t,password:%t,applicationName:%t,connectTimeout:%s,tls:%q,customTLS:%t}",
		options.connectionString != "", options.Host != "", options.Port != 0, options.Database != "",
		options.Username != "", options.password != "", options.ApplicationName != "", options.ConnectTimeout,
		options.TLSMode(), options.tlsConfig != nil)
}

func (options ConnectionOptions) GoString() string { return options.String() }

func (options ConnectionOptions) connectConfig() (*pgx.ConnConfig, error) {
	configuration, err := pgx.ParseConfig(options.parseSource())
	if err != nil {
		return nil, newError(ErrorConfiguration, "parse-connection", err)
	}
	if options.Host != "" {
		configuration.Host = options.Host
	}
	if options.Port != 0 {
		configuration.Port = options.Port
	}
	if options.Database != "" {
		configuration.Database = options.Database
	}
	if options.Username != "" {
		configuration.User = options.Username
	}
	if options.password != "" {
		configuration.Password = options.password
	}
	if options.ConnectTimeout > 0 {
		configuration.ConnectTimeout = options.ConnectTimeout
	} else {
		configuration.ConnectTimeout = defaultConnectTimeout
	}
	applicationName := options.ApplicationName
	if applicationName == "" {
		applicationName = "go-patroni"
	}
	if configuration.RuntimeParams == nil {
		configuration.RuntimeParams = map[string]string{}
	}
	if configuration.RuntimeParams["application_name"] == "" {
		configuration.RuntimeParams["application_name"] = applicationName
	}
	if err := applyTLSMode(configuration, options); err != nil {
		return nil, err
	}
	return configuration, nil
}

// parseSource puts explicit target fields into pgx's initial parse whenever
// there is no caller-supplied connection string. This is significant for
// target-dependent standard sources such as .pgpass: pgx must match the final
// host, port, database, and user rather than environment defaults that control
// will subsequently replace.
func (options ConnectionOptions) parseSource() string {
	if options.connectionString != "" {
		return options.connectionString
	}
	settings := make([]string, 0, 6)
	appendSetting := func(key, value string) {
		if value != "" {
			settings = append(settings, key+"="+quoteConnectionValue(value))
		}
	}
	appendSetting("host", options.Host)
	if options.Port != 0 {
		appendSetting("port", strconv.FormatUint(uint64(options.Port), 10))
	}
	appendSetting("dbname", options.Database)
	appendSetting("user", options.Username)
	appendSetting("application_name", options.ApplicationName)
	return strings.Join(settings, " ")
}

func quoteConnectionValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `'`, `\'`)
	return "'" + value + "'"
}

func applyTLSMode(configuration *pgx.ConnConfig, options ConnectionOptions) error {
	mode := options.TLSMode()
	if mode != TLSVerifyFull && mode != TLSInsecure && mode != TLSDisable && mode != TLSFromSource {
		return configurationError("tls", "unknown TLS mode")
	}
	if mode == TLSFromSource {
		configuration.TLSConfig = minimumTLS(configuration.TLSConfig)
		for _, fallback := range configuration.Fallbacks {
			fallback.TLSConfig = minimumTLS(fallback.TLSConfig)
		}
		return nil
	}
	if mode == TLSDisable {
		configuration.TLSConfig = nil
		for _, fallback := range configuration.Fallbacks {
			fallback.TLSConfig = nil
		}
		return nil
	}
	base := configuration.TLSConfig
	if options.tlsConfig != nil {
		base = options.tlsConfig
	}
	configuration.TLSConfig = enforcedTLS(base, configuration.Host, mode == TLSInsecure)
	for _, fallback := range configuration.Fallbacks {
		fallbackBase := fallback.TLSConfig
		if options.tlsConfig != nil {
			fallbackBase = options.tlsConfig
		} else if fallbackBase == nil {
			fallbackBase = base
		}
		fallback.TLSConfig = enforcedTLS(fallbackBase, fallback.Host, mode == TLSInsecure)
	}
	return nil
}

func minimumTLS(configuration *tls.Config) *tls.Config {
	if configuration == nil {
		return nil
	}
	cloned := configuration.Clone()
	if cloned.MinVersion < tls.VersionTLS12 {
		cloned.MinVersion = tls.VersionTLS12
	}
	return cloned
}

func enforcedTLS(configuration *tls.Config, serverName string, insecure bool) *tls.Config {
	if configuration == nil {
		configuration = &tls.Config{}
	} else {
		configuration = configuration.Clone()
	}
	if configuration.MinVersion < tls.VersionTLS12 {
		configuration.MinVersion = tls.VersionTLS12
	}
	configuration.InsecureSkipVerify = insecure //nolint:gosec -- explicit observable compatibility option
	if insecure {
		configuration.ServerName = ""
		configuration.VerifyPeerCertificate = nil
		configuration.VerifyConnection = nil
	} else {
		configuration.ServerName = serverName
		configuration.VerifyPeerCertificate = nil
	}
	return configuration
}

type Connector interface {
	Connect(context.Context, *pgx.ConnConfig) (Connection, error)
}

type Connection interface {
	Execute(context.Context, string) MultiResultReader
	Close(context.Context) error
}

type MultiResultReader interface {
	NextResult() bool
	ResultReader() ResultReader
	Close() error
}

type ResultReader interface {
	Columns() []Column
	NextRow() bool
	Values() [][]byte
	Close() (CommandTag, error)
}

type nativeConnector struct{}

func (nativeConnector) Connect(ctx context.Context, configuration *pgx.ConnConfig) (Connection, error) {
	connection, err := pgx.ConnectConfig(ctx, configuration)
	if err != nil {
		return nil, err
	}
	return &nativeConnection{connection: connection}, nil
}

type nativeConnection struct{ connection *pgx.Conn }

func (connection *nativeConnection) Execute(ctx context.Context, sql string) MultiResultReader {
	return &nativeMultiResult{reader: connection.connection.PgConn().Exec(ctx, sql)}
}

func (connection *nativeConnection) Close(ctx context.Context) error {
	return connection.connection.Close(ctx)
}

type nativeMultiResult struct {
	reader *pgconn.MultiResultReader
	rows   *nativeRows
}

func (reader *nativeMultiResult) NextResult() bool {
	if !reader.reader.NextResult() {
		reader.rows = nil
		return false
	}
	reader.rows = &nativeRows{reader: reader.reader.ResultReader()}
	return true
}

func (reader *nativeMultiResult) ResultReader() ResultReader { return reader.rows }
func (reader *nativeMultiResult) Close() error               { return reader.reader.Close() }

type nativeRows struct{ reader *pgconn.ResultReader }

func (rows *nativeRows) Columns() []Column {
	fields := rows.reader.FieldDescriptions()
	columns := make([]Column, len(fields))
	for index, field := range fields {
		columns[index] = Column{
			Name: field.Name, TableOID: field.TableOID, TableAttributeNumber: field.TableAttributeNumber,
			DataTypeOID: field.DataTypeOID, DataTypeSize: field.DataTypeSize,
			TypeModifier: field.TypeModifier, Format: field.Format,
		}
	}
	return columns
}

func (rows *nativeRows) NextRow() bool    { return rows.reader.NextRow() }
func (rows *nativeRows) Values() [][]byte { return rows.reader.Values() }
func (rows *nativeRows) Close() (CommandTag, error) {
	tag, err := rows.reader.Close()
	return CommandTag{Text: tag.String(), RowsAffected: tag.RowsAffected()}, err
}

var (
	_ Connector         = nativeConnector{}
	_ Connection        = (*nativeConnection)(nil)
	_ MultiResultReader = (*nativeMultiResult)(nil)
	_ ResultReader      = (*nativeRows)(nil)
)

func copyRow(values [][]byte) (Row, int64) {
	row := make(Row, len(values))
	var bytes int64
	for index, value := range values {
		if value == nil {
			row[index] = Cell{Null: true}
			continue
		}
		bytes += int64(len(value))
		row[index] = Cell{Text: string(value), Bytes: int64(len(value))}
	}
	return row, bytes
}

func connectionError(stage string, err error) error {
	if err == nil {
		return nil
	}
	var typed *Error
	if errors.As(err, &typed) {
		return typed
	}
	return newError(ErrorConnect, stage, err)
}
