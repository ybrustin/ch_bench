package fnf_ch

import "fmt"

func getCreateTableDdl(tableName string) string {
	const ddl = `
CREATE TABLE IF NOT EXISTS sdflow.%s (
	ts DateTime,
	tenantId LowCardinality(String),
	siteId String,
	exporterIp String,

	appBr LowCardinality(String),
	appName String,
	appTc LowCardinality(String),

	clientBytes UInt64,
	clientIp String,
	clientMac String,
	clientPkts UInt64,

	enrichClientManufacturer String,
	enrichClientModel String,
	enrichClientOs String,
	enrichClientType String,
	enrichServerManufacturer String,
	enrichServerModel String,
	enrichServerOs String,
	enrichServerType String,

	newCon UInt32,
	protocol UInt8,

	serverBytes UInt64,
	serverIp String,
	serverMac String,
	serverPkts UInt64,
	serverPort UInt16,

	tcpClientRetrans UInt32,
	tcpDelayServerApp UInt32,
	tcpDelayServerNet UInt32,
	tcpDelayTotalNet UInt32,
	tcpServerResponses UInt32,
	tcpServerRetrans UInt32
)
ENGINE = %s
`

	const mergeTreeParams = `ReplicatedMergeTree
PARTITION BY toYear(ts) * 512 + toDayOfYear(ts)
ORDER BY (tenantId, ts)
`

	var engine string
	if envNullTable {
		engine = "Null"
	} else {
		engine = mergeTreeParams
	}
	return fmt.Sprintf(ddl, tableName, engine)
}
