package fnf_ch

import (
	"fmt"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

var (
	envAsyncThreads    int
	envAsyncMaxMem     int
	envAsyncMaxTime    int
	envAsyncWait       bool
	envBulkAgg         int
	envBulkSize        int
	envDbHost          string
	envDbPass          string
	envDbPort          int
	envDbSecure        bool
	envDbUser          string
	envDedup           bool
	envDur             time.Duration
	envInputDir        string
	envNullTable       bool
	envTenantBulkDelay time.Duration
	envTenantRate      int
	envTenants         int
)

func envInputFill() {
	envInputDir = getEnvStr("envInputDir")
}

func envDbFill() {
	envDbHost = getEnvStr("envDbHost")
	envDbPass = getEnvStr("envDbPass")
	envDbPort = getEnvInt("envDbPort")
	envDbSecure = getEnvBool("envDbSecure")
	envDbUser = getEnvStr("envDbUser")
}

func envFill() {
	envDbFill()
	envInputFill()
	envAsyncThreads = getEnvInt("envAsyncThreads")
	envAsyncMaxMem = getEnvInt("envAsyncMaxMem")
	envAsyncMaxTime = getEnvInt("envAsyncMaxTime")
	envAsyncWait = getEnvBool("envAsyncWait")
	envBulkAgg = getEnvInt("envBulkAgg")
	envDedup = getEnvBool("envDedup")
	envDur = getEnvDur("envDur")
	envNullTable = getEnvBool("envNullTable")
	envTenantRate = getEnvInt("envTenantRate")
	envTenants = getEnvInt("envTenants")

	envBulkSize = envBulkAgg * envTenantRate
	envTenantBulkDelay = time.Second * time.Duration(envBulkAgg)
}

func getEnvParamsMap() map[string]any {
	return map[string]any{
		"envAsyncThreads": envAsyncThreads,
		"envAsyncMaxMem":  envAsyncMaxMem,
		"envAsyncMaxTime": envAsyncMaxTime,
		"envAsyncWait":    envAsyncWait,
		"envBulkAgg":      envBulkAgg,
		"envDur":          envDur,
		"envNullTable":    envNullTable,
		"envTenantRate":   envTenantRate,
		"envTenants":      envTenants,
	}
}

func envPrint() {
	fmt.Println("================")
	fmt.Println("env:")
	m := getEnvParamsMap()
	keys := maps.Keys(m)
	slices.Sort(keys)
	for _, k := range keys {
		fmt.Printf("%15s: %v\n", k, m[k])
	}
	fmt.Println("================")
}
