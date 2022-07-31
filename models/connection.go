package models

type MSSQLConnectionStruct struct {
	ID         interface{} `yaml:"id"`
	Name       interface{} `yaml:"name"`
	Type       string      `yaml:"type"`
	Properties struct {
		DatabaseConnectionURL        string      `yaml:"Database Connection URL"`
		DatabaseDriverClassName      string      `yaml:"Database Driver Class Name"`
		DatabaseUser                 interface{} `yaml:"Database User"`
		MaxTotalConnections          string      `yaml:"Max Total Connections"`
		MaxWaitTime                  string      `yaml:"Max Wait Time"`
		Password                     interface{} `yaml:"Password"`
		ValidationQuery              interface{} `yaml:"Validation-query"`
		DatabaseDriverLocations      string      `yaml:"database-driver-locations"`
		DbcpMaxConnLifetime          string      `yaml:"dbcp-max-conn-lifetime"`
		DbcpMaxIdleConns             string      `yaml:"dbcp-max-idle-conns"`
		DbcpMinEvictableIdleTime     string      `yaml:"dbcp-min-evictable-idle-time"`
		DbcpMinIdleConns             string      `yaml:"dbcp-min-idle-conns"`
		DbcpSoftMinEvictableIdleTime string      `yaml:"dbcp-soft-min-evictable-idle-time"`
		DbcpTimeBetweenEvictionRuns  string      `yaml:"dbcp-time-between-eviction-runs"`
		KerberosCredentialsService   interface{} `yaml:"kerberos-credentials-service"`
		KerberosPassword             interface{} `yaml:"kerberos-password"`
		KerberosPrincipal            interface{} `yaml:"kerberos-principal"`
		KerberosUserService          interface{} `yaml:"kerberos-user-service"`
	} `yaml:"Properties"`
}

type MySQLConnectionStruct struct {
	ID         interface{} `yaml:"id"`
	Name       interface{} `yaml:"name"`
	Type       string      `yaml:"type"`
	Properties struct {
		DatabaseConnectionURL        string      `yaml:"Database Connection URL"`
		DatabaseDriverClassName      string      `yaml:"Database Driver Class Name"`
		DatabaseUser                 interface{} `yaml:"Database User"`
		MaxTotalConnections          string      `yaml:"Max Total Connections"`
		MaxWaitTime                  string      `yaml:"Max Wait Time"`
		Password                     interface{} `yaml:"Password"`
		ValidationQuery              interface{} `yaml:"Validation-query"`
		DatabaseDriverLocations      string      `yaml:"database-driver-locations"`
		DbcpMaxConnLifetime          string      `yaml:"dbcp-max-conn-lifetime"`
		DbcpMaxIdleConns             string      `yaml:"dbcp-max-idle-conns"`
		DbcpMinEvictableIdleTime     string      `yaml:"dbcp-min-evictable-idle-time"`
		DbcpMinIdleConns             string      `yaml:"dbcp-min-idle-conns"`
		DbcpSoftMinEvictableIdleTime string      `yaml:"dbcp-soft-min-evictable-idle-time"`
		DbcpTimeBetweenEvictionRuns  string      `yaml:"dbcp-time-between-eviction-runs"`
		KerberosCredentialsService   interface{} `yaml:"kerberos-credentials-service"`
		KerberosPassword             interface{} `yaml:"kerberos-password"`
		KerberosPrincipal            interface{} `yaml:"kerberos-principal"`
		KerberosUserService          interface{} `yaml:"kerberos-user-service"`
	} `yaml:"Properties"`
}

type OracleConnectionStruct struct {
	ID         interface{} `yaml:"id"`
	Name       interface{} `yaml:"name"`
	Type       string      `yaml:"type"`
	Properties struct {
		DatabaseConnectionURL        string      `yaml:"Database Connection URL"`
		DatabaseDriverClassName      string      `yaml:"Database Driver Class Name"`
		DatabaseUser                 interface{} `yaml:"Database User"`
		MaxTotalConnections          string      `yaml:"Max Total Connections"`
		MaxWaitTime                  string      `yaml:"Max Wait Time"`
		Password                     interface{} `yaml:"Password"`
		ValidationQuery              interface{} `yaml:"Validation-query"`
		DatabaseDriverLocations      string      `yaml:"database-driver-locations"`
		DbcpMaxConnLifetime          string      `yaml:"dbcp-max-conn-lifetime"`
		DbcpMaxIdleConns             string      `yaml:"dbcp-max-idle-conns"`
		DbcpMinEvictableIdleTime     string      `yaml:"dbcp-min-evictable-idle-time"`
		DbcpMinIdleConns             string      `yaml:"dbcp-min-idle-conns"`
		DbcpSoftMinEvictableIdleTime string      `yaml:"dbcp-soft-min-evictable-idle-time"`
		DbcpTimeBetweenEvictionRuns  string      `yaml:"dbcp-time-between-eviction-runs"`
		KerberosCredentialsService   interface{} `yaml:"kerberos-credentials-service"`
		KerberosPassword             interface{} `yaml:"kerberos-password"`
		KerberosPrincipal            interface{} `yaml:"kerberos-principal"`
		KerberosUserService          interface{} `yaml:"kerberos-user-service"`
	} `yaml:"Properties"`
}

type PostgreSQLConnectionStruct struct {
	ID         interface{} `yaml:"id"`
	Name       interface{} `yaml:"name"`
	Type       string      `yaml:"type"`
	Properties struct {
		DatabaseConnectionURL        string      `yaml:"Database Connection URL"`
		DatabaseDriverClassName      string      `yaml:"Database Driver Class Name"`
		DatabaseUser                 interface{} `yaml:"Database User"`
		MaxTotalConnections          string      `yaml:"Max Total Connections"`
		MaxWaitTime                  string      `yaml:"Max Wait Time"`
		Password                     interface{} `yaml:"Password"`
		ValidationQuery              interface{} `yaml:"Validation-query"`
		DatabaseDriverLocations      string      `yaml:"database-driver-locations"`
		DbcpMaxConnLifetime          string      `yaml:"dbcp-max-conn-lifetime"`
		DbcpMaxIdleConns             string      `yaml:"dbcp-max-idle-conns"`
		DbcpMinEvictableIdleTime     string      `yaml:"dbcp-min-evictable-idle-time"`
		DbcpMinIdleConns             string      `yaml:"dbcp-min-idle-conns"`
		DbcpSoftMinEvictableIdleTime string      `yaml:"dbcp-soft-min-evictable-idle-time"`
		DbcpTimeBetweenEvictionRuns  string      `yaml:"dbcp-time-between-eviction-runs"`
		KerberosCredentialsService   interface{} `yaml:"kerberos-credentials-service"`
		KerberosPassword             interface{} `yaml:"kerberos-password"`
		KerberosPrincipal            interface{} `yaml:"kerberos-principal"`
		KerberosUserService          interface{} `yaml:"kerberos-user-service"`
	} `yaml:"Properties"`
}
