package models

type SettingStruct struct {
	Server struct {
		MaxConcurrentThreads int    `yaml:"max_concurrent_threads" json:"max_concurrent_threads"`
		KeystorePath         string `yaml:"keystore_path" json:"keystore_path"`
		KeystorePassword     string `yaml:"keystore_password" json:"keystore_password"`
		TruststorePath       string `yaml:"truststore_path" json:"truststore_path"`
		TruststorePassword   string `yaml:"truststore_password" json:"truststore_password"`
	} `yaml:"server" json:"server"`
	Connections []SettingConnectionStruct `yaml:"connections" json:"connections"`
}

type SettingBrokerStruct struct {
	BootstrapServer string `yaml:"bootstrap_server" json:"bootstrap_server"`
	Topic           string `yaml:"topic" json:"topic"`
}

type MainFlowTemplateDataStruct struct {
	MAXCONCURRENTTHREADS int
	KEYSTORE_PATH        string
	KEYSTORE_PASSWORD    string
	TRUSTSTORE_PATH      string
	TRUSTSTORE_PASSWORD  string
}

type MainFlowTemplateStruct struct {
	BOOTSTRAP_SERVER string
	CONNECTION_NAME  string
	HOST             string
	PORT             int32
	NAME             string
	USERNAME         string
	PASSWORD         string
}

type ConnectoionTemplateStruct struct {
	CONNECTION_UUID string
	CONNECTION_NAME string
	HOST            string
	PORT            int32
	NAME            string
	USERNAME        string
	PASSWORD        string
}

type FlowTemplateStruct struct {
	FLOW_UUID        string
	CONNECTION_UUID  string
	CONNECTION_NAME  string
	CRON_ALL         string
	MANUAL_PATH      string
	HIS_NAME         string
	DAYAGO           int
	TOPIC            string
	HOSPCODE         string
	BOOTSTRAP_SERVER string
	CRON_QUERY       string
	CONNECTION_PATH  string
}

type FlowMainStruct struct {
	MiNiFiConfigVersion int `yaml:"MiNiFi Config Version"`
	FlowController      struct {
		Name    string `yaml:"name"`
		Comment string `yaml:"comment"`
	} `yaml:"Flow Controller"`
	CoreProperties struct {
		FlowControllerGracefulShutdownPeriod string      `yaml:"flow controller graceful shutdown period"`
		FlowServiceWriteDelayInterval        string      `yaml:"flow service write delay interval"`
		AdministrativeYieldDuration          string      `yaml:"administrative yield duration"`
		BoredYieldDuration                   string      `yaml:"bored yield duration"`
		MaxConcurrentThreads                 interface{} `yaml:"max concurrent threads"`
		VariableRegistryProperties           string      `yaml:"variable registry properties"`
	} `yaml:"Core Properties"`
	FlowFileRepository struct {
		Implementation     string `yaml:"implementation"`
		Partitions         int    `yaml:"partitions"`
		CheckpointInterval string `yaml:"checkpoint interval"`
		AlwaysSync         bool   `yaml:"always sync"`
		Swap               struct {
			Threshold  int    `yaml:"threshold"`
			InPeriod   string `yaml:"in period"`
			InThreads  int    `yaml:"in threads"`
			OutPeriod  string `yaml:"out period"`
			OutThreads int    `yaml:"out threads"`
		} `yaml:"Swap"`
	} `yaml:"FlowFile Repository"`
	ContentRepository struct {
		Implementation                             string `yaml:"implementation"`
		ContentClaimMaxAppendableSize              string `yaml:"content claim max appendable size"`
		ContentClaimMaxFlowFiles                   int    `yaml:"content claim max flow files"`
		ContentRepositoryArchiveEnabled            bool   `yaml:"content repository archive enabled"`
		ContentRepositoryArchiveMaxRetentionPeriod string `yaml:"content repository archive max retention period"`
		ContentRepositoryArchiveMaxUsagePercentage string `yaml:"content repository archive max usage percentage"`
		AlwaysSync                                 bool   `yaml:"always sync"`
	} `yaml:"Content Repository"`
	ProvenanceRepository struct {
		ProvenanceRolloverTime   string `yaml:"provenance rollover time"`
		Implementation           string `yaml:"implementation"`
		ProvenanceIndexShardSize string `yaml:"provenance index shard size"`
		ProvenanceMaxStorageSize string `yaml:"provenance max storage size"`
		ProvenanceMaxStorageTime string `yaml:"provenance max storage time"`
		ProvenanceBufferSize     int    `yaml:"provenance buffer size"`
	} `yaml:"Provenance Repository"`
	ComponentStatusRepository struct {
		BufferSize        int    `yaml:"buffer size"`
		SnapshotFrequency string `yaml:"snapshot frequency"`
	} `yaml:"Component Status Repository"`
	SecurityProperties struct {
		Keystore           string `yaml:"keystore"`
		KeystoreType       string `yaml:"keystore type"`
		KeystorePassword   string `yaml:"keystore password"`
		KeyPassword        string `yaml:"key password"`
		Truststore         string `yaml:"truststore"`
		TruststoreType     string `yaml:"truststore type"`
		TruststorePassword string `yaml:"truststore password"`
		SslProtocol        string `yaml:"ssl protocol"`
		SensitiveProps     struct {
			Key       interface{} `yaml:"key"`
			Algorithm string      `yaml:"algorithm"`
		} `yaml:"Sensitive Props"`
	} `yaml:"Security Properties"`
	Processors              []interface{}             `yaml:"Processors"`
	ControllerServices      []ControllerServiceStruct `yaml:"Controller Services"`
	ProcessGroups           []FlowStruct              `yaml:"Process Groups"`
	InputPorts              []interface{}             `yaml:"Input Ports"`
	OutputPorts             []interface{}             `yaml:"Output Ports"`
	Funnels                 []interface{}             `yaml:"Funnels"`
	Connections             []interface{}             `yaml:"Connections"`
	RemoteProcessGroups     []interface{}             `yaml:"Remote Process Groups"`
	NiFiPropertiesOverrides struct {
	} `yaml:"NiFi Properties Overrides"`
}

type ControllerServiceStruct struct {
	ID         string      `yaml:"id"`
	Name       string      `yaml:"name"`
	Type       string      `yaml:"type"`
	Properties interface{} `yaml:"Properties"`
}

type FlowStruct struct {
	ID         string `yaml:"id"`
	Name       string `yaml:"name"`
	Processors []struct {
		ID                              string        `yaml:"id"`
		Name                            string        `yaml:"name"`
		Class                           string        `yaml:"class"`
		MaxConcurrentTasks              int           `yaml:"max concurrent tasks"`
		SchedulingStrategy              string        `yaml:"scheduling strategy"`
		SchedulingPeriod                string        `yaml:"scheduling period"`
		PenalizationPeriod              string        `yaml:"penalization period"`
		YieldPeriod                     string        `yaml:"yield period"`
		RunDurationNanos                int           `yaml:"run duration nanos"`
		AutoTerminatedRelationshipsList []interface{} `yaml:"auto-terminated relationships list"`
		Properties                      struct{}      `yaml:"Properties,omitempty"`
	} `yaml:"Processors"`
	ControllerServices []struct{} `yaml:"Controller Services"`
	ProcessGroups      []struct{} `yaml:"Process Groups"`
	InputPorts         []struct{} `yaml:"Input Ports"`
	OutputPorts        []struct{} `yaml:"Output Ports"`
	Funnels            []struct {
		ID string `yaml:"id"`
	} `yaml:"Funnels"`
	Connections []struct {
		ID                      string   `yaml:"id"`
		Name                    string   `yaml:"name"`
		SourceID                string   `yaml:"source id"`
		SourceRelationshipNames []string `yaml:"source relationship names"`
		DestinationID           string   `yaml:"destination id"`
		MaxWorkQueueSize        int      `yaml:"max work queue size"`
		MaxWorkQueueDataSize    string   `yaml:"max work queue data size"`
		FlowfileExpiration      string   `yaml:"flowfile expiration"`
		QueuePrioritizerClass   string   `yaml:"queue prioritizer class"`
	} `yaml:"Connections"`
	// RemoteProcessGroups []struct{} `yaml:"Remote Process Groups"`
}

type SettingConnectionStruct struct {
	ID       string `yaml:"id" json:"id"`
	Name     string `yaml:"name" json:"name"`
	Hospcode string `yaml:"hospcode" json:"hospcode"`
	Type     string `yaml:"type" json:"type"`
	HisName  string `yaml:"his_name" json:"his_name"`
	Host     string `yaml:"host" json:"host"`
	Port     int32  `yaml:"port" json:"port"`
	Database string `yaml:"database" json:"database"`
	Schema   string `yaml:"schema" json:"schema"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Cronjob  struct {
		CronjobQuery `yaml:"cronjob_query" json:"cronjob_query"`
		CronjobAll   `yaml:"cronjob_all" json:"cronjob_all"`
	} `yaml:"cronjob" json:"cronjob"`
	Broker SettingBrokerStruct `yaml:"broker" json:"broker"`
}

type CronjobQuery struct {
	Dayago  int    `yaml:"dayago" json:"dayago"`
	RunTime string `yaml:"run_time" json:"run_time"`
}

type CronjobAll struct {
	RunEvery int    `yaml:"run_every" json:"run_every"`
	RunTime  string `yaml:"run_time" json:"run_time"`
}

type StatusFileJsonStruct struct {
	Table       string `json:"table"`
	Status      string `json:"status" yaml:"status"`
	Percent     int    `json:"percent" yaml:"percent"`
	LastCreated string `json:"last_created" yaml:"last_created"`
	SendSuccess string `json:"send_success" yaml:"send_success"`
}
