package models

type MySQLFlowStruct struct {
	ID         string      `yaml:"id"`
	Name       interface{} `yaml:"name"`
	Processors []struct {
		ID                              string      `yaml:"id"`
		Name                            string      `yaml:"name"`
		Class                           string      `yaml:"class"`
		MaxConcurrentTasks              int         `yaml:"max concurrent tasks"`
		SchedulingStrategy              string      `yaml:"scheduling strategy"`
		SchedulingPeriod                string      `yaml:"scheduling period"`
		PenalizationPeriod              string      `yaml:"penalization period"`
		YieldPeriod                     string      `yaml:"yield period"`
		RunDurationNanos                int         `yaml:"run duration nanos"`
		AutoTerminatedRelationshipsList []string    `yaml:"auto-terminated relationships list"`
		Properties                      interface{} `yaml:"Properties,omitempty"`
	} `yaml:"Processors"`
	ControllerServices []interface{} `yaml:"Controller Services"`
	InputPorts         []interface{} `yaml:"Input Ports"`
	OutputPorts        []interface{} `yaml:"Output Ports"`
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
	RemoteProcessGroups []interface{} `yaml:"Remote Process Groups"`
}
