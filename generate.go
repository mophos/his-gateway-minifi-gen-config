package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/mophos/minifi-gen-config/models"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

func main() {

	viper.SetConfigName("env")
	viper.AddConfigPath(".")

	confErr := viper.ReadInConfig()

	if confErr != nil {
		panic(confErr.Error())
	}

	var dataPath = viper.GetString("data.path")
	var connectionsPath = viper.GetString("data.connections")

	var settingFilePath = filepath.Join(dataPath, "data/config", "setting.yml")

	confData, errReadYaml := ioutil.ReadFile(settingFilePath)

	if errReadYaml != nil {
		log.Fatal(errReadYaml)
	}

	var configYaml models.SettingStruct

	errConnYaml := yaml.Unmarshal([]byte(confData), &configYaml)
	if errConnYaml != nil {
		log.Fatal(errConnYaml)
	}

	//Template generate
	templateDir := filepath.Join(dataPath, "data/template")
	tmpDir := filepath.Join(dataPath, "data/tmp")
	//file main.yml
	mainFlowFile := filepath.Join(templateDir, "main.yml")

	// Create tmp directory
	err := os.MkdirAll(tmpDir, os.ModePerm)
	if err != nil {
		log.Println("Create tmp directory: ", err.Error())
	}

	// Create tmp directory
	errCreateConnectionDir := os.MkdirAll(connectionsPath, os.ModePerm)
	if errCreateConnectionDir != nil {
		log.Println("Create connection directory: ", errCreateConnectionDir.Error())
	}

	mainFlowFileData := models.MainFlowTemplateDataStruct{
		MAXCONCURRENTTHREADS: configYaml.Server.MaxConcurrentThreads,
		KEYSTORE_PATH:        configYaml.Server.KeystorePath,
		KEYSTORE_PASSWORD:    configYaml.Server.KeystorePassword,
		TRUSTSTORE_PATH:      configYaml.Server.TruststorePath,
		TRUSTSTORE_PASSWORD:  configYaml.Server.TruststorePassword,
	}

	mainFlowFileTemplate, errParseMainFlowFile := template.ParseFiles(mainFlowFile)

	if errParseMainFlowFile != nil {
		log.Fatal("Read main.yml template: ", errParseMainFlowFile.Error())
	}

	// tmp file
	tmpMainFlowFilePath := filepath.Join(tmpDir, "main.yml")
	tmpMainFlowFile, errWriteTmpMainFlowFile := os.Create(tmpMainFlowFilePath)

	if errWriteTmpMainFlowFile != nil {
		log.Fatal("create file: ", errWriteTmpMainFlowFile.Error())
	}

	errWriteMainFlowFileTemplate := mainFlowFileTemplate.Execute(tmpMainFlowFile, mainFlowFileData)

	if errWriteMainFlowFileTemplate != nil {
		log.Fatal("create file: ", errWriteMainFlowFileTemplate.Error())
	}

	// Get all conections
	connections := configYaml.Connections

	if len(connections) == 0 {
		log.Fatal("create file: ", errWriteMainFlowFileTemplate.Error())
	}

	// read file main.yml
	mainFlowTmpPath, err := ioutil.ReadFile(tmpMainFlowFilePath)

	if err != nil {
		log.Fatal(err)
	}

	var mainFlowData models.FlowMainStruct

	errFlowMainYaml := yaml.Unmarshal([]byte(mainFlowTmpPath), &mainFlowData)
	if errFlowMainYaml != nil {
		log.Fatal(errFlowMainYaml.Error())
	}

	// connection loop
	for _, conn := range connections {

		connectionName := conn.Name
		connectionId := conn.ID
		databaseType := conn.Type

		connectionTmpDir := filepath.Join(tmpDir, connectionName)
		// configure connection.yml/flow.yml
		err := os.MkdirAll(connectionTmpDir, os.ModePerm)
		if err != nil {
			log.Fatal("Create tmp directory: ", err.Error())
		}

		// template path
		connectionFilePath := filepath.Join(templateDir, databaseType, "connection.yml")
		flowFilePath := filepath.Join(templateDir, databaseType, "flow.yml")

		// parse template file
		connectionFileTemplate, errParseConnectionFileTemplate := template.ParseFiles(connectionFilePath)

		if errParseConnectionFileTemplate != nil {
			log.Fatal("Read connection.yml file: ", errParseConnectionFileTemplate.Error())
		}

		flowFileTemplate, errParseFlowFileTemplate := template.ParseFiles(flowFilePath)

		if errParseFlowFileTemplate != nil {
			log.Fatal("Read connection.yml file: ", errParseFlowFileTemplate.Error())
		}

		connData := models.ConnectoionTemplateStruct{
			CONNECTION_UUID: connectionId,
			CONNECTION_NAME: connectionName,
			HOST:            conn.Host,
			PORT:            conn.Port,
			NAME:            conn.Database,
			USERNAME:        conn.Username,
			PASSWORD:        conn.Password,
		}

		cronQuery := strings.Split(conn.Cronjob.CronjobQuery.RunTime, ":")
		queryCronTab := fmt.Sprintf("%s %s * * *", cronQuery[1], cronQuery[0])

		cronAll := strings.Split(conn.Cronjob.CronjobAll.RunTime, ":")
		allCronTab := fmt.Sprintf("%s %s * * *", cronAll[1], cronAll[0])

		manualPath := filepath.Join(dataPath, "data/connections", connectionId, "table_manual")

		flowData := models.FlowTemplateStruct{
			CONNECTION_UUID:  connectionId,
			CONNECTION_NAME:  connectionName,
			MANUAL_PATH:      manualPath,
			HIS_NAME:         conn.HisName,
			DAYAGO:           conn.Cronjob.Dayago,
			TOPIC:            conn.Broker.Topic,
			HOSPCODE:         conn.Hospcode,
			BOOTSTRAP_SERVER: conn.Broker.BootstrapServer,
			CRON_QUERY:       queryCronTab,
			CRON_ALL:         allCronTab,
			CONNECTION_PATH:  connectionsPath,
		}

		// tmp connection file
		tmpConnectionFlowFilePath := filepath.Join(connectionTmpDir, "connection.yml")
		tmpConnectionFlowFile, errWriteTmpConnectionFlowFile := os.Create(tmpConnectionFlowFilePath)

		if errWriteTmpConnectionFlowFile != nil {
			log.Fatal("apply template data (connection.yml): ", errWriteTmpConnectionFlowFile.Error())
		}

		errWriteConnectionFlowFileTemplate := connectionFileTemplate.Execute(tmpConnectionFlowFile, connData)

		if errWriteConnectionFlowFileTemplate != nil {
			log.Fatal("create file: ", errWriteConnectionFlowFileTemplate.Error())
		}

		// tmp flow file
		tmpFlowFilePath := filepath.Join(connectionTmpDir, "flow.yml")
		tmpFlowFile, errWriteTmpFlowFile := os.Create(tmpFlowFilePath)

		if errWriteTmpFlowFile != nil {
			log.Fatal("apply template data (flow.yml): ", errWriteTmpFlowFile.Error())
		}

		errWriteFlowFileTemplate := flowFileTemplate.Execute(tmpFlowFile, flowData)

		if errWriteFlowFileTemplate != nil {
			log.Fatal("create file: ", errWriteFlowFileTemplate.Error())
		}

		// read connection/flow file

		var _connData models.ControllerServiceStruct
		var _flowData models.FlowStruct

		connYamlData, errReadConnection := ioutil.ReadFile(tmpConnectionFlowFilePath)

		flowYamlData, errReadFlow := ioutil.ReadFile(tmpFlowFilePath)

		if errReadConnection != nil {
			log.Fatal(errReadConnection)
		}

		if errReadFlow != nil {
			log.Fatal(errReadFlow)
		}

		errConnsYaml := yaml.Unmarshal([]byte(connYamlData), &_connData)
		if errConnYaml != nil {
			log.Fatal(errConnsYaml.Error())
		}

		errFlowsYaml := yaml.Unmarshal([]byte(flowYamlData), &_flowData)
		if errFlowsYaml != nil {
			log.Fatal(errFlowsYaml)
		}

		mainFlowData.ControllerServices = append(mainFlowData.ControllerServices, _connData)
		mainFlowData.ProcessGroups = append(mainFlowData.ProcessGroups, _flowData)

	} // end connection loop

	// create file
	yamlData, errMarshal := yaml.Marshal(&mainFlowData)
	if errMarshal != nil {
		log.Fatal(errMarshal.Error())
	}

	configPath := filepath.Join(dataPath, "data/config", "config.yml")
	errWriteFile := ioutil.WriteFile(configPath, yamlData, os.ModePerm)
	if errWriteFile != nil {
		log.Fatal(errWriteFile.Error())
	}

	// remove all tmp
	errSettingfile := os.RemoveAll(tmpDir)
	if errSettingfile != nil {
		log.Fatal(errSettingfile)
	}

	log.Println("****** Create config.yml successfully. ******")
}
