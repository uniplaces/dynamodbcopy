package dynamodbcopy

import "github.com/spf13/viper"

type Config struct {
	SourceTable       string `mapstructure:"source-table"`
	TargetTable       string `mapstructure:"target-table"`
	SourceProfile     string `mapstructure:"source-profile"`
	TargetProfile     string `mapstructure:"target-profile"`
	ReadProvisioning  int64  `mapstructure:"read-units"`
	WriteProvisioning int64  `mapstructure:"write-units"`
}

func NewConfig(viperConfig viper.Viper) (Config, error) {
	config := Config{}

	if err := viperConfig.Unmarshal(&config); err != nil {
		return config, err
	}

	return config, nil
}
