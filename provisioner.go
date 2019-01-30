package dynamodbcopy

import "github.com/aws/aws-sdk-go/service/dynamodb"

type Provisioner interface {
	Fetch() (Provisioning, error)
	Update(provisioning Provisioning) (Provisioning, error)
}

type provisioningService struct {
	srcTable DynamoDBService
	trgTable DynamoDBService
	logger   Logger
}

func NewProvisioner(srcTableService, trgTableService DynamoDBService, logger Logger) Provisioner {
	return provisioningService{
		srcTable: srcTableService,
		trgTable: trgTableService,
		logger:   logger,
	}
}

func (dc provisioningService) Fetch() (Provisioning, error) {
	srcDescription, err := dc.srcTable.DescribeTable()
	if err != nil {
		return Provisioning{}, err
	}

	trgDescription, err := dc.trgTable.DescribeTable()
	if err != nil {
		return Provisioning{}, err
	}

	return NewProvisioning(srcDescription, trgDescription), nil
}

func (dc provisioningService) Update(provisioning Provisioning) (Provisioning, error) {
	currentProvisioning, err := dc.Fetch()
	if err != nil {
		return Provisioning{}, err
	}

	if needsProvisioningUpdate(currentProvisioning.Source, provisioning.Source) {
		if err := dc.srcTable.UpdateCapacity(*provisioning.Source); err != nil {
			return Provisioning{}, err
		}

		dc.logger.Printf("updated source table r: %d w: %d", provisioning.Source.Read, provisioning.Source.Write)
	}

	if needsProvisioningUpdate(currentProvisioning.Target, provisioning.Target) {
		if err := dc.trgTable.UpdateCapacity(*provisioning.Target); err != nil {
			return Provisioning{}, err
		}

		dc.logger.Printf("updated target table r: %d w: %d", provisioning.Target.Read, provisioning.Target.Write)
	}

	return provisioning, nil
}

func needsProvisioningUpdate(c1, c2 *Capacity) bool {
	return c1 != nil && c2 != nil && (c1.Read != c2.Read || c1.Write != c2.Write)
}

type Capacity struct {
	Read  int64
	Write int64
}

type Provisioning struct {
	Source *Capacity
	Target *Capacity
}

func NewProvisioning(srcDescription, trgDescription *dynamodb.TableDescription) Provisioning {
	provisioning := Provisioning{}

	if *srcDescription.BillingModeSummary.BillingMode == dynamodb.BillingModeProvisioned {
		provisioning.Source = &Capacity{
			Write: *srcDescription.ProvisionedThroughput.WriteCapacityUnits,
			Read:  *srcDescription.ProvisionedThroughput.ReadCapacityUnits,
		}
	}

	if *trgDescription.BillingModeSummary.BillingMode == dynamodb.BillingModeProvisioned {
		provisioning.Target = &Capacity{
			Write: *trgDescription.ProvisionedThroughput.WriteCapacityUnits,
			Read:  *trgDescription.ProvisionedThroughput.ReadCapacityUnits,
		}
	}

	return provisioning
}
