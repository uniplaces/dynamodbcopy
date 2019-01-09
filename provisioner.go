package dynamodbcopy

import "github.com/aws/aws-sdk-go/service/dynamodb"

type Provisioner interface {
	Fetch() (Provisioning, error)
	Update(provisioning Provisioning) (Provisioning, error)
}

type provisioningService struct {
	srcTable DynamoDBService
	trgTable DynamoDBService
}

func NewProvisioner(srcTableService, trgTableService DynamoDBService) Provisioner {
	return provisioningService{
		srcTable: srcTableService,
		trgTable: trgTableService,
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

	return Provisioning{*srcDescription, *trgDescription}, nil
}

func (dc provisioningService) Update(provisioning Provisioning) (Provisioning, error) {
	currentProvisioning, err := dc.Fetch()
	if err != nil {
		return Provisioning{}, err
	}

	if needsProvisioningUpdate(currentProvisioning.SourceCapacity(), provisioning.SourceCapacity()) {
		if err := dc.srcTable.UpdateCapacity(provisioning.SourceCapacity()); err != nil {
			return Provisioning{}, err
		}
	}

	if needsProvisioningUpdate(currentProvisioning.TargetCapacity(), provisioning.TargetCapacity()) {
		if err := dc.trgTable.UpdateCapacity(provisioning.TargetCapacity()); err != nil {
			return Provisioning{}, err
		}
	}

	return provisioning, nil
}

func needsProvisioningUpdate(c1, c2 Capacity) bool {
	return c1.Read != c2.Read || c1.Write != c2.Write
}

type Provisioning struct {
	Source dynamodb.TableDescription
	Target dynamodb.TableDescription
}

type Capacity struct {
	Read  int64
	Write int64
}

func (p Provisioning) SourceCapacity() Capacity {
	return Capacity{
		Write: *p.Source.ProvisionedThroughput.WriteCapacityUnits,
		Read:  *p.Source.ProvisionedThroughput.ReadCapacityUnits,
	}
}

func (p Provisioning) TargetCapacity() Capacity {
	return Capacity{
		Write: *p.Target.ProvisionedThroughput.WriteCapacityUnits,
		Read:  *p.Target.ProvisionedThroughput.ReadCapacityUnits,
	}
}
