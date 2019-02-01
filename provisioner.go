package dynamodbcopy

import "github.com/aws/aws-sdk-go/service/dynamodb"

// Provisioner is the interface that provides the methods to manipulate DynamoDB's provisioning values
type Provisioner interface {
	Fetch() (Provisioning, error)
	Update(provisioning Provisioning) (Provisioning, error)
}

type provisioningService struct {
	srcTable DynamoDBService
	trgTable DynamoDBService
	logger   Logger
}

// NewProvisioner returns a new Provisioner to manipulate the source and target table provisioning values
func NewProvisioner(srcTableService, trgTableService DynamoDBService, logger Logger) Provisioner {
	return provisioningService{
		srcTable: srcTableService,
		trgTable: trgTableService,
		logger:   logger,
	}
}

// Fetch returns the current provisioning values for the source and target DynamoDB tables
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

// Update will update the provisioning of the source and target table with the provided Provisioning value
//
// For each table, Update checks if the given provisioning value differs from the current provisioning value
// on each table. If so, it will update each table accordingly.
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

// Capacity abstracts the read and write units capacities values
type Capacity struct {
	Read  int64
	Write int64
}

// Provisioning stores the provisioning capacities for the source and target tables
// The Capacity for each table will be nil when the table's billing mode isn't BillingModeProvisioned
type Provisioning struct {
	Source *Capacity
	Target *Capacity
}

// NewProvisioning creates a new Provisioning based on the source and target tables dynamodb.TableDescription
// It will only set capacity for each table if the is BillingModeProvisioned
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
