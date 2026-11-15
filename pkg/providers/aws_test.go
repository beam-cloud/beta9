package providers

import (
	"context"
	"encoding/base64"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// awsCloud stands in for EC2. The client talks to it through the AWS SDK's
// own request and response types, so what is checked here is what the client
// asks for and what it makes of the answers, not how the SDK puts them on
// the wire. The stand-in keeps the rules of EC2's API reference for the
// calls this client makes: filters, idempotency tokens, error codes, and
// which states an instance goes through. The instance types are a few of
// AWS's published ones. The Spot prices are made up, because EC2 tells them
// to its customers only.
type awsCloud struct {
	t   *testing.T
	mu  sync.Mutex
	now time.Time
	// regions holds each region's EC2.
	regions map[string]*awsEC2
}

type awsEC2 struct {
	cloud  *awsCloud
	region string

	types     []ec2types.InstanceTypeInfo
	prices    []ec2types.SpotPrice
	subnets   []ec2types.Subnet
	images    []ec2types.Image
	instances []ec2types.Instance
	runs      []*ec2.RunInstancesInput
	// byToken remembers which instance an idempotency token ran.
	byToken map[string]string
	refusal error
	// terminateRefusal, when set, is the answer to every terminate.
	terminateRefusal error
	// describeRefusal, when set, is the answer to every question about
	// instances.
	describeRefusal error
	// priceRefusal, when set, is the answer to every question about prices.
	priceRefusal error
	// listsEverything makes the instance list ignore the filters asked for.
	listsEverything bool
	nextID          int
	calls           map[string]int
	// pageOf, when set, is the most a list returns at a time.
	pageOf int
	// repeatsToken makes every page of a list end with the same token, as
	// a list that never ends does. repeated counts those pages.
	repeatsToken bool
	repeated     int
}

const (
	awsTestImage    = "ami-0beam0000000newer"
	awsTestOldImage = "ami-0beam0000000older"
)

func awsTypeInfo(name string, vcpus int32, memoryGiB int64, gpu string, gpus, gpuMiB int32) ec2types.InstanceTypeInfo {
	return ec2types.InstanceTypeInfo{
		InstanceType:          ec2types.InstanceType(name),
		VCpuInfo:              &ec2types.VCpuInfo{DefaultVCpus: aws.Int32(vcpus)},
		MemoryInfo:            &ec2types.MemoryInfo{SizeInMiB: aws.Int64(memoryGiB * 1024)},
		ProcessorInfo:         &ec2types.ProcessorInfo{SupportedArchitectures: []ec2types.ArchitectureType{ec2types.ArchitectureTypeX8664}},
		SupportedUsageClasses: []ec2types.UsageClassType{ec2types.UsageClassTypeOnDemand, ec2types.UsageClassTypeSpot},
		GpuInfo: &ec2types.GpuInfo{Gpus: []ec2types.GpuDeviceInfo{{
			Name:         aws.String(gpu),
			Manufacturer: aws.String("NVIDIA"),
			Count:        aws.Int32(gpus),
			MemoryInfo:   &ec2types.GpuDeviceMemoryInfo{SizeInMiB: aws.Int32(gpuMiB)},
		}}},
	}
}

func newAWSCloud(t *testing.T) *awsCloud {
	cloud := &awsCloud{t: t, now: time.Date(2026, 11, 14, 3, 0, 0, 0, time.UTC), regions: map[string]*awsEC2{}}

	// What every region sells. The last five are not for this client: a
	// share of a GPU, a GPU that is not NVIDIA's, an Arm machine, a type
	// that is not sold as Spot and so has no Spot price, and a machine
	// without a GPU.
	share := awsTypeInfo("g6f.large", 2, 8, "L4", 1, 3072)
	share.GpuInfo.Gpus[0].GpuPartitionSize = aws.Float64(0.125)
	radeon := awsTypeInfo("g4ad.xlarge", 4, 16, "Radeon Pro V520", 1, 8192)
	radeon.GpuInfo.Gpus[0].Manufacturer = aws.String("AMD")
	arm := awsTypeInfo("g5g.xlarge", 4, 8, "T4g", 1, 16384)
	arm.ProcessorInfo.SupportedArchitectures = []ec2types.ArchitectureType{ec2types.ArchitectureTypeArm64}
	onDemandOnly := awsTypeInfo("p2.xlarge", 4, 61, "K80", 1, 12288)
	onDemandOnly.SupportedUsageClasses = []ec2types.UsageClassType{ec2types.UsageClassTypeOnDemand}
	plain := awsTypeInfo("m5.large", 2, 8, "", 0, 0)
	plain.GpuInfo = nil
	sold := []ec2types.InstanceTypeInfo{
		awsTypeInfo("g4dn.xlarge", 4, 16, "T4", 1, 16384),
		awsTypeInfo("g5.xlarge", 4, 16, "A10G", 1, 24576),
		awsTypeInfo("g6e.xlarge", 4, 32, "L40S", 1, 46068),
		awsTypeInfo("p4d.24xlarge", 96, 1152, "A100", 8, 40960),
		awsTypeInfo("p4de.24xlarge", 96, 1152, "A100", 8, 81920),
		awsTypeInfo("p5.48xlarge", 192, 2048, "H100", 8, 81920),
		awsTypeInfo("p3.2xlarge", 8, 61, "V100", 1, 16384),
		awsTypeInfo("p3dn.24xlarge", 96, 768, "V100", 8, 32768),
		awsTypeInfo("g7e.2xlarge", 8, 64, "RTX PRO Server 6000", 1, 98304),
		share, radeon, arm, onDemandOnly, plain,
	}

	image := func(id, name, created string, architecture ec2types.ArchitectureValues) ec2types.Image {
		return ec2types.Image{
			ImageId: aws.String(id), Name: aws.String(name), CreationDate: aws.String(created), Architecture: architecture,
			State: ec2types.ImageStateAvailable, OwnerId: aws.String("898082745236"), ImageOwnerAlias: aws.String("amazon"), RootDeviceName: aws.String("/dev/sda1"),
		}
	}
	for region, zones := range map[string][]string{"us-east-1": {"a", "b", "c"}, "us-west-2": {"a"}} {
		ec2 := &awsEC2{cloud: cloud, region: region, types: sold, byToken: map[string]string{}, calls: map[string]int{}}
		for _, zone := range zones {
			// us-east-1c has a subnet, and no default one.
			ec2.subnets = append(ec2.subnets, ec2types.Subnet{
				SubnetId:         aws.String("subnet-" + region + zone),
				AvailabilityZone: aws.String(region + zone),
				VpcId:            aws.String("vpc-" + region),
				DefaultForAz:     aws.Bool(region+zone != "us-east-1c"),
			})
		}
		ec2.images = []ec2types.Image{
			image(awsTestOldImage, "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 22.04) 20260901", "2026-09-01T10:00:00.000Z", ec2types.ArchitectureValuesX8664),
			image(awsTestImage, "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 22.04) 20261101", "2026-11-01T10:00:00.000Z", ec2types.ArchitectureValuesX8664),
			image("ami-0beam000000000arm", "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 22.04) 20261105", "2026-11-05T10:00:00.000Z", ec2types.ArchitectureValuesArm64),
			image("ami-0beam00000ubuntu24", "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 24.04) 20261110", "2026-11-10T10:00:00.000Z", ec2types.ArchitectureValuesX8664),
		}
		// Anyone may publish an image under Amazon's name for one.
		lookalike := image("ami-0beam0000someones", "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 22.04) 20261112", "2026-11-12T10:00:00.000Z", ec2types.ArchitectureValuesX8664)
		lookalike.OwnerId, lookalike.ImageOwnerAlias = aws.String("123456789012"), nil
		ec2.images = append(ec2.images, lookalike)
		cloud.regions[region] = ec2
	}

	price := func(region, zone, instanceType, dollars string, age time.Duration) {
		cloud.regions[region].prices = append(cloud.regions[region].prices, ec2types.SpotPrice{
			AvailabilityZone: aws.String(region + zone), InstanceType: ec2types.InstanceType(instanceType),
			ProductDescription: ec2types.RIProductDescription("Linux/UNIX"), SpotPrice: aws.String(dollars), Timestamp: aws.Time(cloud.now.Add(-age)),
		})
	}
	price("us-east-1", "a", "g5.xlarge", "0.402100", 3*time.Hour)
	// The price a zone had before is not its price.
	price("us-east-1", "b", "g5.xlarge", "0.450000", 9*time.Hour)
	price("us-east-1", "b", "g5.xlarge", "0.389900", 2*time.Hour)
	// A zone the account has no subnet to use in.
	price("us-east-1", "c", "g5.xlarge", "0.350000", time.Hour)
	price("us-east-1", "b", "g4dn.xlarge", "0.157800", 5*time.Hour)
	price("us-east-1", "a", "g6e.xlarge", "0.930400", 4*time.Hour)
	price("us-east-1", "a", "p4d.24xlarge", "9.661700", time.Hour)
	price("us-east-1", "b", "p4de.24xlarge", "12.404000", time.Hour)
	price("us-east-1", "a", "p3.2xlarge", "0.918000", time.Hour)
	price("us-east-1", "a", "p3dn.24xlarge", "9.364800", time.Hour)
	price("us-east-1", "b", "g7e.2xlarge", "1.336000", time.Hour)
	// The types this client does not buy have Spot prices too.
	price("us-east-1", "a", "g6f.large", "0.080000", time.Hour)
	price("us-east-1", "a", "g4ad.xlarge", "0.120000", time.Hour)
	price("us-east-1", "a", "g5g.xlarge", "0.140000", time.Hour)
	price("us-east-1", "a", "m5.large", "0.035000", time.Hour)
	price("us-west-2", "a", "g5.xlarge", "0.512300", time.Hour)
	// The same machine with Windows on it costs more, whenever its price
	// was set.
	windows := cloud.regions["us-west-2"].prices[0]
	windows.ProductDescription, windows.SpotPrice, windows.Timestamp = "Windows", aws.String("0.696300"), aws.Time(cloud.now.Add(-time.Minute))
	cloud.regions["us-west-2"].prices = append(cloud.regions["us-west-2"].prices, windows)
	return cloud
}

// client returns a client of the stand-in's EC2, configured as a deployment
// is unless the test changes it.
func (cloud *awsCloud) client(owner string, configure ...func(*types.AWSProviderConfig)) *AWS {
	config := types.AWSProviderConfig{Regions: []types.AWSProviderRegionConfig{{Name: "us-east-1"}, {Name: "us-west-2"}}}
	for _, change := range configure {
		change(&config)
	}
	client, err := NewAWS(config, owner)
	require.NoError(cloud.t, err)
	// The stand-in's clock, and its EC2 in place of the SDK's.
	client.now, client.ec2 = cloud.clock, cloud.ec2
	return client
}

// ec2 returns the stand-in's EC2 of a region.
func (cloud *awsCloud) ec2(region string) (ec2API, error) {
	ec2, ok := cloud.regions[region]
	if !ok {
		return nil, fmt.Errorf("no such region: %s", region)
	}
	return ec2, nil
}

func (cloud *awsCloud) clock() time.Time {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	return cloud.now
}

func (cloud *awsCloud) after(d time.Duration) {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	cloud.now = cloud.now.Add(d)
}

// called counts the calls of one kind to every region.
func (cloud *awsCloud) called(call string) int {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	total := 0
	for _, ec2 := range cloud.regions {
		total += ec2.calls[call]
	}
	return total
}

// lastRun returns the last create any region received.
func (cloud *awsCloud) lastRun(region string) *ec2.RunInstancesInput {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	runs := cloud.regions[region].runs
	require.NotEmpty(cloud.t, runs)
	return runs[len(runs)-1]
}

// find returns the instance with an ID of this client's, "region/i-...".
func (cloud *awsCloud) find(id string) (*awsEC2, int) {
	region, instance, _ := strings.Cut(id, "/")
	ec2 := cloud.regions[region]
	if ec2 == nil {
		return nil, -1
	}
	return ec2, slices.IndexFunc(ec2.instances, func(i ec2types.Instance) bool { return aws.ToString(i.InstanceId) == instance })
}

// state makes EC2 report an instance in the state, for the reason if any.
func (cloud *awsCloud) state(id, state string, reason *ec2types.StateReason) {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	ec2, at := cloud.find(id)
	require.GreaterOrEqual(cloud.t, at, 0, "no instance %s", id)
	ec2.instances[at].State = &ec2types.InstanceState{Name: ec2types.InstanceStateName(state)}
	ec2.instances[at].StateReason = reason
}

func (cloud *awsCloud) lose(id string) {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	if ec2, at := cloud.find(id); at >= 0 {
		ec2.instances = slices.Delete(ec2.instances, at, at+1)
	}
}

func (cloud *awsCloud) refuse(with error) {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	for _, ec2 := range cloud.regions {
		ec2.refusal = with
	}
}

func awsError(code, message string) error {
	return &smithy.GenericAPIError{Code: code, Message: message}
}

// awsFilters reports whether a resource passes EC2 filters: for each filter,
// one of its values has to fit one of the resource's values under that
// name, where * stands for anything.
func awsFilters(filters []ec2types.Filter, values func(name string) []string) bool {
	for _, filter := range filters {
		fits := slices.ContainsFunc(values(aws.ToString(filter.Name)), func(value string) bool {
			return slices.ContainsFunc(filter.Values, func(pattern string) bool {
				ok, _ := path.Match(pattern, value)
				return ok
			})
		})
		if !fits {
			return false
		}
	}
	return true
}

// awsPage cuts a list into EC2's pages.
func awsPage[T any](ec2 *awsEC2, items []T, token *string) ([]T, *string) {
	if ec2.repeatsToken {
		// A client that reads on would never stop, and hang its test.
		if ec2.repeated++; ec2.repeated > 100 {
			ec2.cloud.t.Errorf("the client read %d pages of lists that repeat their token", ec2.repeated)
			return items, nil
		}
		return items, aws.String("the-same-again")
	}
	start, _ := strconv.Atoi(aws.ToString(token))
	end := len(items)
	if ec2.pageOf > 0 {
		end = min(start+ec2.pageOf, len(items))
	}
	if end < len(items) {
		return items[start:end], aws.String(strconv.Itoa(end))
	}
	return items[min(start, end):end], nil
}

func (e *awsEC2) begin(call string) func() {
	e.cloud.mu.Lock()
	e.calls[call]++
	return e.cloud.mu.Unlock
}

func (e *awsEC2) DescribeInstanceTypes(ctx context.Context, in *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	defer e.begin("DescribeInstanceTypes")()
	var matching []ec2types.InstanceTypeInfo
	for _, info := range e.types {
		fits := awsFilters(in.Filters, func(name string) []string {
			switch name {
			case "processor-info.supported-architecture":
				architectures := []string{}
				for _, architecture := range info.ProcessorInfo.SupportedArchitectures {
					architectures = append(architectures, string(architecture))
				}
				return architectures
			}
			e.cloud.t.Errorf("the stand-in does not know the instance type filter %q", name)
			return nil
		})
		if fits {
			matching = append(matching, info)
		}
	}
	page, next := awsPage(e, matching, in.NextToken)
	return &ec2.DescribeInstanceTypesOutput{InstanceTypes: page, NextToken: next}, nil
}

// DescribeSpotPriceHistory answers as EC2 does when given a start time: the
// price in force then for each place, and the changes since.
func (e *awsEC2) DescribeSpotPriceHistory(ctx context.Context, in *ec2.DescribeSpotPriceHistoryInput, _ ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error) {
	defer e.begin("DescribeSpotPriceHistory")()
	if e.priceRefusal != nil {
		return nil, e.priceRefusal
	}
	if in.StartTime == nil {
		return nil, awsError("InvalidParameterValue", "the stand-in answers only from a start time")
	}
	type place struct {
		zone, product string
		instanceType  ec2types.InstanceType
	}
	inForce := map[place]ec2types.SpotPrice{}
	var matching []ec2types.SpotPrice
	for _, price := range e.prices {
		if len(in.InstanceTypes) > 0 && !slices.Contains(in.InstanceTypes, price.InstanceType) {
			continue
		}
		if len(in.ProductDescriptions) > 0 && !slices.Contains(in.ProductDescriptions, string(price.ProductDescription)) {
			continue
		}
		if price.Timestamp.After(*in.StartTime) {
			matching = append(matching, price)
			continue
		}
		at := place{aws.ToString(price.AvailabilityZone), string(price.ProductDescription), price.InstanceType}
		if known, ok := inForce[at]; !ok || price.Timestamp.After(*known.Timestamp) {
			inForce[at] = price
		}
	}
	for _, price := range inForce {
		matching = append(matching, price)
	}
	slices.SortFunc(matching, func(a, b ec2types.SpotPrice) int {
		return strings.Compare(aws.ToString(a.AvailabilityZone)+string(a.InstanceType), aws.ToString(b.AvailabilityZone)+string(b.InstanceType))
	})
	page, next := awsPage(e, matching, in.NextToken)
	// EC2 ends this list with an empty token, where its others end with none.
	if next == nil {
		next = aws.String("")
	}
	return &ec2.DescribeSpotPriceHistoryOutput{SpotPriceHistory: page, NextToken: next}, nil
}

func (e *awsEC2) DescribeSubnets(ctx context.Context, in *ec2.DescribeSubnetsInput, _ ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	defer e.begin("DescribeSubnets")()
	for _, id := range in.SubnetIds {
		if !slices.ContainsFunc(e.subnets, func(s ec2types.Subnet) bool { return aws.ToString(s.SubnetId) == id }) {
			return nil, awsError("InvalidSubnetID.NotFound", "The subnet ID '"+id+"' does not exist")
		}
	}
	var matching []ec2types.Subnet
	for _, subnet := range e.subnets {
		named := len(in.SubnetIds) == 0 || slices.Contains(in.SubnetIds, aws.ToString(subnet.SubnetId))
		fits := awsFilters(in.Filters, func(name string) []string {
			if name == "default-for-az" {
				return []string{strconv.FormatBool(aws.ToBool(subnet.DefaultForAz))}
			}
			e.cloud.t.Errorf("the stand-in does not know the subnet filter %q", name)
			return nil
		})
		if named && fits {
			matching = append(matching, subnet)
		}
	}
	page, next := awsPage(e, matching, in.NextToken)
	return &ec2.DescribeSubnetsOutput{Subnets: page, NextToken: next}, nil
}

func (e *awsEC2) DescribeImages(ctx context.Context, in *ec2.DescribeImagesInput, _ ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error) {
	defer e.begin("DescribeImages")()
	for _, id := range in.ImageIds {
		if !slices.ContainsFunc(e.images, func(i ec2types.Image) bool { return aws.ToString(i.ImageId) == id }) {
			return nil, awsError("InvalidAMIID.NotFound", "The image id '["+id+"]' does not exist")
		}
	}
	var matching []ec2types.Image
	for _, image := range e.images {
		named := len(in.ImageIds) == 0 || slices.Contains(in.ImageIds, aws.ToString(image.ImageId))
		owned := len(in.Owners) == 0 || slices.Contains(in.Owners, aws.ToString(image.ImageOwnerAlias)) || slices.Contains(in.Owners, aws.ToString(image.OwnerId))
		fits := awsFilters(in.Filters, func(name string) []string {
			switch name {
			case "name":
				return []string{aws.ToString(image.Name)}
			case "state":
				return []string{string(image.State)}
			case "architecture":
				return []string{string(image.Architecture)}
			}
			e.cloud.t.Errorf("the stand-in does not know the image filter %q", name)
			return nil
		})
		if named && owned && fits {
			matching = append(matching, image)
		}
	}
	return &ec2.DescribeImagesOutput{Images: matching}, nil
}

func (e *awsEC2) RunInstances(ctx context.Context, in *ec2.RunInstancesInput, _ ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error) {
	defer e.begin("RunInstances")()
	if e.refusal != nil {
		return nil, e.refusal
	}
	if why := e.wrong(in); why != nil {
		return nil, why
	}
	// "If you repeat the request with the same client token, the same
	// response is returned."
	if id, ran := e.byToken[aws.ToString(in.ClientToken)]; ran && in.ClientToken != nil {
		at := slices.IndexFunc(e.instances, func(i ec2types.Instance) bool { return aws.ToString(i.InstanceId) == id })
		return &ec2.RunInstancesOutput{Instances: []ec2types.Instance{e.instances[at]}}, nil
	}
	e.runs = append(e.runs, in)

	e.nextID++
	instance := ec2types.Instance{
		InstanceId:   aws.String(fmt.Sprintf("i-0%s%014d", e.region[len(e.region)-1:], e.nextID)),
		InstanceType: in.InstanceType,
		ImageId:      in.ImageId,
		SubnetId:     in.NetworkInterfaces[0].SubnetId,
		State:        &ec2types.InstanceState{Name: ec2types.InstanceStateNamePending},
	}
	if in.InstanceMarketOptions != nil && in.InstanceMarketOptions.MarketType == ec2types.MarketTypeSpot {
		instance.InstanceLifecycle = ec2types.InstanceLifecycleTypeSpot
	}
	for _, specification := range in.TagSpecifications {
		if specification.ResourceType == ec2types.ResourceTypeInstance {
			instance.Tags = slices.Clone(specification.Tags)
		}
	}
	e.instances = append(e.instances, instance)
	if in.ClientToken != nil {
		e.byToken[*in.ClientToken] = *instance.InstanceId
	}
	return &ec2.RunInstancesOutput{Instances: []ec2types.Instance{instance}}, nil
}

// wrong says what about a create EC2 would turn down.
func (e *awsEC2) wrong(in *ec2.RunInstancesInput) error {
	invalid := func(why string) error { return awsError("InvalidParameterCombination", why) }
	switch {
	case aws.ToInt32(in.MinCount) != 1 || aws.ToInt32(in.MaxCount) != 1:
		return invalid("the stand-in runs one instance at a time")
	case !slices.ContainsFunc(e.images, func(i ec2types.Image) bool { return aws.ToString(i.ImageId) == aws.ToString(in.ImageId) }):
		return awsError("InvalidAMIID.NotFound", "The image id '["+aws.ToString(in.ImageId)+"]' does not exist")
	case !slices.ContainsFunc(e.types, func(t ec2types.InstanceTypeInfo) bool { return t.InstanceType == in.InstanceType }):
		return awsError("InvalidParameterValue", "Invalid value '"+string(in.InstanceType)+"' for InstanceType")
	case len(in.NetworkInterfaces) != 1 || in.SubnetId != nil || len(in.SecurityGroupIds) > 0:
		return invalid("Network interfaces and an instance-level subnet ID or security groups may not be specified on the same request")
	case len(aws.ToString(in.ClientToken)) > 64:
		return awsError("InvalidParameterValue", "the client token is longer than 64 characters")
	}
	subnet := aws.ToString(in.NetworkInterfaces[0].SubnetId)
	if !slices.ContainsFunc(e.subnets, func(s ec2types.Subnet) bool { return aws.ToString(s.SubnetId) == subnet }) {
		return awsError("InvalidSubnetID.NotFound", "The subnet ID '"+subnet+"' does not exist")
	}
	if in.UserData != nil {
		if _, err := base64.StdEncoding.DecodeString(*in.UserData); err != nil {
			return awsError("InvalidUserData.Malformed", "Invalid BASE64 encoding of user data")
		}
	}
	return nil
}

func (e *awsEC2) DescribeInstances(ctx context.Context, in *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	defer e.begin("DescribeInstances")()
	if e.describeRefusal != nil {
		return nil, e.describeRefusal
	}
	for _, id := range in.InstanceIds {
		if !strings.HasPrefix(id, "i-") {
			return nil, awsError("InvalidInstanceID.Malformed", "Invalid id: \""+id+"\"")
		}
		if !slices.ContainsFunc(e.instances, func(i ec2types.Instance) bool { return aws.ToString(i.InstanceId) == id }) {
			return nil, awsError("InvalidInstanceID.NotFound", "The instance ID '"+id+"' does not exist")
		}
	}
	var matching []ec2types.Reservation
	for _, instance := range e.instances {
		named := len(in.InstanceIds) == 0 || slices.Contains(in.InstanceIds, aws.ToString(instance.InstanceId))
		fits := awsFilters(in.Filters, func(name string) []string {
			if key, ok := strings.CutPrefix(name, "tag:"); ok {
				values := []string{}
				for _, tag := range instance.Tags {
					if aws.ToString(tag.Key) == key {
						values = append(values, aws.ToString(tag.Value))
					}
				}
				return values
			}
			e.cloud.t.Errorf("the stand-in does not know the instance filter %q", name)
			return nil
		})
		if named && (fits || e.listsEverything) {
			matching = append(matching, ec2types.Reservation{Instances: []ec2types.Instance{instance}})
		}
	}
	page, next := awsPage(e, matching, in.NextToken)
	return &ec2.DescribeInstancesOutput{Reservations: page, NextToken: next}, nil
}

func (e *awsEC2) TerminateInstances(ctx context.Context, in *ec2.TerminateInstancesInput, _ ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error) {
	defer e.begin("TerminateInstances")()
	if e.terminateRefusal != nil {
		return nil, e.terminateRefusal
	}
	for _, id := range in.InstanceIds {
		at := slices.IndexFunc(e.instances, func(i ec2types.Instance) bool { return aws.ToString(i.InstanceId) == id })
		if at < 0 {
			return nil, awsError("InvalidInstanceID.NotFound", "The instance ID '"+id+"' does not exist")
		}
		// Terminating what is terminated already succeeds and changes
		// nothing.
		switch e.instances[at].State.Name {
		case ec2types.InstanceStateNameShuttingDown, ec2types.InstanceStateNameTerminated:
		default:
			e.instances[at].State = &ec2types.InstanceState{Name: ec2types.InstanceStateNameShuttingDown}
			e.instances[at].StateReason = &ec2types.StateReason{Code: aws.String("Client.UserInitiatedShutdown"), Message: aws.String("Client.UserInitiatedShutdown: User initiated shutdown")}
		}
	}
	return &ec2.TerminateInstancesOutput{}, nil
}

// How EC2 turns a create down, by its API reference.
var awsRefusals = map[error]error{
	ErrOutOfStock: awsError("InsufficientInstanceCapacity", "There is no Spot capacity available that matches your request."),
	ErrQuota:      awsError("MaxSpotInstanceCountExceeded", "Max spot instance count exceeded"),
}

// The reason EC2 gives for an instance it took back.
var awsTakenBack = &ec2types.StateReason{
	Code:    aws.String("Server.SpotInstanceTermination"),
	Message: aws.String("Server.SpotInstanceTermination: Spot instance termination"),
}

func newAWSSite(t *testing.T) site {
	cloud := newAWSCloud(t)
	return site{
		provider: cloud.client(testOwner),
		statuses: map[string]Status{
			"pending":       StatusPending,
			"running":       StatusActive,
			"shutting-down": StatusDeleted,
			"terminated":    StatusDeleted,
			"stopping":      StatusFailed,
			"stopped":       StatusFailed,
		},
		report: func(id, status string) { cloud.state(id, status, nil) },
		refuse: func(reason error) { cloud.refuse(awsRefusals[reason]) },
		lose:   cloud.lose,
		bought: func() (string, string) {
			run := cloud.lastRun("us-east-1")
			script, err := base64.StdEncoding.DecodeString(aws.ToString(run.UserData))
			require.NoError(t, err)
			return awsTagsOf(run)[awsNameTag], string(script)
		},
		plantForeign: func() string {
			offers, err := cloud.client(foreignOwner).Offers(context.Background(), OfferFilter{})
			require.NoError(t, err)
			instance, err := cloud.client(foreignOwner).Create(context.Background(), CreateRequest{Offer: offers[0], Name: "someone-elses"})
			require.NoError(t, err)
			return instance.ID
		},
		boughtSpot: func() bool {
			market := cloud.lastRun("us-east-1").InstanceMarketOptions
			return market != nil && market.MarketType == ec2types.MarketTypeSpot
		},
		takeBack: func(id string) { cloud.state(id, "terminated", awsTakenBack) },
	}
}

// awsTagsOf returns the tags a create put on its instance.
func awsTagsOf(run *ec2.RunInstancesInput) map[string]string {
	tags := map[string]string{}
	for _, specification := range run.TagSpecifications {
		if specification.ResourceType == ec2types.ResourceTypeInstance {
			for _, tag := range specification.Tags {
				tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
			}
		}
	}
	return tags
}

// What a machine's 200 GB disk costs an hour, at $0.08 a gigabyte a month.
const awsTestDisk = 21_917

func awsOffer(zone, instanceType, gpu string, gpus uint32, vcpus, memoryGiB, micros int64) Offer {
	return Offer{
		ID: zone + "/" + instanceType, Provider: "aws", Cloud: "aws", InstanceType: instanceType, Region: zone[:len(zone)-1],
		GPU: gpu, GPUCount: gpus, CPUMillicores: vcpus * 1000, MemoryMB: memoryGiB * 1024, StorageMB: 200 * 1024,
		HourlyCostMicros: micros + awsTestDisk, Spot: true,
	}
}

var (
	awsA10GInA  = awsOffer("us-east-1a", "g5.xlarge", "A10G", 1, 4, 16, 402_100)
	awsA10GInB  = awsOffer("us-east-1b", "g5.xlarge", "A10G", 1, 4, 16, 389_900)
	awsA10GWest = awsOffer("us-west-2a", "g5.xlarge", "A10G", 1, 4, 16, 512_300)
)

func TestAWSOffersEachGPUTypeInEachZoneThatHasASpotPrice(t *testing.T) {
	cloud := newAWSCloud(t)
	offers, err := cloud.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)

	// EC2 calls both A100s "A100" and both V100s "V100"; their memory tells
	// them apart. The H100 type has no Spot price anywhere, and us-east-1c
	// no subnet to use. Of the two zones that sell the A10G, the cheaper
	// comes first.
	require.Equal(t, []Offer{
		awsOffer("us-east-1a", "p4d.24xlarge", "A100-40", 8, 96, 1152, 9_661_700),
		awsOffer("us-east-1b", "p4de.24xlarge", "A100-80", 8, 96, 1152, 12_404_000),
		awsA10GInB,
		awsA10GInA,
		awsOffer("us-east-1a", "g6e.xlarge", "L40S", 1, 4, 32, 930_400),
		awsOffer("us-east-1b", "g7e.2xlarge", "RTXPro6000", 1, 8, 64, 1_336_000),
		awsOffer("us-east-1b", "g4dn.xlarge", "T4", 1, 4, 16, 157_800),
		awsOffer("us-east-1a", "p3.2xlarge", "V100", 1, 8, 61, 918_000),
		awsOffer("us-east-1a", "p3dn.24xlarge", "V100-32", 8, 96, 768, 9_364_800),
		awsA10GWest,
	}, offers)
}

// A price that changes as it is asked for comes back next to the one it
// replaced.
func TestAWSTakesTheLatestOfTwoPricesForAZone(t *testing.T) {
	cloud := newAWSCloud(t)
	changed := cloud.regions["us-west-2"].prices[0]
	changed.SpotPrice, changed.Timestamp = aws.String("0.530000"), aws.Time(cloud.now.Add(time.Second))
	cloud.regions["us-west-2"].prices = append([]ec2types.SpotPrice{changed}, cloud.regions["us-west-2"].prices...)

	offers, err := cloud.client(testOwner).Offers(context.Background(), OfferFilter{Regions: []string{"us-west-2"}})
	require.NoError(t, err)
	require.Equal(t, []Offer{awsOffer("us-west-2a", "g5.xlarge", "A10G", 1, 4, 16, 530_000)}, offers)
	require.Zero(t, cloud.regions["us-east-1"].calls["DescribeSpotPriceHistory"], "a region that was not asked for is not read")
}

func TestAWSReadsEveryPageOfAList(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	ctx := context.Background()
	for _, ec2 := range cloud.regions {
		ec2.pageOf = 1
	}

	offers, err := client.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.Len(t, offers, 10)

	for _, name := range []string{"beam-a10g-a1b2c3d4", "beam-a10g-e5f6a7b8", "beam-a10g-c9d0e1f2"} {
		_, err := client.Create(ctx, CreateRequest{Offer: awsA10GInA, Name: name})
		require.NoError(t, err)
	}
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 3)
}

func TestAWSReadsARegionRarelyAndItsPricesEveryTime(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	for range 3 {
		_, err := client.Offers(context.Background(), OfferFilter{})
		require.NoError(t, err)
	}
	for _, call := range []string{"DescribeInstanceTypes", "DescribeSubnets", "DescribeImages"} {
		require.Equal(t, 2, cloud.called(call), "%s, once for each region", call)
	}
	require.Equal(t, 6, cloud.called("DescribeSpotPriceHistory"))

	cloud.after(awsRegionFor)
	_, err := client.Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.Equal(t, 4, cloud.called("DescribeInstanceTypes"))
}

func TestAWSCreateSendsWhatEC2Needs(t *testing.T) {
	cloud := newAWSCloud(t)
	created, err := cloud.client(testOwner).Create(context.Background(), CreateRequest{
		Offer:         awsA10GInB,
		Name:          "beam-a10g-a1b2c3d4",
		StartupScript: "curl -fsSL https://gateway.example/join | sh",
		Tags:          map[string]string{"beam-machine": "a1b2c3d4"},
	})
	require.NoError(t, err)
	require.Equal(t, Instance{ID: "us-east-1/i-0100000000000001", Status: StatusPending, Tags: map[string]string{"beam-machine": "a1b2c3d4"}}, created)

	tags := []ec2types.Tag{
		{Key: aws.String("Name"), Value: aws.String("beam-a10g-a1b2c3d4")},
		{Key: aws.String("beam-machine"), Value: aws.String("a1b2c3d4")},
		{Key: aws.String(OwnerTag), Value: aws.String(testOwner)},
	}
	require.Equal(t, &ec2.RunInstancesInput{
		// The newest image of the name, for the machine's kind of CPU.
		ImageId:      aws.String(awsTestImage),
		InstanceType: "g5.xlarge",
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		ClientToken:  aws.String("beam-a10g-a1b2c3d4"),
		InstanceMarketOptions: &ec2types.InstanceMarketOptionsRequest{
			MarketType: ec2types.MarketTypeSpot,
			// No price limit was asked for.
			SpotOptions: &ec2types.SpotMarketOptions{
				SpotInstanceType:             ec2types.SpotInstanceTypeOneTime,
				InstanceInterruptionBehavior: ec2types.InstanceInterruptionBehaviorTerminate,
			},
		},
		NetworkInterfaces: []ec2types.InstanceNetworkInterfaceSpecification{{
			DeviceIndex: aws.Int32(0),
			// The default subnet of the offer's zone.
			SubnetId:                 aws.String("subnet-us-east-1b"),
			AssociatePublicIpAddress: aws.Bool(true),
		}},
		BlockDeviceMappings: []ec2types.BlockDeviceMapping{{
			DeviceName: aws.String("/dev/sda1"),
			Ebs:        &ec2types.EbsBlockDevice{VolumeSize: aws.Int32(200), VolumeType: ec2types.VolumeTypeGp3, DeleteOnTermination: aws.Bool(true)},
		}},
		InstanceInitiatedShutdownBehavior: ec2types.ShutdownBehaviorTerminate,
		MetadataOptions:                   &ec2types.InstanceMetadataOptionsRequest{HttpTokens: ec2types.HttpTokensStateRequired},
		TagSpecifications: []ec2types.TagSpecification{
			{ResourceType: ec2types.ResourceTypeInstance, Tags: tags},
			{ResourceType: ec2types.ResourceTypeVolume, Tags: tags},
		},
		UserData: aws.String(base64.StdEncoding.EncodeToString([]byte("#!/usr/bin/env bash\nset -euo pipefail\ncurl -fsSL https://gateway.example/join | sh\n"))),
	}, cloud.lastRun("us-east-1"))
}

func TestAWSPassesThePriceLimitLessTheDisk(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)

	// $0.50 an hour for the machine leaves $0.478083 for the instance.
	_, err := client.Create(context.Background(), CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4", MaxHourlyCostMicros: 500_000})
	require.NoError(t, err)
	require.Equal(t, "0.4780", aws.ToString(cloud.lastRun("us-east-1").InstanceMarketOptions.SpotOptions.MaxPrice))

	_, err = client.Create(context.Background(), CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-e5f6a7b8", MaxHourlyCostMicros: 12_345_678})
	require.NoError(t, err)
	require.Equal(t, "12.3237", aws.ToString(cloud.lastRun("us-east-1").InstanceMarketOptions.SpotOptions.MaxPrice))

	_, err = client.Create(context.Background(), CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-c9d0e1f2", MaxHourlyCostMicros: awsTestDisk})
	require.ErrorContains(t, err, "leaves nothing for an AWS instance")
	require.Len(t, cloud.regions["us-east-1"].runs, 2)
}

// The SDK sends a call again when it gets no answer. A create carries a
// token that EC2 runs one instance for, however often it arrives.
func TestAWSACreateSentTwiceBuysOneMachine(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	req := CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4"}

	first, err := client.Create(context.Background(), req)
	require.NoError(t, err)
	again, err := client.Create(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, first, again)
	require.Len(t, cloud.regions["us-east-1"].instances, 1)
	// The token is the machine's name, which is its alone.
	require.Equal(t, "beam-a10g-a1b2c3d4", aws.ToString(cloud.lastRun("us-east-1").ClientToken))
}

func TestAWSNamesWhyACreateWasTurnedDown(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	req := CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4"}
	for code, want := range map[string]error{
		"InsufficientInstanceCapacity": ErrOutOfStock,
		"SpotMaxPriceTooLow":           ErrOutOfStock,
		"MaxSpotInstanceCountExceeded": ErrQuota,
		"VcpuLimitExceeded":            ErrQuota,
		"InstanceLimitExceeded":        ErrQuota,
	} {
		cloud.refuse(awsError(code, "refused"))
		_, err := client.Create(context.Background(), req)
		require.ErrorIs(t, err, want, code)
	}

	// The configuration is wrong, which waiting does not put right.
	cloud.refuse(awsError("Unsupported", "The requested configuration is currently not supported."))
	_, err := client.Create(context.Background(), req)
	require.ErrorContains(t, err, "not supported")
	require.NotErrorIs(t, err, ErrOutOfStock)
	require.NotErrorIs(t, err, ErrQuota)
}

// EC2 has no API for on-demand prices, so this client has no offer for an
// on-demand instance, and does not buy one for the price of a Spot one.
func TestAWSBuysNothingButSpotInstances(t *testing.T) {
	cloud := newAWSCloud(t)
	onDemand := awsA10GInA
	onDemand.Spot = false
	_, err := cloud.client(testOwner).Create(context.Background(), CreateRequest{Offer: onDemand, Name: "beam-a10g-a1b2c3d4"})
	require.ErrorContains(t, err, "Spot")

	for name, offer := range map[string]Offer{
		"a zone without a subnet":  awsOffer("us-east-1c", "g5.xlarge", "A10G", 1, 4, 16, 350_000),
		"a region not configured":  awsOffer("eu-west-1a", "g5.xlarge", "A10G", 1, 4, 16, 350_000),
		"a zone of another region": {ID: "us-east-1a/g5.xlarge", Region: "eu-west-1", Spot: true},
		"no zone":                  {ID: "g5.xlarge", Region: "us-east-1", Spot: true},
	} {
		_, err := cloud.client(testOwner).Create(context.Background(), CreateRequest{Offer: offer, Name: "beam-a10g-a1b2c3d4"})
		require.Error(t, err, name)
	}
	require.Zero(t, cloud.called("RunInstances"))
}

func TestAWSUsesTheSubnetsGroupsImageAndKeyItIsGiven(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner, func(config *types.AWSProviderConfig) {
		config.Regions[0] = types.AWSProviderRegionConfig{
			Name:             "us-east-1",
			SubnetIDs:        []string{"subnet-us-east-1c"},
			SecurityGroupIDs: []string{"sg-0beamworkers"},
			ImageID:          awsTestOldImage,
		}
		config.KeyName = "ops"
		config.DiskGB = 500
		config.ImageName = "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 24.04) *"
	})

	offers, err := client.Offers(context.Background(), OfferFilter{Regions: []string{"us-east-1"}})
	require.NoError(t, err)
	// The one zone that has a named subnet, with the price of the disk.
	inC := awsOffer("us-east-1c", "g5.xlarge", "A10G", 1, 4, 16, 350_000)
	inC.StorageMB, inC.HourlyCostMicros = 500*1024, 350_000+54_794
	require.Equal(t, []Offer{inC}, offers)

	_, err = client.Create(context.Background(), CreateRequest{Offer: inC, Name: "beam-a10g-a1b2c3d4"})
	require.NoError(t, err)
	run := cloud.lastRun("us-east-1")
	require.Equal(t, awsTestOldImage, aws.ToString(run.ImageId))
	require.Equal(t, "ops", aws.ToString(run.KeyName))
	require.Equal(t, "subnet-us-east-1c", aws.ToString(run.NetworkInterfaces[0].SubnetId))
	require.Equal(t, []string{"sg-0beamworkers"}, run.NetworkInterfaces[0].Groups)
	require.Equal(t, int32(500), aws.ToInt32(run.BlockDeviceMappings[0].Ebs.VolumeSize))
	require.Nil(t, run.UserData, "no script was asked for")

	// The other region keeps its defaults, and boots the newest image of
	// the name that was given.
	_, err = client.Create(context.Background(), CreateRequest{Offer: awsA10GWest, Name: "beam-a10g-e5f6a7b8"})
	require.NoError(t, err)
	require.Empty(t, cloud.lastRun("us-west-2").NetworkInterfaces[0].Groups)
	require.Equal(t, "ami-0beam00000ubuntu24", aws.ToString(cloud.lastRun("us-west-2").ImageId))
}

// A region that cannot be bought in is left out of the offers. Asked for it
// alone, the client says what it lacks.
func TestAWSSaysWhatARegionLacks(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(*awsCloud, *types.AWSProviderConfig)
		region string
		want   string
		// unreachable marks a region whose machines cannot be read either.
		unreachable bool
	}{
		"no default subnets": {
			change: func(cloud *awsCloud, _ *types.AWSProviderConfig) { cloud.regions["us-west-2"].subnets = nil },
			region: "us-west-2",
			want:   "AWS region us-west-2: it has no default subnets: name the subnets to use as subnetIds under providers.aws.regions",
		},
		"a subnet that is not there": {
			change: func(_ *awsCloud, config *types.AWSProviderConfig) {
				config.Regions[1].SubnetIDs = []string{"subnet-nosuch"}
			},
			region: "us-west-2",
			want:   "AWS region us-west-2: its subnets:",
		},
		"no image of the name": {
			change: func(_ *awsCloud, config *types.AWSProviderConfig) { config.ImageName = "No Such Image *" },
			region: "us-east-1",
			want:   `AWS region us-east-1: it has no image "No Such Image *"`,
		},
		"an image that is not there": {
			change: func(_ *awsCloud, config *types.AWSProviderConfig) { config.Regions[0].ImageID = "ami-nosuch" },
			region: "us-east-1",
			want:   "AWS region us-east-1: its image:",
		},
		"a region EC2 does not have": {
			change: func(_ *awsCloud, config *types.AWSProviderConfig) {
				config.Regions = append(config.Regions, types.AWSProviderRegionConfig{Name: "mars-north-1"})
			},
			region:      "mars-north-1",
			want:        "AWS region mars-north-1: no such region",
			unreachable: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cloud := newAWSCloud(t)
			client := cloud.client(testOwner, func(config *types.AWSProviderConfig) { tc.change(cloud, config) })
			_, err := client.Offers(context.Background(), OfferFilter{Regions: []string{tc.region}})
			require.ErrorContains(t, err, tc.want)
			_, err = client.List(context.Background())
			if tc.unreachable {
				require.ErrorContains(t, err, tc.want, "what cannot be listed must not read as nothing bought")
			} else {
				require.NoError(t, err, "what was bought there can still be listed")
			}
		})
	}
}

// AWS says why it ended an instance. The reasons for a Spot Instance it
// took back start with Server.Spot.
func TestAWSReadsASpotInstanceItTookBackAsInterrupted(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4"})
	require.NoError(t, err)
	read := func(state string, reason *ec2types.StateReason) Instance {
		t.Helper()
		cloud.state(created.ID, state, reason)
		found, err := client.Get(ctx, created.ID)
		require.NoError(t, err)
		return found
	}

	// AWS is taking it back: two minutes after the notice it shuts down.
	require.Equal(t, Instance{ID: created.ID, Status: StatusInterrupted, StatusDetail: "shutting-down: Server.SpotInstanceTermination: Spot instance termination"},
		read("shutting-down", awsTakenBack))
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)

	require.Equal(t, StatusInterrupted, read("terminated", awsTakenBack).Status)
	stoppedByAWS := &ec2types.StateReason{Code: aws.String("Server.SpotInstanceShutdown"), Message: aws.String("Server.SpotInstanceShutdown: Spot instance shutdown")}
	require.Equal(t, StatusInterrupted, read("stopped", stoppedByAWS).Status)

	// Ended for another reason, it was not taken back.
	byUser := &ec2types.StateReason{Code: aws.String("Client.UserInitiatedShutdown"), Message: aws.String("Client.UserInitiatedShutdown: User initiated shutdown")}
	require.Equal(t, Instance{ID: created.ID, Status: StatusDeleted}, read("terminated", byUser))
	failed := &ec2types.StateReason{Code: aws.String("Server.InternalError"), Message: aws.String("Server.InternalError: Internal error on launch")}
	require.Equal(t, Instance{ID: created.ID, Status: StatusFailed, StatusDetail: "stopped: Server.InternalError: Internal error on launch"}, read("stopped", failed))
	require.Equal(t, Instance{ID: created.ID, Status: StatusUnknown, StatusDetail: "rebooting"}, read("rebooting", nil))
}

// EC2 shows a terminated instance for about an hour. Nothing is left of it
// to delete or to pay for.
func TestAWSListLeavesOutWhatIsTerminated(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	ctx := context.Background()
	east, err := client.Create(ctx, CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4", Tags: map[string]string{"beam-machine": "a1b2c3d4"}})
	require.NoError(t, err)
	west, err := client.Create(ctx, CreateRequest{Offer: awsA10GWest, Name: "beam-a10g-e5f6a7b8", Tags: map[string]string{"beam-machine": "e5f6a7b8"}})
	require.NoError(t, err)

	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []Instance{
		{ID: east.ID, Status: StatusPending, Tags: map[string]string{"beam-machine": "a1b2c3d4"}},
		{ID: west.ID, Status: StatusPending, Tags: map[string]string{"beam-machine": "e5f6a7b8"}},
	}, listed)

	cloud.state(east.ID, "terminated", awsTakenBack)
	listed, err = client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, west.ID, listed[0].ID)
}

func TestAWSReportsWhereAMachineCanBeReached(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	created, err := client.Create(context.Background(), CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4"})
	require.NoError(t, err)

	found, err := client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Empty(t, found.PublicIP, "a machine that is not up has no address")

	cloud.state(created.ID, "running", nil)
	cloud.regions["us-east-1"].instances[0].PublicIpAddress = aws.String("54.210.10.20")
	found, err = client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusActive, PublicIP: "54.210.10.20", SSHHost: "54.210.10.20", SSHPort: 22}, found)
}

// An instance's ID here names its region, because EC2's is the region's
// own. One that is not a region and an instance is nobody's.
func TestAWSFindsAnInstanceByTheRegionInItsID(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: awsA10GWest, Name: "beam-a10g-a1b2c3d4"})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(created.ID, "us-west-2/i-"), created.ID)

	_, instance, _ := strings.Cut(created.ID, "/")
	for _, id := range []string{"us-east-1/" + instance, instance, "us-west-2/", "/" + instance, "us-west-2/not-an-id"} {
		_, err := client.Get(ctx, id)
		require.ErrorIs(t, err, ErrNotFound, id)
		require.NoError(t, client.Delete(ctx, id), id)
	}
	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusPending, found.Status, "none of those deleted it")

	// A region that was taken out of the config still has the machines
	// bought in it. They are not gone, and saying so would drop the record
	// of a machine that goes on running.
	eastOnly := cloud.client(testOwner, func(config *types.AWSProviderConfig) {
		config.Regions = []types.AWSProviderRegionConfig{{Name: "us-east-1"}}
	})
	_, err = eastOnly.Get(ctx, created.ID)
	require.ErrorContains(t, err, `providers.aws.regions does not name "us-west-2"`)
	require.NotErrorIs(t, err, ErrNotFound)
	err = eastOnly.Delete(ctx, created.ID)
	require.ErrorContains(t, err, `providers.aws.regions does not name "us-west-2"`)
	found, err = client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusPending, found.Status, "it was not deleted")

	// A delete that EC2 refuses for another reason is an error.
	cloud.regions["us-west-2"].terminateRefusal = awsError("UnauthorizedOperation", "You are not authorized to perform this operation.")
	require.ErrorContains(t, client.Delete(ctx, created.ID), "not authorized")
	cloud.regions["us-west-2"].terminateRefusal = nil
	require.NoError(t, client.Delete(ctx, created.ID))
	found, err = client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusDeleted, found.Status)
}

// Every other test of this client talks to a stand-in for EC2. This one
// looks at the client the AWS SDK is asked for: it is the region's, and it
// signs with the IAM user's key that the config holds.
func TestAWSTalksToARegionsEC2WithTheKeyItIsGiven(t *testing.T) {
	client, err := NewAWS(types.AWSProviderConfig{
		AccessKeyID:     "AKIABEAMTEST",
		SecretAccessKey: "secret",
		Regions:         []types.AWSProviderRegionConfig{{Name: "us-east-1"}, {Name: "us-west-2"}},
	}, testOwner)
	require.NoError(t, err)

	sdk, err := client.connect(t.Context(), "us-west-2")
	require.NoError(t, err)
	options := sdk.(*ec2.Client).Options()
	require.Equal(t, "us-west-2", options.Region)
	key, err := options.Credentials.Retrieve(t.Context())
	require.NoError(t, err)
	require.Equal(t, "AKIABEAMTEST", key.AccessKeyID)
	require.Equal(t, "secret", key.SecretAccessKey)
}
