package providers

import (
	"cmp"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	awsName = "aws"

	// Amazon's image with the NVIDIA driver and Docker. The newest whose
	// name fits is booted.
	awsImageName  = "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 22.04) *"
	awsImageOwner = "amazon"
	// EC2 has no API for what a disk costs. This is the price of a gp3
	// volume in us-east-1, $0.08 for a gigabyte for a month; other regions
	// charge up to a fifth more.
	awsDiskMicrosPerGBMonth = 80_000

	// The price of Linux without a licence on top.
	awsLinux = "Linux/UNIX"
	// The tag EC2's console shows as an instance's name.
	awsNameTag = "Name"

	// A region's instance types, subnets and image change rarely.
	awsRegionFor = 6 * time.Hour
)

// AWS buys EC2 Spot Instances, which AWS takes back with two minutes'
// notice.
//
// It sells nothing else here: EC2 has an API for Spot prices and none for
// on-demand ones. EC2 does not say whether it has Spot capacity either, so
// every GPU instance type is offered wherever it has a Spot price, and a
// create finds out.
type AWS struct {
	// AWS ends an instance only when asked, or when it takes a Spot Instance
	// back.
	noDeletionTimer

	owner  string
	config types.AWSProviderConfig
	// now is the clock, and ec2 makes the EC2 client of a region in place
	// of the AWS SDK. A test sets them.
	now func() time.Time
	ec2 func(region string) (ec2API, error)
	// regions holds the configured regions by name.
	regions map[string]*awsRegion
}

// ec2API is the part of the EC2 client this provider calls.
type ec2API interface {
	DescribeInstanceTypes(ctx context.Context, in *ec2.DescribeInstanceTypesInput, opts ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error)
	DescribeSpotPriceHistory(ctx context.Context, in *ec2.DescribeSpotPriceHistoryInput, opts ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error)
	DescribeSubnets(ctx context.Context, in *ec2.DescribeSubnetsInput, opts ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error)
	DescribeImages(ctx context.Context, in *ec2.DescribeImagesInput, opts ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error)
	RunInstances(ctx context.Context, in *ec2.RunInstancesInput, opts ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error)
	DescribeInstances(ctx context.Context, in *ec2.DescribeInstancesInput, opts ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
	TerminateInstances(ctx context.Context, in *ec2.TerminateInstancesInput, opts ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error)
}

// NewAWS makes a client that buys as owner: the name this control plane puts
// on the instances it buys. It fails when there is no region to buy in, or
// half of an IAM user's key.
func NewAWS(config types.AWSProviderConfig, owner string) (*AWS, error) {
	if len(config.Regions) == 0 {
		return nil, errors.New("providers.aws.regions names no region to buy in")
	}
	if config.AccessKeyID != "" && config.SecretAccessKey == "" {
		return nil, errors.New("providers.aws.secretAccessKey is not set, and accessKeyId is")
	}
	if config.SecretAccessKey != "" && config.AccessKeyID == "" {
		return nil, errors.New("providers.aws.accessKeyId is not set, and secretAccessKey is")
	}
	if config.ImageName == "" {
		config.ImageName = awsImageName
	}
	if config.DiskGB <= 0 {
		config.DiskGB = defaultDiskGB
	}
	c := &AWS{owner: owner, config: config, now: time.Now, regions: map[string]*awsRegion{}}
	for _, region := range config.Regions {
		c.regions[region.Name] = &awsRegion{config: region}
	}
	return c, nil
}

func (c *AWS) Name() string { return awsName }

// awsRegion is a configured region with what EC2 says about it.
type awsRegion struct {
	config types.AWSProviderRegionConfig
	// client talks to the region's EC2. It is kept apart from what a
	// machine is bought with, which reading and deleting one do not need.
	client cached[ec2API]
	known  cached[awsRegionFacts]
}

// awsRegionFacts are what a region's offers and creates are made from.
type awsRegionFacts struct {
	// subnets holds the subnet machines are put in, by availability zone.
	subnets map[string]string
	// groups are a machine's security groups; none means its VPC's
	// default one.
	groups []string
	image  string
	// rootDevice is the device the image's disk is attached as.
	rootDevice string
	// types are the instance types with NVIDIA GPUs.
	types map[string]awsType
}

type awsType struct {
	gpu       string
	gpus      uint32
	vcpus     int64
	memoryMiB int64
}

// within gives one call to EC2 the time every provider call gets.
func within[T any](ctx context.Context, call func(ctx context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return call(ctx)
}

// The SDK reads a list page by page, by the token each page ends with. A
// list that keeps ending with the same token has no next page: these end it
// there, where the SDK would read it for ever. Each list has options of its
// own type.
func endSubnets(o *ec2.DescribeSubnetsPaginatorOptions)             { o.StopOnDuplicateToken = true }
func endInstanceTypes(o *ec2.DescribeInstanceTypesPaginatorOptions) { o.StopOnDuplicateToken = true }
func endSpotPrices(o *ec2.DescribeSpotPriceHistoryPaginatorOptions) { o.StopOnDuplicateToken = true }
func endInstances(o *ec2.DescribeInstancesPaginatorOptions)         { o.StopOnDuplicateToken = true }

// connect makes the EC2 client of a region.
func (c *AWS) connect(ctx context.Context, region string) (ec2API, error) {
	if c.ec2 != nil {
		return c.ec2(region)
	}
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if c.config.AccessKeyID != "" {
		static := credentials.NewStaticCredentialsProvider(c.config.AccessKeyID, c.config.SecretAccessKey, "")
		options = append(options, awsconfig.WithCredentialsProvider(static))
	}
	loaded, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("AWS credentials for %s: %w", region, err)
	}
	return ec2.NewFromConfig(loaded), nil
}

// configured finds a region by its name.
func (c *AWS) configured(name string) (*awsRegion, error) {
	region, ok := c.regions[name]
	if !ok {
		return nil, fmt.Errorf("providers.aws.regions does not name %q", name)
	}
	return region, nil
}

// client returns the EC2 client of a configured region.
func (c *AWS) client(ctx context.Context, name string) (ec2API, error) {
	region, err := c.configured(name)
	if err != nil {
		return nil, err
	}
	return region.client.get(c.now(), awsRegionFor, func() (ec2API, error) {
		client, err := c.connect(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("AWS region %s: %w", name, err)
		}
		return client, nil
	})
}

// region returns what a configured region's offers and creates are made
// from.
func (c *AWS) region(ctx context.Context, name string) (awsRegionFacts, error) {
	region, err := c.configured(name)
	if err != nil {
		return awsRegionFacts{}, err
	}
	return region.known.get(c.now(), awsRegionFor, func() (awsRegionFacts, error) {
		client, err := c.client(ctx, name)
		if err != nil {
			return awsRegionFacts{}, err
		}
		facts, err := c.learn(ctx, client, region.config)
		if err != nil {
			return facts, fmt.Errorf("AWS region %s: %w", name, err)
		}
		return facts, nil
	})
}

// learn asks EC2 for what a region's offers and creates are made from.
func (c *AWS) learn(ctx context.Context, client ec2API, region types.AWSProviderRegionConfig) (awsRegionFacts, error) {
	facts := awsRegionFacts{groups: region.SecurityGroupIDs}
	var err error
	if facts.subnets, err = awsSubnets(ctx, client, region.SubnetIDs); err != nil {
		return facts, err
	}
	if facts.image, facts.rootDevice, err = c.image(ctx, client, region.ImageID); err != nil {
		return facts, err
	}
	facts.types, err = awsGPUTypes(ctx, client)
	return facts, err
}

// awsSubnets finds the subnet machines are put in, for each availability
// zone: among the subnets named, or among the default ones of the region's
// default VPC when none is named.
func awsSubnets(ctx context.Context, client ec2API, named []string) (map[string]string, error) {
	wanted := &ec2.DescribeSubnetsInput{SubnetIds: named}
	if len(named) == 0 {
		wanted.Filters = []ec2types.Filter{{Name: aws.String("default-for-az"), Values: []string{"true"}}}
	}
	byZone := map[string]string{}
	for pages := ec2.NewDescribeSubnetsPaginator(client, wanted, endSubnets); pages.HasMorePages(); {
		page, err := within(ctx, func(ctx context.Context) (*ec2.DescribeSubnetsOutput, error) { return pages.NextPage(ctx) })
		if err != nil {
			return nil, fmt.Errorf("its subnets: %w", err)
		}
		for _, subnet := range page.Subnets {
			if zone := aws.ToString(subnet.AvailabilityZone); byZone[zone] == "" {
				byZone[zone] = aws.ToString(subnet.SubnetId)
			}
		}
	}
	if len(byZone) == 0 {
		return nil, fmt.Errorf("it has no default subnets: name the subnets to use as subnetIds under providers.aws.regions")
	}
	return byZone, nil
}

// image finds the image a machine boots, and returns its ID and the device
// its disk is attached as. It is the image with the given ID, or without
// one the newest of Amazon's that fits the configured name.
func (c *AWS) image(ctx context.Context, client ec2API, id string) (string, string, error) {
	image := &ec2.DescribeImagesInput{Owners: []string{awsImageOwner}, Filters: []ec2types.Filter{
		{Name: aws.String("name"), Values: []string{c.config.ImageName}},
		{Name: aws.String("state"), Values: []string{"available"}},
		{Name: aws.String("architecture"), Values: []string{"x86_64"}},
	}}
	if id != "" {
		image = &ec2.DescribeImagesInput{ImageIds: []string{id}}
	}
	images, err := within(ctx, func(ctx context.Context) (*ec2.DescribeImagesOutput, error) { return client.DescribeImages(ctx, image) })
	if err != nil {
		return "", "", fmt.Errorf("its image: %w", err)
	}
	if len(images.Images) == 0 {
		return "", "", fmt.Errorf("it has no image %q", cmp.Or(id, c.config.ImageName))
	}
	// EC2 writes an image's date so that the newest sorts last.
	newest := slices.MaxFunc(images.Images, func(a, b ec2types.Image) int {
		return cmp.Compare(aws.ToString(a.CreationDate), aws.ToString(b.CreationDate))
	})
	return aws.ToString(newest.ImageId), aws.ToString(newest.RootDeviceName), nil
}

// awsGPUTypes lists the instance types to buy, by name: those for the
// image's kind of CPU that awsMachine takes.
func awsGPUTypes(ctx context.Context, client ec2API) (map[string]awsType, error) {
	// The image is for x86 CPUs.
	sold := &ec2.DescribeInstanceTypesInput{Filters: []ec2types.Filter{
		{Name: aws.String("processor-info.supported-architecture"), Values: []string{"x86_64"}},
	}}
	machines := map[string]awsType{}
	for pages := ec2.NewDescribeInstanceTypesPaginator(client, sold, endInstanceTypes); pages.HasMorePages(); {
		page, err := within(ctx, func(ctx context.Context) (*ec2.DescribeInstanceTypesOutput, error) { return pages.NextPage(ctx) })
		if err != nil {
			return nil, fmt.Errorf("its instance types: %w", err)
		}
		for _, info := range page.InstanceTypes {
			if machine, ok := awsMachine(info); ok {
				machines[string(info.InstanceType)] = machine
			}
		}
	}
	return machines, nil
}

// awsMachine reads what an instance type is, and whether it is one to buy:
// a machine with whole NVIDIA GPUs of one kind.
func awsMachine(info ec2types.InstanceTypeInfo) (awsType, bool) {
	if info.GpuInfo == nil || len(info.GpuInfo.Gpus) != 1 || info.VCpuInfo == nil || info.MemoryInfo == nil {
		return awsType{}, false
	}
	gpu := info.GpuInfo.Gpus[0]
	// Some types have a share of a GPU.
	shared := gpu.GpuPartitionSize != nil && *gpu.GpuPartitionSize < 1
	if !strings.EqualFold(aws.ToString(gpu.Manufacturer), "nvidia") || shared || aws.ToInt32(gpu.Count) <= 0 {
		return awsType{}, false
	}
	// EC2 calls both of NVIDIA's A100s "A100", and both V100s "V100".
	// Their memory tells them apart.
	name := types.NormalizeGPUType(aws.ToString(gpu.Name))
	if (name == types.GPU_A100 || name == types.GPU_V100) && gpu.MemoryInfo != nil {
		name = types.NormalizeGPUType(fmt.Sprintf("%s %dGB", name, aws.ToInt32(gpu.MemoryInfo.SizeInMiB)/1024))
	}
	return awsType{
		gpu:       string(name),
		gpus:      uint32(aws.ToInt32(gpu.Count)),
		vcpus:     int64(aws.ToInt32(info.VCpuInfo.DefaultVCpus)),
		memoryMiB: aws.ToInt64(info.MemoryInfo.SizeInMiB),
	}, true
}

// Offers lists the GPU instance types that have a Spot price now: one offer
// for each type and availability zone, since the price and the stock are the
// zone's.
func (c *AWS) Offers(ctx context.Context, filter OfferFilter) ([]Offer, error) {
	offers, err := offersByRegion(awsName, slices.Sorted(maps.Keys(c.regions)), filter, func(region string) ([]Offer, error) {
		return c.regionOffers(ctx, region)
	})
	if err != nil {
		return nil, err
	}
	sortOffers(offers)
	return offers, nil
}

// regionOffers lists what one region sells.
func (c *AWS) regionOffers(ctx context.Context, name string) ([]Offer, error) {
	facts, err := c.region(ctx, name)
	if err != nil {
		return nil, err
	}
	client, err := c.client(ctx, name)
	if err != nil {
		return nil, err
	}
	latest, err := c.spotPrices(ctx, client, facts.types)
	if err != nil {
		return nil, fmt.Errorf("AWS Spot prices in %s: %w", name, err)
	}

	var offers []Offer
	for at, price := range latest {
		machine, known := facts.types[at.instanceType]
		hourly, err := strconv.ParseFloat(aws.ToString(price.SpotPrice), 64)
		if !known || facts.subnets[at.zone] == "" || err != nil || hourly <= 0 {
			continue
		}
		offers = append(offers, Offer{
			ID:               at.zone + "/" + at.instanceType,
			Provider:         awsName,
			Cloud:            awsName,
			InstanceType:     at.instanceType,
			Region:           name,
			GPU:              machine.gpu,
			GPUCount:         machine.gpus,
			CPUMillicores:    machine.vcpus * 1000,
			MemoryMB:         machine.memoryMiB,
			StorageMB:        c.config.DiskGB * 1024,
			HourlyCostMicros: DollarsToMicros(hourly) + hourlyDiskMicros(c.config.DiskGB, awsDiskMicrosPerGBMonth),
			Spot:             true,
		})
	}
	return offers, nil
}

// awsMarket is what EC2 sets one Spot price for: an instance type in an
// availability zone.
type awsMarket struct{ zone, instanceType string }

// spotPrices asks for the Spot price in force of each of the instance
// types, in each availability zone that has one.
func (c *AWS) spotPrices(ctx context.Context, client ec2API, instanceTypes map[string]awsType) (map[awsMarket]ec2types.SpotPrice, error) {
	sold := make([]ec2types.InstanceType, 0, len(instanceTypes))
	for instanceType := range instanceTypes {
		sold = append(sold, ec2types.InstanceType(instanceType))
	}
	if len(sold) == 0 {
		// Asked about no instance type, EC2 answers about all of them.
		return nil, nil
	}
	slices.Sort(sold)

	// Asked for the prices from now on, EC2 answers with the one in force
	// in each zone.
	latest := map[awsMarket]ec2types.SpotPrice{}
	prices := ec2.NewDescribeSpotPriceHistoryPaginator(client, &ec2.DescribeSpotPriceHistoryInput{
		InstanceTypes:       sold,
		ProductDescriptions: []string{awsLinux},
		StartTime:           aws.Time(c.now()),
	}, endSpotPrices)
	for prices.HasMorePages() {
		page, err := within(ctx, func(ctx context.Context) (*ec2.DescribeSpotPriceHistoryOutput, error) { return prices.NextPage(ctx) })
		if err != nil {
			return nil, err
		}
		for _, price := range page.SpotPriceHistory {
			at := awsMarket{aws.ToString(price.AvailabilityZone), string(price.InstanceType)}
			if known, ok := latest[at]; !ok || aws.ToTime(price.Timestamp).After(aws.ToTime(known.Timestamp)) {
				latest[at] = price
			}
		}
	}
	return latest, nil
}

func (c *AWS) Create(ctx context.Context, req CreateRequest) (Instance, error) {
	tags, err := ownedTags(c.owner, req.Tags)
	if err != nil {
		return Instance{}, err
	}
	zone, instanceType, ok := strings.Cut(req.Offer.ID, "/")
	if !ok || zone == "" || instanceType == "" || req.Offer.Region == "" {
		return Instance{}, fmt.Errorf("an AWS offer needs an availability zone, an instance type and a region: %+v", req.Offer)
	}
	if !req.Offer.Spot {
		return Instance{}, fmt.Errorf("this client buys Spot Instances from AWS, and the offer is not for one: %+v", req.Offer)
	}
	facts, err := c.region(ctx, req.Offer.Region)
	if err != nil {
		return Instance{}, err
	}
	client, err := c.client(ctx, req.Offer.Region)
	if err != nil {
		return Instance{}, err
	}
	tags[awsNameTag] = req.Name
	run, err := c.runInput(req, tags, facts, zone, instanceType)
	if err != nil {
		return Instance{}, err
	}

	started, err := within(ctx, func(ctx context.Context) (*ec2.RunInstancesOutput, error) { return client.RunInstances(ctx, run) })
	if err != nil {
		return Instance{}, awsRefusal(err)
	}
	if len(started.Instances) != 1 || aws.ToString(started.Instances[0].InstanceId) == "" {
		return Instance{}, fmt.Errorf("AWS accepted the create and returned %d instances", len(started.Instances))
	}
	return Instance{ID: req.Offer.Region + "/" + aws.ToString(started.Instances[0].InstanceId), Status: StatusPending, Tags: req.Tags}, nil
}

// runInput describes the instance a create asks EC2 to run: a Spot Instance
// of the type in the zone's subnet, with a disk and tags of its own, that is
// gone for good once AWS takes it back or it shuts itself down.
func (c *AWS) runInput(req CreateRequest, tags map[string]string, facts awsRegionFacts, zone, instanceType string) (*ec2.RunInstancesInput, error) {
	subnet := facts.subnets[zone]
	if subnet == "" {
		return nil, fmt.Errorf("AWS region %s has no subnet to use in %s", req.Offer.Region, zone)
	}

	spot := &ec2types.SpotMarketOptions{
		SpotInstanceType: ec2types.SpotInstanceTypeOneTime,
		// AWS takes the machine back for good: nothing waits for it to
		// come back.
		InstanceInterruptionBehavior: ec2types.InstanceInterruptionBehaviorTerminate,
	}
	if req.MaxHourlyCostMicros > 0 {
		// The limit is the machine's; AWS's is the instance's, less the
		// disk. Without one AWS charges up to the on-demand price.
		limit := req.MaxHourlyCostMicros - hourlyDiskMicros(c.config.DiskGB, awsDiskMicrosPerGBMonth)
		if limit <= 0 {
			return nil, fmt.Errorf("a price limit of %d micros an hour leaves nothing for an AWS instance", req.MaxHourlyCostMicros)
		}
		spot.MaxPrice = aws.String(fmt.Sprintf("%d.%04d", limit/1_000_000, limit%1_000_000/100))
	}

	tagList := awsTagList(tags)
	run := &ec2.RunInstancesInput{
		ImageId:      aws.String(facts.image),
		InstanceType: ec2types.InstanceType(instanceType),
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		// EC2 runs one instance for a token however often it is sent, so
		// the SDK may send a create again. A name is no longer than a
		// token may be.
		ClientToken:           aws.String(req.Name),
		InstanceMarketOptions: &ec2types.InstanceMarketOptionsRequest{MarketType: ec2types.MarketTypeSpot, SpotOptions: spot},
		NetworkInterfaces: []ec2types.InstanceNetworkInterfaceSpecification{{
			DeviceIndex:              aws.Int32(0),
			SubnetId:                 aws.String(subnet),
			Groups:                   facts.groups,
			AssociatePublicIpAddress: aws.Bool(true),
		}},
		BlockDeviceMappings: []ec2types.BlockDeviceMapping{{
			DeviceName: aws.String(facts.rootDevice),
			Ebs: &ec2types.EbsBlockDevice{
				VolumeSize:          aws.Int32(int32(c.config.DiskGB)),
				VolumeType:          ec2types.VolumeTypeGp3,
				DeleteOnTermination: aws.Bool(true),
			},
		}},
		// A machine that shuts itself down is not left stopped, with a
		// disk that is paid for.
		InstanceInitiatedShutdownBehavior: ec2types.ShutdownBehaviorTerminate,
		// What a machine is told about itself, its startup script
		// included, is given only to a caller that first fetched a
		// token, which a request forged through a workload cannot.
		MetadataOptions: &ec2types.InstanceMetadataOptionsRequest{HttpTokens: ec2types.HttpTokensStateRequired},
		TagSpecifications: []ec2types.TagSpecification{
			{ResourceType: ec2types.ResourceTypeInstance, Tags: tagList},
			{ResourceType: ec2types.ResourceTypeVolume, Tags: tagList},
		},
	}
	if req.StartupScript != "" {
		run.UserData = aws.String(base64.StdEncoding.EncodeToString([]byte(bootScript(req.StartupScript))))
	}
	if c.config.KeyName != "" {
		run.KeyName = aws.String(c.config.KeyName)
	}
	return run, nil
}

// awsRefusal names why a create was turned down.
func awsRefusal(err error) error {
	switch awsCode(err) {
	case "InsufficientInstanceCapacity", "SpotMaxPriceTooLow":
		return because(ErrOutOfStock, err)
	case "MaxSpotInstanceCountExceeded", "VcpuLimitExceeded", "InstanceLimitExceeded":
		return because(ErrQuota, err)
	}
	return err
}

// awsCode returns the code of an error of EC2's, or "".
func awsCode(err error) string {
	var refused smithy.APIError
	if errors.As(err, &refused) {
		return refused.ErrorCode()
	}
	return ""
}

// awsGone reports whether EC2 says there is no such instance.
func awsGone(err error) bool {
	return strings.HasPrefix(awsCode(err), "InvalidInstanceID.")
}

// awsPlace splits an instance's ID here, "region/i-0123". An ID of another
// form is not one of this client's, and names nothing at AWS.
func awsPlace(id string) (region, instance string, err error) {
	region, instance, ok := strings.Cut(id, "/")
	if !ok || region == "" || instance == "" {
		return "", "", because(ErrNotFound, fmt.Errorf("AWS instance %q is not a region and an instance ID", id))
	}
	return region, instance, nil
}

// awsTagList writes tags the way EC2 takes them, in the order of their keys.
func awsTagList(tags map[string]string) []ec2types.Tag {
	list := make([]ec2types.Tag, 0, len(tags))
	for _, key := range slices.Sorted(maps.Keys(tags)) {
		list = append(list, ec2types.Tag{Key: aws.String(key), Value: aws.String(tags[key])})
	}
	return list
}

func awsTagMap(list []ec2types.Tag) map[string]string {
	tags := make(map[string]string, len(list))
	for _, tag := range list {
		tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return tags
}

// awsInstance is what EC2 says of an instance.
type awsInstance ec2types.Instance

func (c *AWS) instance(region string, raw awsInstance) Instance {
	tags := awsTagMap(raw.Tags)
	delete(tags, awsNameTag)
	instance := Instance{
		ID:     region + "/" + aws.ToString(raw.InstanceId),
		Status: raw.status(),
		Tags:   callerTags(tags),
	}
	detail := raw.state()
	if raw.StateReason != nil {
		detail = strings.TrimSpace(detail + ": " + aws.ToString(raw.StateReason.Message))
	}
	instance.StatusDetail = statusDetail(instance.Status, detail)
	instance.reachableAt(aws.ToString(raw.PublicIpAddress))
	return instance
}

// state is EC2's name for what the instance is doing.
func (i awsInstance) state() string {
	if i.State == nil {
		return ""
	}
	return string(i.State.Name)
}

func (i awsInstance) status() Status {
	// AWS gives the reason it ended an instance as a code, and the ones
	// for a Spot Instance it took back start like this.
	takenBack := i.StateReason != nil && strings.HasPrefix(aws.ToString(i.StateReason.Code), "Server.Spot")
	switch ec2types.InstanceStateName(i.state()) {
	case ec2types.InstanceStateNamePending:
		return StatusPending
	case ec2types.InstanceStateNameRunning:
		return StatusActive
	case ec2types.InstanceStateNameShuttingDown, ec2types.InstanceStateNameTerminated:
		if takenBack {
			return StatusInterrupted
		}
		return StatusDeleted
	case ec2types.InstanceStateNameStopping, ec2types.InstanceStateNameStopped:
		if takenBack {
			return StatusInterrupted
		}
		// Nothing here stops a machine. One that is stopped is no use,
		// and its disk is paid for.
		return StatusFailed
	}
	return unknownStatus(awsName, i.state())
}

func (c *AWS) Get(ctx context.Context, id string) (Instance, error) {
	region, instance, err := awsPlace(id)
	if err != nil {
		return Instance{}, err
	}
	// An instance in a region that is no longer configured is not gone: the
	// machine may be running there, so asking about it is an error.
	client, err := c.client(ctx, region)
	if err != nil {
		return Instance{}, fmt.Errorf("AWS instance %q: %w", id, err)
	}
	found, err := within(ctx, func(ctx context.Context) (*ec2.DescribeInstancesOutput, error) {
		return client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{instance}})
	})
	if awsGone(err) {
		return Instance{}, because(ErrNotFound, err)
	}
	if err != nil {
		return Instance{}, err
	}
	var described []ec2types.Instance
	for _, reservation := range found.Reservations {
		described = append(described, reservation.Instances...)
	}
	if len(described) == 0 {
		return Instance{}, because(ErrNotFound, fmt.Errorf("AWS has no instance %s", id))
	}
	return c.instance(region, awsInstance(described[0])), nil
}

// List returns the instances this control plane bought, in every configured
// region, that AWS still has. A terminated instance is shown for about an
// hour, and nothing is left of it.
func (c *AWS) List(ctx context.Context) ([]Instance, error) {
	instances := []Instance{}
	for _, name := range slices.Sorted(maps.Keys(c.regions)) {
		client, err := c.client(ctx, name)
		if err != nil {
			return nil, err
		}
		pages := ec2.NewDescribeInstancesPaginator(client, &ec2.DescribeInstancesInput{Filters: []ec2types.Filter{
			{Name: aws.String("tag:" + OwnerTag), Values: []string{c.owner}},
		}}, endInstances)
		for pages.HasMorePages() {
			page, err := within(ctx, func(ctx context.Context) (*ec2.DescribeInstancesOutput, error) { return pages.NextPage(ctx) })
			if err != nil {
				return nil, fmt.Errorf("AWS instances in %s: %w", name, err)
			}
			for _, reservation := range page.Reservations {
				for _, described := range reservation.Instances {
					// What is listed here may be deleted as an orphan,
					// so the filter is not left to EC2 alone.
					raw := awsInstance(described)
					mine := awsTagMap(raw.Tags)[OwnerTag] == c.owner
					if mine && raw.state() != string(ec2types.InstanceStateNameTerminated) {
						instances = append(instances, c.instance(name, raw))
					}
				}
			}
		}
	}
	return instances, nil
}

// Delete terminates the instance, and its disk goes with it.
func (c *AWS) Delete(ctx context.Context, id string) error {
	region, instance, err := awsPlace(id)
	if err != nil {
		return nil
	}
	// As for Get: one in a region that is no longer configured is not gone.
	client, err := c.client(ctx, region)
	if err != nil {
		return fmt.Errorf("AWS instance %q: %w", id, err)
	}
	_, err = within(ctx, func(ctx context.Context) (*ec2.TerminateInstancesOutput, error) {
		return client.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{instance}})
	})
	if awsGone(err) {
		return nil
	}
	return err
}
