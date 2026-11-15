package providers

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

// Fake is a provider that exists only in memory. Tests and the scheduler
// simulator buy from it, and decide what it has in stock and what becomes of
// the machines it sold.
type Fake struct {
	name  string
	owner string

	mu      sync.Mutex
	offers  []Offer
	refusal error
	// allowed is how many more creates succeed before the refusal applies.
	allowed   int
	deleteErr error
	instances map[string]*fakeInstance
	sold      int
	creates   []CreateRequest
	deletes   []string
	// unanswered is how many more creates are carried out and then reported
	// as failed.
	unanswered int
}

type fakeInstance struct {
	Instance
	owner string
	until time.Time
	offer Offer
}

func NewFake(name, owner string) *Fake {
	return &Fake{name: name, owner: owner, instances: map[string]*fakeInstance{}}
}

func (f *Fake) Name() string { return f.name }

// SetOffers replaces what the fake sells. An offer with a count sells out.
func (f *Fake) SetOffers(offers ...Offer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.offers = slices.Clone(offers)
	for i := range f.offers {
		f.offers[i].Provider = f.name
	}
}

// RefuseCreates makes every create fail with err, until it is called with nil.
func (f *Fake) RefuseCreates(err error) {
	f.RefuseCreatesAfter(0, err)
}

// RefuseCreatesAfter lets the next n creates through and makes the ones after
// them fail with err: a provider whose catalogue promised more than it had.
func (f *Fake) RefuseCreatesAfter(n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refusal, f.allowed = err, n
}

// LoseAnswers makes the next n creates succeed at the fake and fail for the
// caller: a request that timed out after the provider had acted on it.
func (f *Fake) LoseAnswers(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unanswered = n
}

// FailDeletes makes every delete fail with err, until it is called with nil.
func (f *Fake) FailDeletes(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteErr = err
}

// Plant puts an instance at the fake that owner bought some other way, and
// returns its ID.
func (f *Fake) Plant(owner string, status Status, tags map[string]string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.add(owner, status, tags).ID
}

// SetStatus changes what the fake reports about an instance.
func (f *Fake) SetStatus(id string, status Status, detail string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if instance, ok := f.instances[id]; ok {
		instance.Status, instance.StatusDetail = status, detail
	}
}

// SetAddress gives an instance the public address the fake reports for it.
func (f *Fake) SetAddress(id, publicIP string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if instance, ok := f.instances[id]; ok {
		instance.PublicIP = publicIP
	}
}

// Forget removes an instance as if the provider had lost it.
func (f *Fake) Forget(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.instances, id)
}

// Everything returns every instance at the fake, whoever bought it, in the
// order they were sold.
func (f *Fake) Everything() []Instance {
	f.mu.Lock()
	defer f.mu.Unlock()
	instances := make([]Instance, 0, len(f.instances))
	for _, instance := range f.instances {
		instances = append(instances, instance.Instance)
	}
	slices.SortFunc(instances, func(a, b Instance) int { return compareIDs(a.ID, b.ID) })
	return instances
}

// Sold returns the offer an instance was bought from. It is the zero offer
// for an instance that was planted.
func (f *Fake) Sold(id string) Offer {
	f.mu.Lock()
	defer f.mu.Unlock()
	if instance, ok := f.instances[id]; ok {
		return instance.offer
	}
	return Offer{}
}

// Creates returns every create request the fake accepted.
func (f *Fake) Creates() []CreateRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.creates)
}

// Deletes returns the ID of every delete the fake received, failed or not.
func (f *Fake) Deletes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.deletes)
}

// Until returns when the instance is set to delete itself.
func (f *Fake) Until(id string) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	if instance, ok := f.instances[id]; ok {
		return instance.until
	}
	return time.Time{}
}

func (f *Fake) Offers(ctx context.Context, filter OfferFilter) ([]Offer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	offers := []Offer{}
	for _, offer := range f.offers {
		if filter.Matches(offer) {
			offers = append(offers, offer)
		}
	}
	return offers, nil
}

func (f *Fake) Create(ctx context.Context, req CreateRequest) (Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := ownedTags(f.owner, req.Tags); err != nil {
		return Instance{}, err
	}
	if f.refusal != nil {
		if f.allowed == 0 {
			return Instance{}, f.refusal
		}
		f.allowed--
	}
	sold, err := f.sell(req.Offer)
	if err != nil {
		return Instance{}, err
	}
	instance := f.add(f.owner, StatusPending, req.Tags)
	instance.until = req.Until
	instance.offer = sold
	f.creates = append(f.creates, req)
	if f.unanswered > 0 {
		f.unanswered--
		return Instance{}, fmt.Errorf("%s: create %s: the connection was reset before the answer arrived", f.name, req.Name)
	}
	return instance.Instance, nil
}

// sell takes one machine out of the offer's stock and returns the offer as
// the fake holds it now, which may differ from what the buyer last saw.
func (f *Fake) sell(wanted Offer) (Offer, error) {
	for i, offer := range f.offers {
		if offer.ID != wanted.ID || offer.Region != wanted.Region || offer.Cloud != wanted.Cloud || offer.Spot != wanted.Spot {
			continue
		}
		switch offer.Available {
		case 0: // not counted: never sells out
		case 1:
			f.offers = slices.Delete(f.offers, i, i+1)
		default:
			f.offers[i].Available--
		}
		return offer, nil
	}
	return Offer{}, because(ErrOutOfStock, fmt.Errorf("%s has no %s in %s", f.name, wanted.ID, wanted.Region))
}

func (f *Fake) add(owner string, status Status, tags map[string]string) *fakeInstance {
	f.sold++
	instance := &fakeInstance{
		Instance: Instance{ID: fmt.Sprintf("%s-%d", f.name, f.sold), Status: status, Tags: maps.Clone(tags)},
		owner:    owner,
	}
	f.instances[instance.ID] = instance
	return instance
}

func (f *Fake) Get(ctx context.Context, id string) (Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	instance, ok := f.instances[id]
	if !ok {
		return Instance{}, because(ErrNotFound, fmt.Errorf("%s has no instance %s", f.name, id))
	}
	return instance.Instance, nil
}

func (f *Fake) List(ctx context.Context) ([]Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	instances := []Instance{}
	for _, instance := range f.instances {
		if instance.owner == f.owner {
			instances = append(instances, instance.Instance)
		}
	}
	slices.SortFunc(instances, func(a, b Instance) int { return compareIDs(a.ID, b.ID) })
	return instances, nil
}

func (f *Fake) Extend(ctx context.Context, id string, until time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	instance, ok := f.instances[id]
	if !ok {
		return fmt.Errorf("%s has no instance %s", f.name, id)
	}
	instance.until = until
	return nil
}

func (f *Fake) Delete(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, id)
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.instances, id)
	return nil
}

// compareIDs orders the fake's IDs by the order they were sold in.
func compareIDs(a, b string) int {
	return cmp.Or(cmp.Compare(len(a), len(b)), cmp.Compare(a, b))
}
