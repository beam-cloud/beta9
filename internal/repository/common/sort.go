package common

type TwoSlices struct {
	MainSlice  []string
	OtherSlice []string
}

func (ts TwoSlices) Len() int {
	return len(ts.MainSlice)
}

func (ts TwoSlices) Swap(i, j int) {
	ts.MainSlice[i], ts.MainSlice[j] = ts.MainSlice[j], ts.MainSlice[i]
	ts.OtherSlice[i], ts.OtherSlice[j] = ts.OtherSlice[j], ts.OtherSlice[i]
}

func (ts TwoSlices) Less(i, j int) bool {
	return ts.MainSlice[i] < ts.MainSlice[j]
}
