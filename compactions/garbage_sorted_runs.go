package compactions

type GarbageSortedRuns struct {
	sortedRuns []*SortedRun
}

func NewGarbageSortedRuns() *GarbageSortedRuns {
	return &GarbageSortedRuns{}
}

func (gr *GarbageSortedRuns) Add(sortedRun *SortedRun) {
	sortedRun.DecCounterLink()
	gr.sortedRuns = append(gr.sortedRuns, sortedRun)
}

func (gr *GarbageSortedRuns) NeedToDelete() []*SortedRun {
	var needToDelete, newSortedRuns []*SortedRun
	for _, sortedRun := range gr.sortedRuns{
		if sortedRun.CounterLink() == 0 {
			needToDelete = append(needToDelete, sortedRun)
		} else {
			newSortedRuns = append(newSortedRuns, sortedRun)
		}
	}

	gr.sortedRuns = newSortedRuns
	return needToDelete
}
