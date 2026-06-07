package runtime

import (
	"reflect"
	"testing"
)

func TestPlanReconcileStartsDesiredSorted(t *testing.T) {
	plan := planReconcile([]string{"b", "a", "c"}, nil, 0)
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(plan.toStart, want) {
		t.Errorf("toStart = %v, want %v", plan.toStart, want)
	}
	if len(plan.toStop) != 0 {
		t.Errorf("toStop = %v, want empty", plan.toStop)
	}
	if plan.startSlots != 3 {
		t.Errorf("startSlots = %d, want 3", plan.startSlots)
	}
}

func TestPlanReconcileStopsUndesired(t *testing.T) {
	plan := planReconcile([]string{"a"}, []string{"a", "b"}, 0)
	if want := []string{"b"}; !reflect.DeepEqual(plan.toStop, want) {
		t.Errorf("toStop = %v, want %v", plan.toStop, want)
	}
	if len(plan.toStart) != 0 {
		t.Errorf("toStart = %v, want empty", plan.toStart)
	}
}

func TestPlanReconcileNoChurnWhenSteady(t *testing.T) {
	plan := planReconcile([]string{"a", "b"}, []string{"a", "b"}, 0)
	if len(plan.toStart) != 0 || len(plan.toStop) != 0 {
		t.Errorf("expected no churn, got start=%v stop=%v", plan.toStart, plan.toStop)
	}
}

func TestPlanReconcileRespectsMaxGroups(t *testing.T) {
	// One slot is free. All candidates are returned so the node can continue
	// after a failed lease attempt, but only one successful start may consume
	// the remaining slot.
	plan := planReconcile([]string{"a", "b", "c", "d"}, []string{"a"}, 2)
	if want := []string{"b", "c", "d"}; !reflect.DeepEqual(plan.toStart, want) {
		t.Errorf("toStart = %v, want %v", plan.toStart, want)
	}
	if plan.startSlots != 1 {
		t.Errorf("startSlots = %d, want 1", plan.startSlots)
	}
}

func TestPlanReconcileFullCapacityStartsNothing(t *testing.T) {
	plan := planReconcile([]string{"a", "b", "c"}, []string{"a", "b"}, 2)
	if want := []string{"c"}; !reflect.DeepEqual(plan.toStart, want) {
		t.Errorf("toStart = %v, want candidates %v", plan.toStart, want)
	}
	if plan.startSlots != 0 {
		t.Errorf("startSlots = %d, want 0 at full capacity", plan.startSlots)
	}
}

func TestPlanReconcileStopFreesCapacityForStart(t *testing.T) {
	// "x" is no longer desired -> stop; that frees the only slot so "a" starts.
	plan := planReconcile([]string{"a"}, []string{"x"}, 1)
	if want := []string{"x"}; !reflect.DeepEqual(plan.toStop, want) {
		t.Errorf("toStop = %v, want %v", plan.toStop, want)
	}
	if want := []string{"a"}; !reflect.DeepEqual(plan.toStart, want) {
		t.Errorf("toStart = %v, want %v", plan.toStart, want)
	}
	if plan.startSlots != 1 {
		t.Errorf("startSlots = %d, want 1", plan.startSlots)
	}
}

func TestPlanReconcileUnlimitedWhenMaxNonPositive(t *testing.T) {
	plan := planReconcile([]string{"a", "b", "c", "d", "e"}, nil, 0)
	if len(plan.toStart) != 5 {
		t.Errorf("toStart = %v, want all 5 with unlimited capacity", plan.toStart)
	}
	if plan.startSlots != 5 {
		t.Errorf("startSlots = %d, want 5", plan.startSlots)
	}
}
