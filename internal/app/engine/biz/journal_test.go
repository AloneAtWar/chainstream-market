package biz

import "testing"

func TestJournalRevertRestoresPreviousValues(t *testing.T) {
	values := newJournal[string, int](stringKeys("test"))
	values.set(10, "a", 1)
	values.set(11, "a", 2)
	values.set(11, "b", 3)
	values.set(12, "a", 4)

	values.revert(11)
	if got, _ := values.get("a"); got != 2 {
		t.Fatalf("a after revert(11) = %d, want 2", got)
	}
	values.revert(10)
	if got, _ := values.get("a"); got != 1 {
		t.Fatalf("a after revert(10) = %d, want 1", got)
	}
	if _, ok := values.get("b"); ok {
		t.Fatal("b survived revert(10)")
	}
}

func TestJournalPruneMakesWritesPermanent(t *testing.T) {
	values := newJournal[string, int](stringKeys("test"))
	values.set(10, "a", 1)
	values.set(11, "a", 2)
	values.set(12, "a", 3)
	values.prune(11)
	if len(values.undo) != 1 {
		t.Fatalf("undo entries = %d, want 1", len(values.undo))
	}
	values.revert(0)
	if got, _ := values.get("a"); got != 2 {
		t.Fatalf("a = %d, want 2 (writes at or below 11 are permanent)", got)
	}
}
