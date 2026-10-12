package alert

import "testing"

func TestEvaluate(t *testing.T) {
	type step struct {
		value      float64
		wantState  State
		wantAction Action
	}

	tests := []struct {
		name             string
		consecutiveCount int
		initialState     State
		steps            []step
	}{
		{
			name:             "at or below threshold is normal",
			consecutiveCount: 3,
			steps: []step{
				{value: 94, wantState: State{}, wantAction: ActionNone},
				{value: 95, wantState: State{}, wantAction: ActionNone},
			},
		},
		{
			name:             "normal reading breaks the streak",
			consecutiveCount: 3,
			steps: []step{
				{value: 96, wantState: State{ConsecutiveBreaches: 1}, wantAction: ActionNone},
				{value: 97, wantState: State{ConsecutiveBreaches: 2}, wantAction: ActionNone},
				{value: 95, wantState: State{}, wantAction: ActionNone},
				{value: 98, wantState: State{ConsecutiveBreaches: 1}, wantAction: ActionNone},
			},
		},
		{
			name:             "sustained breach opens once",
			consecutiveCount: 3,
			steps: []step{
				{value: 96, wantState: State{ConsecutiveBreaches: 1}, wantAction: ActionNone},
				{value: 97, wantState: State{ConsecutiveBreaches: 2}, wantAction: ActionNone},
				{value: 98, wantState: State{ConsecutiveBreaches: 3, AlertOpen: true}, wantAction: ActionOpen},
				{value: 99, wantState: State{ConsecutiveBreaches: 3, AlertOpen: true}, wantAction: ActionNone},
				{value: 100, wantState: State{ConsecutiveBreaches: 3, AlertOpen: true}, wantAction: ActionNone},
			},
		},
		{
			name:             "recovery closes once and allows reopening",
			consecutiveCount: 3,
			steps: []step{
				{value: 96, wantState: State{ConsecutiveBreaches: 1}, wantAction: ActionNone},
				{value: 97, wantState: State{ConsecutiveBreaches: 2}, wantAction: ActionNone},
				{value: 98, wantState: State{ConsecutiveBreaches: 3, AlertOpen: true}, wantAction: ActionOpen},
				{value: 95, wantState: State{}, wantAction: ActionClose},
				{value: 94, wantState: State{}, wantAction: ActionNone},
				{value: 96, wantState: State{ConsecutiveBreaches: 1}, wantAction: ActionNone},
				{value: 97, wantState: State{ConsecutiveBreaches: 2}, wantAction: ActionNone},
				{value: 98, wantState: State{ConsecutiveBreaches: 3, AlertOpen: true}, wantAction: ActionOpen},
			},
		},
		{
			name:             "rule can require just one breach",
			consecutiveCount: 1,
			steps: []step{
				{value: 96, wantState: State{ConsecutiveBreaches: 1, AlertOpen: true}, wantAction: ActionOpen},
				{value: 97, wantState: State{ConsecutiveBreaches: 1, AlertOpen: true}, wantAction: ActionNone},
				{value: 94, wantState: State{}, wantAction: ActionClose},
			},
		},
		{
			name:             "restored open alert needs no breach tally",
			consecutiveCount: 3,
			initialState:     State{AlertOpen: true},
			steps: []step{
				{value: 96, wantState: State{AlertOpen: true}, wantAction: ActionNone},
				{value: 94, wantState: State{}, wantAction: ActionClose},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := Rule{Threshold: 95, ConsecutiveCount: tc.consecutiveCount}
			state := tc.initialState

			for i, s := range tc.steps {
				before := state
				nextState, action := Evaluate(rule, state, s.value)

				if state != before {
					t.Fatalf("step %d (value %g): input state changed from %+v to %+v", i+1, s.value, before, state)
				}
				if action != s.wantAction || nextState != s.wantState {
					t.Fatalf("step %d (value %g): got state %+v, action %v; want state %+v, action %v",
						i+1, s.value, nextState, action, s.wantState, s.wantAction)
				}

				// Feed the returned state into the next reading in this sequence.
				state = nextState
			}
		})
	}
}
