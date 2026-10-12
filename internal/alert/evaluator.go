package alert

type Action int

const (
	ActionNone Action = iota
	ActionOpen
	ActionClose
)

// equipment_id + metric (compressor + vibration or pump + temperature)
type Key struct {
	EquipmentID string // comp , pump, turbine
	Metric      string // tempreture, pressure, vibration
}

type State struct {
	ConsecutiveBreaches int
	AlertOpen           bool
}

type Rule struct {
	Threshold        float64
	ConsecutiveCount int
}

func Evaluate(rule Rule, currentState State, value float64) (State, Action) {
	nextState := currentState
	if value <= rule.Threshold {
		nextState.ConsecutiveBreaches = 0
		nextState.AlertOpen = false

		if currentState.AlertOpen {
			return nextState, ActionClose
		}

		return nextState, ActionNone
	}

	if currentState.AlertOpen {
		return nextState, ActionNone
	}

	nextState.ConsecutiveBreaches += 1

	if nextState.ConsecutiveBreaches >= rule.ConsecutiveCount {
		nextState.AlertOpen = true
		return nextState, ActionOpen
	}

	return nextState, ActionNone
}
