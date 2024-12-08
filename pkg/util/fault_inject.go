package util

import (
	"sync"
	"sync/atomic"
)

const (
	FAULTS_COUNT     int = 1024
	FAULTS_SCOPE_TXN int = 0
)

var faultsSwitch [FAULTS_COUNT]Faults

type Faults struct {
	_enable atomic.Bool
	_faults sync.Map
}

type FaultAction struct {
	Args   []string
	Action func([]string) error
}

func Open(scope int) {
	if scope >= FAULTS_COUNT || scope < 0 {
		return
	}
	faultsSwitch[scope]._enable.Store(true)
}

func Close(scope int) {
	if scope >= FAULTS_COUNT || scope < 0 {
		return
	}
	faultsSwitch[scope]._enable.Store(false)
	faultsSwitch[scope]._faults.Clear()
}

func Check(scope int, faultName string) *FaultAction {
	if scope >= FAULTS_COUNT || scope < 0 {
		return nil
	}
	if !faultsSwitch[scope]._enable.Load() {
		return nil
	}
	val, ok := faultsSwitch[scope]._faults.Load(faultName)
	if !ok || val == nil {
		return nil
	}
	return val.(*FaultAction)
}

func Register(scope int, faultName string, args []string, action func([]string) error) {
	if scope >= FAULTS_COUNT || scope < 0 {
		return
	}
	if !faultsSwitch[scope]._enable.Load() {
		return
	}
	faultsSwitch[scope]._faults.Store(faultName, &FaultAction{Args: args, Action: action})
}
