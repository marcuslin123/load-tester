package orchestrator

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/config"
	"google.golang.org/protobuf/types/known/durationpb"
)

// splitLoad returns one complete slice per worker, including zero-load slices
// that explicitly keep surplus workers idle.
func splitLoad(load config.Load, workerIDs []string) (map[string]*loadtestv1.LoadSlice, error) {
	if len(workerIDs) == 0 {
		return nil, errors.New("at least one worker is required")
	}
	workers := slices.Clone(workerIDs)
	slices.Sort(workers)
	for index, workerID := range workers {
		if strings.TrimSpace(workerID) == "" {
			return nil, errors.New("worker ID is required")
		}
		if index > 0 && workerID == workers[index-1] {
			return nil, fmt.Errorf("duplicate worker ID %q", workerID)
		}
	}

	result := make(map[string]*loadtestv1.LoadSlice, len(workers))
	for _, workerID := range workers {
		result[workerID] = &loadtestv1.LoadSlice{RampUp: durationpb.New(load.RampUp)}
	}

	switch load.Model {
	case config.LoadConstantVUs:
		if load.VirtualUsers <= 0 {
			return nil, errors.New("virtual users must be greater than zero")
		}
		active := min(len(workers), load.VirtualUsers)
		shares := divide(load.VirtualUsers, active)
		for index, workerID := range workers {
			result[workerID].Model = loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_VUS
			if index < active {
				result[workerID].VirtualUsers = uint64(shares[index])
			}
		}
	case config.LoadConstantRate:
		if load.Rate <= 0 {
			return nil, errors.New("rate must be greater than zero")
		}
		if load.MaxInFlight <= 0 {
			return nil, errors.New("max in flight must be greater than zero")
		}
		active := min(len(workers), load.Rate, load.MaxInFlight)
		rates := divide(load.Rate, active)
		limits := divide(load.MaxInFlight, active)
		for index, workerID := range workers {
			result[workerID].Model = loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_RATE
			if index < active {
				result[workerID].Rate = uint64(rates[index])
				result[workerID].MaxInFlight = uint64(limits[index])
			}
		}
	default:
		return nil, fmt.Errorf("unsupported load model %q", load.Model)
	}
	return result, nil
}

func divide(total, parts int) []int {
	shares := make([]int, parts)
	base := total / parts
	remainder := total % parts
	for index := range shares {
		shares[index] = base
		if index < remainder {
			shares[index]++
		}
	}
	return shares
}
