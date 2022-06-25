package darkmq

import "time"

const (
	sleepBeforeReconnect = time.Second

	publisherMandatory = false
	publisherImmediate = false

	waitAfterConnClosed = 3 * time.Second
)
