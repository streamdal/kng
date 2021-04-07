package kng

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestBuildBatch(t *testing.T) {
	perBatch := 100

	cases := map[int]int{
		23929: 240,
		4828:  49,
		1:     1,
		2:     1,
		99:    1,
		100:   1,
		101:   2,
		10000: 100,
	}

	for fullSize, expected := range cases {
		msgs := make([]kafka.Message, 0)

		for i := 0; i < fullSize; i++ {
			msgs = append(msgs, kafka.Message{})
		}

		result := buildBatch(msgs, perBatch)

		assert.Equal(t, len(result), expected)
	}
}
