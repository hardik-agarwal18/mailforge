package delivery

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

const DefaultDeliveryStreamMaxLen int64 = 20000

// TrimAcknowledged removes old stream entries only when they are
// outside the retention window and safe for every consumer group.
//
// XTRIM MINID deletes IDs strictly lower than the supplied threshold.
func (c *Consumer) TrimAcknowledged(
	ctx context.Context,
	maxLen int64,
) (int64, error) {
	if maxLen < 1 {
		return 0, fmt.Errorf("maxLen must be positive")
	}

	length, err := c.client.XLen(ctx, DeliveryStream).Result()
	if err != nil {
		return 0, fmt.Errorf("get stream length: %w", err)
	}

	if length <= maxLen {
		return 0, nil
	}

	// Retrieve the newest maxLen entries.
	tail, err := c.client.XRevRangeN(
		ctx,
		DeliveryStream,
		"+",
		"-",
		maxLen,
	).Result()
	if err != nil {
		return 0, fmt.Errorf("read retention boundary: %w", err)
	}

	if int64(len(tail)) < maxLen {
		return 0, nil
	}

	// The oldest entry inside our desired retention window.
	safeBefore := tail[len(tail)-1].ID

	groups, err := c.client.XInfoGroups(
		ctx,
		DeliveryStream,
	).Result()
	if err != nil {
		return 0, fmt.Errorf("inspect consumer groups: %w", err)
	}

	// Never trim without knowing the consumer-group state.
	if len(groups) == 0 {
		return 0, nil
	}

	for _, group := range groups {
		if group.LastDeliveredID == "" {
			return 0, fmt.Errorf(
				"group %q has no delivery cursor; refusing trim",
				group.Name,
			)
		}

		// Preserve entries newer than the group's delivery cursor.
		safeBefore, err = earlierStreamID(
			safeBefore,
			group.LastDeliveredID,
		)
		if err != nil {
			return 0, err
		}

		// Preserve the oldest pending entry and everything after it.
		if group.Pending > 0 {
			pending, err := c.client.XPendingExt(
				ctx,
				&redis.XPendingExtArgs{
					Stream: DeliveryStream,
					Group:  group.Name,
					Start:  "-",
					End:    "+",
					Count:  1,
				},
			).Result()

			if err != nil {
				return 0, fmt.Errorf(
					"inspect pending entries for %q: %w",
					group.Name,
					err,
				)
			}

			// Be conservative if the pending summary and detail disagree.
			if len(pending) == 0 {
				return 0, fmt.Errorf(
					"group %q reports pending messages but none were returned; refusing trim",
					group.Name,
				)
			}

			safeBefore, err = earlierStreamID(
				safeBefore,
				pending[0].ID,
			)
			if err != nil {
				return 0, err
			}
		}
	}

	if safeBefore == "0-0" {
		return 0, nil
	}

	// Redis 7 supports MINID. Approximate trimming is efficient and
	// can retain slightly more than maxLen, which is intentional.
	removed, err := c.client.XTrimMinIDApprox(
		ctx,
		DeliveryStream,
		safeBefore,
		0,
	).Result()

	if err != nil {
		return 0, fmt.Errorf("trim delivery stream: %w", err)
	}

	return removed, nil
}

func earlierStreamID(a, b string) (string, error) {
	comparison, err := compareStreamIDs(a, b)
	if err != nil {
		return "", err
	}

	if comparison <= 0 {
		return a, nil
	}

	return b, nil
}

func compareStreamIDs(a, b string) (int, error) {
	parse := func(id string) (uint64, uint64, error) {
		parts := strings.Split(id, "-")
		if len(parts) != 2 {
			return 0, 0, fmt.Errorf("invalid Redis stream ID %q", id)
		}

		ms, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid stream ID %q: %w", id, err)
		}

		sequence, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid stream ID %q: %w", id, err)
		}

		return ms, sequence, nil
	}

	aMS, aSeq, err := parse(a)
	if err != nil {
		return 0, err
	}

	bMS, bSeq, err := parse(b)
	if err != nil {
		return 0, err
	}

	if aMS < bMS {
		return -1, nil
	}
	if aMS > bMS {
		return 1, nil
	}
	if aSeq < bSeq {
		return -1, nil
	}
	if aSeq > bSeq {
		return 1, nil
	}

	return 0, nil
}
