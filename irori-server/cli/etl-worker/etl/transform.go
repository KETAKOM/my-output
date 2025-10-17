package etl

import (
	"context"
	"strings"
)

// ====== Transform ======
func Transformer(ctx context.Context, in <-chan Record, out chan<- Record) {
	for {
		select {
		case <-ctx.Done():
			return
		case r, ok := <-in:
			if !ok {
				return
			}
			r.Message = strings.ToUpper(r.Message)
			out <- r
		}
	}
}
