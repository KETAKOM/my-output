package env

import (
	"os"
)

func IsPRD() bool {
	return os.Getenv("ENV") == "production"
}
