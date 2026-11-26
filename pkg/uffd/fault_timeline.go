package uffd

import "os"

var faultTimelineEnabled = os.Getenv("KUASAR_REMOTE_FAULT_TRACE") == "1"
