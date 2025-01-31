package keeper

import (
	"github.com/smartcontractkit/chainlink/core/store/models"
)

func (rs *RegistrySynchronizer) ExportedFullSync() {
	rs.fullSync()
}

func (rs *RegistrySynchronizer) ExportedProcessLogs(head models.Head) {
	rs.processLogs(head)
}
