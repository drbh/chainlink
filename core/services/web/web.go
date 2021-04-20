package web

import (
	"github.com/smartcontractkit/chainlink/core/gracefulpanic"
	log "github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/services/job"
	"github.com/smartcontractkit/chainlink/core/services/pipeline"
	"github.com/smartcontractkit/chainlink/core/utils"
)

// WebJob runs a cron jobSpec from a CronSpec
type WebJob struct {
	chDone       chan struct{}
	chStop       chan struct{}
	jobID        int32
	logger       *log.Logger
	runner       pipeline.Runner
	pipelineSpec pipeline.Spec
}

// NewFromJobSpec() - instantiates a WEbJob singleton to execute the given job pipelineSpec
func NewFromJobSpec(
	jobSpec job.Job,
	runner pipeline.Runner,
) (*WebJob, error) {
	pipelineSpec := jobSpec.PipelineSpec

	logger := log.CreateLogger(
		log.Default.With(
			"jobID", jobSpec.ID,
		),
	)

	chDone := make(chan struct{})
	chStop := make(chan struct{})

	return &WebJob{chDone, chStop, jobSpec.ID, logger, runner, *pipelineSpec}, nil
}

// Start implements the job.Service interface.
func (web *WebJob) Start() error {
	web.logger.Debug("WebJob: Start")
	go gracefulpanic.WrapRecover(func() {
		defer close(web.chDone)
		web.runWebPipeline()
	})

	return nil
}

// Close implements the job.Service interface. It stops this instance from
// polling, cleaning up resources.
func (web *WebJob) Close() error {
	web.logger.Debug("WebJob: Closing")
	close(web.chStop)
	<-web.chDone

	return nil
}

func (web *WebJob) runWebPipeline() {
	ctx, cancel := utils.ContextFromChan(web.chStop)
	defer cancel()
	_, _, err := web.runner.ExecuteAndInsertFinishedRun(ctx, web.pipelineSpec, pipeline.JSONSerializable{}, *web.logger, true)
	if err != nil {
		web.logger.Errorf("Error executing new runWebRequest for jobSpec ID %v", web.jobID)
	}
}
