package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/telemetry"
)

// idempotencyKeyHeader carries the client-supplied retry correlation key.
// Absent it, a deterministic key is derived from asset, kind and sequence.
const idempotencyKeyHeader = "Idempotency-Key"

// TelemetryAck is the trackable receipt for an accepted ingestion.
type TelemetryAck struct {
	IngestionID string                    `json:"ingestion_id"`
	SampleID    string                    `json:"sample_id"`
	Status      telemetry.IngestionStatus `json:"status"`
	Replayed    bool                      `json:"replayed"`
	Attempts    int                       `json:"attempts"`
	Sample      telemetry.Sample          `json:"sample"`
}

type telemetryErrorResponse struct {
	Error       string `json:"error"`
	IngestionID string `json:"ingestion_id,omitempty"`
	Stage       string `json:"stage,omitempty"`
	Attempts    int    `json:"attempts,omitempty"`
}

func (s *Server) ingestTelemetry(writer http.ResponseWriter, request *http.Request) {
	repeaterID := chi.URLParam(request, "id")
	if _, err := s.services.Repeater(repeaterID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input TelemetryRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	sample := telemetry.Sample{
		ID: s.services.IDs.New("sample"), AssetID: repeaterID, Kind: telemetry.Kind(input.Kind),
		Value: input.Value, Unit: input.Unit, Sequence: input.Sequence, Generation: input.Generation,
		ObservedAt: s.services.Clock.Now().UTC(),
	}
	key := request.Header.Get(idempotencyKeyHeader)
	if key == "" {
		key = telemetry.IngestionKey(sample)
	}

	ingestion, replayed, err := s.services.Telemetry.Ingest(request.Context(), sample, key)
	if err != nil {
		s.writeIngestionError(writer, ingestion, err)
		return
	}

	ack := TelemetryAck{
		IngestionID: ingestion.Key, SampleID: ingestion.Sample.ID, Status: ingestion.Status,
		Replayed: replayed, Attempts: ingestion.Attempts, Sample: ingestion.Sample,
	}
	// A replayed request never re-projects; a fresh ingestion is accepted once
	// the sample is durably received and every projection has committed.
	status := http.StatusAccepted
	if replayed {
		status = http.StatusOK
	}
	writeJSON(writer, status, ack)
}

func (s *Server) writeIngestionError(writer http.ResponseWriter, ingestion telemetry.Ingestion, err error) {
	response := telemetryErrorResponse{
		Error: err.Error(), IngestionID: ingestion.Key, Attempts: ingestion.Attempts,
		Stage: string(telemetry.StageAdmit),
	}
	status := http.StatusInternalServerError

	var ingestionErr *telemetry.IngestionError
	if errors.As(err, &ingestionErr) {
		response.IngestionID = ingestionErr.Key
		response.Attempts = ingestionErr.Attempts
		response.Stage = string(ingestionErr.Stage)
		switch {
		case ingestionErr.Stage == telemetry.StageAdmit &&
			(errors.Is(ingestionErr, telemetry.ErrInFlight) || errors.Is(ingestionErr, telemetry.ErrIngestionConflict)):
			status = http.StatusConflict
		case ingestionErr.Stage == telemetry.StageAdmit:
			status = http.StatusBadRequest
		case ingestionErr.Stage == telemetry.StagePersist:
			status = http.StatusServiceUnavailable
		case ingestionErr.Stage == telemetry.StageProjection:
			status = http.StatusBadGateway
		case ingestionErr.Stage == telemetry.StageConfirm && ingestionErr.Status == telemetry.IngestionConfirmed:
			// Projection committed and history holds the sample; only the
			// durable confirmation record failed. It remains effectively
			// accepted and a retry will short-circuit as a replay.
			writeJSON(writer, http.StatusAccepted, TelemetryAck{
				IngestionID: ingestionErr.Key, SampleID: ingestionErr.SampleID,
				Status: telemetry.IngestionConfirmed, Attempts: ingestionErr.Attempts,
			})
			return
		}
	}
	writeJSON(writer, status, response)
}

func (s *Server) getRepeater(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Repeater(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, target.Snapshot())
}
