package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/telemetry"
)

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
	// A client-supplied Idempotency-Key identifies a logical submission across
	// retries. When absent, the generated sample ID serves as the key for this
	// attempt only.
	key := request.Header.Get("Idempotency-Key")
	sample := telemetry.Sample{
		ID: s.services.IDs.New("sample"), AssetID: repeaterID, Kind: telemetry.Kind(input.Kind),
		Value: input.Value, Unit: input.Unit, Sequence: input.Sequence, Generation: input.Generation,
		ObservedAt: s.services.Clock.Now().UTC(),
	}
	if key == "" {
		key = sample.ID
	}
	if err := sample.Validate(); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	result, err := s.services.IngestTelemetry(request.Context(), key, sample)
	if err != nil {
		switch {
		case errors.Is(err, ErrIdempotencyConflict), errors.Is(err, telemetry.ErrSamplePending):
			writeError(writer, http.StatusConflict, err)
		case result.Sample.ID == "":
			// The durable write never happened: there is no retry handle.
			writeError(writer, http.StatusInternalServerError, err)
		default:
			// The sample is durably persisted but not yet projected. Surface
			// the sample ID so the client can retry with it as the
			// Idempotency-Key instead of producing a duplicate inbound.
			writeJSON(writer, http.StatusServiceUnavailable, map[string]any{
				"error":           err.Error(),
				"sample_id":       result.Sample.ID,
				"idempotency_key": key,
			})
		}
		return
	}
	// 200 for an idempotent replay, 202 for a sample that just crossed the
	// projection/commit boundary for the first time.
	status := http.StatusAccepted
	if !result.Stored || result.Replayed {
		status = http.StatusOK
	}
	writeJSON(writer, status, result.Sample)
}

func (s *Server) getRepeater(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Repeater(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, target.Snapshot())
}
