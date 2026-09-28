package httpserver

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/store"
)

type keyView struct {
	ID               int64  `json:"id"`
	Name             string `json:"name"`
	KeyPrefix        string `json:"key_prefix"`
	QuotaBalance     int64  `json:"quota_balance"`
	QuotaGranted     int64  `json:"quota_granted"`
	RPMLimit         int    `json:"rpm_limit"`
	TPMLimit         int    `json:"tpm_limit"`
	ConcurrencyLimit int    `json:"concurrency_limit"`
	Enabled          bool   `json:"enabled"`
}

type createKeyRequest struct {
	Name             string `json:"name"`
	Quota            int64  `json:"quota"`
	RPMLimit         int    `json:"rpm_limit"`
	TPMLimit         int    `json:"tpm_limit"`
	ConcurrencyLimit int    `json:"concurrency_limit"`
}

func (s *Server) createKey(w http.ResponseWriter, r *http.Request) {
	var body createKeyRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_json", "请求体不是合法的 JSON")
		return
	}
	if body.Name == "" || body.Quota < 0 || body.RPMLimit < 0 || body.TPMLimit < 0 || body.ConcurrencyLimit < 0 {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_request", "name 必填，额度和限额不能为负")
		return
	}
	raw, key, err := s.store.CreateKey(r.Context(), body.Name, body.Quota, body.RPMLimit, body.TPMLimit, body.ConcurrencyLimit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "create_key_failed", "创建密钥失败")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"key":  raw,
		"item": toView(key),
	})
}

func (s *Server) getKey(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_id", "密钥 id 不合法")
		return
	}
	key, err := s.store.GetKey(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "invalid_request_error", "key_not_found", "密钥不存在")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "read_failed", "读取密钥失败")
		return
	}
	writeJSON(w, http.StatusOK, toView(key))
}

func (s *Server) topup(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_id", "密钥 id 不合法")
		return
	}
	var body struct {
		Amount int64 `json:"amount"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil || body.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_amount", "amount 必须为正整数")
		return
	}
	key, err := s.store.TopUp(r.Context(), id, body.Amount)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "invalid_request_error", "key_not_found", "密钥不存在")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "topup_failed", "充值失败")
		return
	}
	writeJSON(w, http.StatusOK, toView(key))
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_id", "密钥 id 不合法")
		return
	}
	rec, err := s.billing.Reconcile(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "invalid_request_error", "key_not_found", "密钥不存在或对账失败")
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) reloadChannels(w http.ResponseWriter, r *http.Request) {
	channels, err := s.store.ListChannels(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "reload_failed", "读取渠道失败")
		return
	}
	s.router.SetChannels(channels)
	writeJSON(w, http.StatusOK, map[string]any{"channels": len(channels)})
}

func toView(k domain.APIKey) keyView {
	return keyView{
		ID:               k.ID,
		Name:             k.Name,
		KeyPrefix:        k.KeyPrefix,
		QuotaBalance:     k.QuotaBalance,
		QuotaGranted:     k.QuotaGranted,
		RPMLimit:         k.RPMLimit,
		TPMLimit:         k.TPMLimit,
		ConcurrencyLimit: k.ConcurrencyLimit,
		Enabled:          k.Enabled,
	}
}
