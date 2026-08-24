// Package common 定义全局错误码、业务异常与金额精度工具。
package common

import (
	"errors"
	"fmt"
	"net/http"
)

// 错误码字典（详细设计文档 §6 OpenAPI 错误码）
const (
	CodeSuccess                        = 0
	CodeReservationExpired             = 1001 // RESERVATION_EXPIRED 预留已过期
	CodeBudgetExhausted                = 1002 // BUDGET_EXHAUSTED 预算耗尽
	CodeEventTimeOutOfWindow           = 1003 // EVENT_TIME_OUT_OF_WINDOW 事件超出回溯窗口
	CodeInvalidReservationTransition   = 1004 // INVALID_RESERVATION_STATE_TRANSITION 预留状态非法流转
	CodeUnauthorized                   = 1005 // UNAUTHORIZED 鉴权失败
	CodeNotFound                       = 1006 // NOT_FOUND 资源不存在
	CodeDuplicateTransaction           = 1007 // DUPLICATE_TRANSACTION 事务重复提交
	CodeNoProvider                     = 1008 // NO_PROVIDER 无可用 Provider
	CodeUpstreamError                  = 1009 // UPSTREAM_ERROR 上游 LLM 返回错误
	CodeBadRequest                     = 1010 // BAD_REQUEST 参数错误
	CodeNotImplemented                 = 1011 // NOT_IMPLEMENTED MVP 未实现
	CodeIPNotAllowed                   = 1012 // IP_NOT_ALLOWED 来源 IP 不在密钥白名单
	CodeModelNotAllowed                = 1013 // MODEL_NOT_ALLOWED 模型不在密钥白名单
	CodeRateLimited                    = 1014 // RATE_LIMITED 触发速率限制（RPM/TPM）
	CodeConcurrencyLimited             = 1015 // CONCURRENCY_LIMITED 触发并发限制
	CodeKeyDisabled                    = 1016 // KEY_DISABLED 密钥已禁用或过期
	CodeCustomerNotAllowed             = 1017 // CUSTOMER_NOT_ALLOWED 请求客户与密钥绑定不符
	CodeInternal                       = 2001 // INTERNAL_ERROR 内部系统异常
)

// BizError 业务错误，携带错误码与建议 HTTP 状态码。
type BizError struct {
	Code       int
	Msg        string
	HTTPStatus int
	Err        error
}

func (e *BizError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("code=%d msg=%s: %v", e.Code, e.Msg, e.Err)
	}
	return fmt.Sprintf("code=%d msg=%s", e.Code, e.Msg)
}

func (e *BizError) Unwrap() error { return e.Err }

func NewBiz(code int, msg string, httpStatus int) *BizError {
	return &BizError{Code: code, Msg: msg, HTTPStatus: httpStatus}
}

func (e *BizError) Wrap(err error) *BizError {
	return &BizError{Code: e.Code, Msg: e.Msg, HTTPStatus: e.HTTPStatus, Err: err}
}

var (
	ErrReservationExpired           = NewBiz(CodeReservationExpired, "RESERVATION_EXPIRED", http.StatusConflict)
	ErrBudgetExhausted              = NewBiz(CodeBudgetExhausted, "BUDGET_EXHAUSTED", http.StatusPaymentRequired)
	ErrEventTimeOutOfWindow         = NewBiz(CodeEventTimeOutOfWindow, "EVENT_TIME_OUT_OF_WINDOW", http.StatusBadRequest)
	ErrInvalidReservationTransition = NewBiz(CodeInvalidReservationTransition, "INVALID_RESERVATION_STATE_TRANSITION", http.StatusConflict)
	ErrUnauthorized                 = NewBiz(CodeUnauthorized, "UNAUTHORIZED", http.StatusUnauthorized)
	ErrNotFound                     = NewBiz(CodeNotFound, "NOT_FOUND", http.StatusNotFound)
	ErrDuplicateTransaction         = NewBiz(CodeDuplicateTransaction, "DUPLICATE_TRANSACTION", http.StatusConflict)
	ErrNoProvider                   = NewBiz(CodeNoProvider, "NO_PROVIDER", http.StatusUnprocessableEntity)
	ErrUpstream                     = NewBiz(CodeUpstreamError, "UPSTREAM_ERROR", http.StatusBadGateway)
	ErrBadRequest                   = NewBiz(CodeBadRequest, "BAD_REQUEST", http.StatusBadRequest)
	ErrNotImplemented               = NewBiz(CodeNotImplemented, "NOT_IMPLEMENTED", http.StatusNotImplemented)
	ErrIPNotAllowed                 = NewBiz(CodeIPNotAllowed, "IP_NOT_ALLOWED", http.StatusForbidden)
	ErrModelNotAllowed              = NewBiz(CodeModelNotAllowed, "MODEL_NOT_ALLOWED", http.StatusForbidden)
	ErrRateLimited                  = NewBiz(CodeRateLimited, "RATE_LIMITED", http.StatusTooManyRequests)
	ErrConcurrencyLimited           = NewBiz(CodeConcurrencyLimited, "CONCURRENCY_LIMITED", http.StatusTooManyRequests)
	ErrKeyDisabled                  = NewBiz(CodeKeyDisabled, "KEY_DISABLED", http.StatusUnauthorized)
	ErrCustomerNotAllowed           = NewBiz(CodeCustomerNotAllowed, "CUSTOMER_NOT_ALLOWED", http.StatusForbidden)
	ErrInternal                     = NewBiz(CodeInternal, "INTERNAL_ERROR", http.StatusInternalServerError)
)

// FromErr 将任意 error 归一化为 BizError。
func FromErr(err error) *BizError {
	var be *BizError
	if errors.As(err, &be) {
		return be
	}
	return ErrInternal.Wrap(err)
}
