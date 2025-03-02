package knats_test

import (
	"fmt"

	"git.kanosolution.net/kano/kaos"
	"github.com/sebarcode/codekit"
)

const (
	HttpWriter     string = "http_writer"
	HttpRequest    string = "http_request"
	JwtReferenceID string = "jwt_reference_id"
	JwtData        string = "jwt_data"
)

func PrepareCtxData(ctx *kaos.Context, userid, companyid string) *kaos.Context {
	if userid != "" {
		ctx.Data().Set(JwtReferenceID, userid)
	}

	if companyid == "" {
		companyid = "Demo00"
	}
	ctx.Data().Set(JwtData, codekit.M{}.Set("CompanyID", companyid))
	return ctx
}

func InvokeAPI[M, R any](svc *kaos.Service, uriPath string, payload M, respond R, userid, coid string) (R, error) {
	if svc == nil {
		return respond, fmt.Errorf("missing: service, source: invokeAPI %s", uriPath)
	}

	sr := svc.GetRoute(uriPath)
	if sr == nil {
		return respond, fmt.Errorf("missing: route: %s", uriPath)
	}
	ctx := PrepareCtxData(kaos.NewContextFromService(svc, sr), userid, coid)
	e := svc.CallTo(uriPath, respond, ctx, payload)
	return respond, e
}
