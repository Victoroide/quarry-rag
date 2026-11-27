package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	supersededListMethod  = "/VectorService/List"
	supersededFetchMethod = "/VectorService/Fetch"
)

func validateSupersededReadNamespace(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if err := invoke(ctx, method, req, reply, cc, opts...); err != nil {
		return err
	}
	if method != supersededListMethod && method != supersededFetchMethod {
		return nil
	}
	// The SDK wrappers replace the wire namespace with the requested one.
	response, ok := reply.(interface{ GetNamespace() string })
	if !ok || !isDefaultVectorNamespace(response.GetNamespace()) {
		return status.Errorf(codes.DataLoss, "%s reply is outside the bound default namespace", method)
	}
	return nil
}

func fetchSupersededOwners(ctx context.Context, index vectorIndex, ids []string, owners map[string]string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	response, err := index.FetchVectors(ctx, ids)
	if err != nil {
		if !isSupersededFetchReceiveSizeError(err) {
			return fmt.Errorf("fetch active vector metadata: %w", err)
		}
		if len(ids) == 1 {
			return fmt.Errorf("fetch active vector metadata for %q exceeds the receive limit even by itself (reducing -page cannot help this record): %w", ids[0], err)
		}
		middle := len(ids) / 2
		if err := fetchSupersededOwners(ctx, index, ids[:middle], owners); err != nil {
			return err
		}
		return fetchSupersededOwners(ctx, index, ids[middle:], owners)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if response == nil || !isDefaultVectorNamespace(response.Namespace) {
		return errors.New("invalid vector fetch for the bound default namespace")
	}
	requested := make(map[string]bool, len(ids))
	for _, id := range ids {
		requested[id] = true
	}
	for id, vector := range response.Vectors {
		if !requested[id] || vector == nil || vector.Id != id {
			return errors.New("vector fetch returned an unexpected record")
		}
	}
	for id, vector := range response.Vectors {
		if doc := supersededVectorDocument(id, vector); doc != "" {
			owners[id] = doc
		}
	}
	return nil
}

// These diagnostics and the default 4 MiB receive limit follow grpc-go v1.85.0.
// TestSupersededFetchPageSplitsByReceiveSize checks the SDK transport contract.
const supersededFetchReceiveLimit = 4 * 1024 * 1024

var (
	supersededFetchSizeWithLength = regexp.MustCompile(`^grpc: (?:received message|message after decompression) larger than max \(([1-9][0-9]*) vs\. ([1-9][0-9]*)\)$`)
	supersededFetchSizeLimitOnly  = regexp.MustCompile(`^grpc: received message after decompression larger than max ([1-9][0-9]*)$`)
)

func isSupersededFetchReceiveSizeError(err error) bool {
	direct, ok := err.(interface{ GRPCStatus() *status.Status })
	if !ok {
		return false
	}
	st := direct.GRPCStatus()
	if st.Code() != codes.ResourceExhausted || len(st.Details()) != 0 {
		return false
	}
	if match := supersededFetchSizeWithLength.FindStringSubmatch(st.Message()); match != nil {
		size, sizeErr := strconv.ParseUint(match[1], 10, 64)
		limit, limitErr := strconv.ParseUint(match[2], 10, 64)
		return sizeErr == nil && limitErr == nil && limit == supersededFetchReceiveLimit && size > limit
	}
	if match := supersededFetchSizeLimitOnly.FindStringSubmatch(st.Message()); match != nil {
		limit, err := strconv.ParseUint(match[1], 10, 64)
		return err == nil && limit == supersededFetchReceiveLimit
	}
	return false
}
