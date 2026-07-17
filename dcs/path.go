package dcs

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pgsty/go-patroni/model"
)

func NamespacePrefix(namespace string) string {
	normalized := (model.Target{Namespace: namespace}).Normalize().Namespace
	return "/" + strings.Trim(normalized, "/") + "/"
}

func ClusterPrefix(target model.Target) (string, error) {
	target = target.Normalize()
	if err := target.Validate(true); err != nil {
		return "", fmt.Errorf("dcs cluster path: %w", err)
	}
	components := []string{"", strings.Trim(target.Namespace, "/"), target.Scope}
	if target.Group != nil {
		components = append(components, strconv.Itoa(*target.Group))
	}
	return strings.Join(components, "/"), nil
}

func KeyPath(target model.Target, relative string) (string, error) {
	prefix, err := ClusterPrefix(target)
	if err != nil {
		return "", err
	}
	relative = strings.Trim(relative, "/")
	if relative == "" {
		return prefix + "/", nil
	}
	return prefix + "/" + relative, nil
}
