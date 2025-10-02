package types

import "github.com/collibra/go-bexpression"

type ExportedAccessControl struct {
	Id                string                                `yaml:"id" json:"id"`
	Name              string                                `yaml:"name" json:"name"`
	Description       string                                `yaml:"description" json:"description"`
	NamingHint        string                                `yaml:"namingHint" json:"namingHint"`
	ExternalId        *string                               `yaml:"externalId" json:"externalId"`
	Who               ExportedWhoItem                       `yaml:"who" json:"who"`
	DeletedWho        *ExportedWhoItem                      `yaml:"deletedWho" json:"deletedWho"`
	Action            AccessControlAction                   `yaml:"action" json:"action"`
	Delete            bool                                  `yaml:"delete" json:"delete"`
	WhoLocked         bool                                  `yaml:"whoLocked" json:"whoLocked"`
	WhatLocked        bool                                  `yaml:"whatLocked" json:"whatLocked"`
	InheritanceLocked bool                                  `yaml:"inheritanceLocked" json:"inheritanceLocked"`
	DeleteLocked      bool                                  `yaml:"deleteLocked" json:"deleteLocked"`
	ActualName        *string                               `yaml:"actualName" json:"actualName"`
	What              []ExportedWhatItem                    `yaml:"what" json:"what"`
	DeleteWhat        []ExportedWhatItem                    `yaml:"deleteWhat" json:"deleteWhat"`
	Type              *string                               `yaml:"type" json:"type,omitempty"`
	FilterCriteria    *bexpression.DataComparisonExpression `yaml:"filterCriteria,omitempty" json:"filterCriteria,omitempty"`
	PolicyRule        *string                               `yaml:"policyRule,omitempty" json:"policyRule,omitempty"`
	CommonDataObject  *string                               `yaml:"commonDataObject,omitempty" json:"commonDataObject,omitempty"`
	Owners            []ExportedAccessOwner                 `yaml:"owners" json:"owners"`
}

type ExportedAccessOwner struct {
	Email             *string `json:"email,omitempty"`
	AccountName       *string `json:"accountName,omitempty"`
	AccessControlName *string `json:"accessControlName,omitempty"`
}

type ExportedWhoItem struct {
	Users       []string `yaml:"users,omitempty" json:"users,omitempty"`
	InheritFrom []string `yaml:"inheritFrom,omitempty" json:"inheritFrom,omitempty"`
	Recipients  []string `yaml:"recipients,omitempty" json:"recipients,omitempty"`

	InheritedFromIds []string `yaml:"-" json:"-"`
	Groups           []string `yaml:"-" json:"-"`
}

type ExportedWhatItem struct {
	DataObject  YDataObjectReference `yaml:"dataObject" json:"dataObject"`
	Permissions []string             `yaml:"permissions" json:"permissions"`
}

type YDataObjectReference struct {
	FullName string `json:"fullName" yaml:"fullName"`
	Id       string `json:"id" yaml:"id"`
	Type     string `json:"type" yaml:"type"`
}
