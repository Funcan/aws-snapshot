package awsclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
)

// IAMClient wraps the AWS IAM client with snapshot capabilities.
type IAMClient struct {
	client      *iam.Client
	statusf     StatusFunc
	concurrency int
}

// IAMOption is a functional option for configuring the IAMClient.
type IAMOption func(*iamOptions)

type iamOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithIAMStatusFunc sets a callback for progress messages.
func WithIAMStatusFunc(f StatusFunc) IAMOption {
	return func(o *iamOptions) {
		o.statusf = f
	}
}

// WithIAMConcurrency sets the maximum number of resources to process in parallel.
func WithIAMConcurrency(n int) IAMOption {
	return func(o *iamOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// IAMClient returns an IAMClient configured with the given options.
func (c *Client) IAMClient(opts ...IAMOption) *IAMClient {
	o := &iamOptions{
		concurrency: 50,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &IAMClient{
		client:      iam.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (i *IAMClient) status(format string, args ...any) {
	if i.statusf != nil {
		i.statusf(format, args...)
	}
}

// UserSummary represents key attributes of an IAM user.
type UserSummary struct {
	Name             string   `json:"name"`
	Arn              string   `json:"arn,omitempty"`
	Path             string   `json:"path,omitempty"`
	Groups           []string `json:"groups,omitempty"`
	AttachedPolicies []string `json:"attached_policies,omitempty"`
	InlinePolicies   []string `json:"inline_policies,omitempty"`
	MFAEnabled       bool     `json:"mfa_enabled"`
	HasPassword      bool     `json:"has_password"`
	AccessKeyCount   int      `json:"access_key_count"`
}

// GroupSummary represents key attributes of an IAM group.
type GroupSummary struct {
	Name             string   `json:"name"`
	Arn              string   `json:"arn,omitempty"`
	Path             string   `json:"path,omitempty"`
	AttachedPolicies []string `json:"attached_policies,omitempty"`
	InlinePolicies   []string `json:"inline_policies,omitempty"`
}

// RoleSummary represents key attributes of an IAM role.
type RoleSummary struct {
	Name                     string   `json:"name"`
	Arn                      string   `json:"arn,omitempty"`
	Path                     string   `json:"path,omitempty"`
	Description              string   `json:"description,omitempty"`
	MaxSessionDurationSecs   int32    `json:"max_session_duration_seconds,omitempty"`
	AttachedPolicies         []string `json:"attached_policies,omitempty"`
	InlinePolicies           []string `json:"inline_policies,omitempty"`
	AssumeRolePolicyDocument bool     `json:"has_assume_role_policy"`
}

// PolicySummary represents key attributes of an IAM managed policy.
type PolicySummary struct {
	Name            string `json:"name"`
	Arn             string `json:"arn,omitempty"`
	Path            string `json:"path,omitempty"`
	Description     string `json:"description,omitempty"`
	IsAWSManaged    bool   `json:"is_aws_managed"`
	AttachmentCount int32  `json:"attachment_count"`
	DefaultVersion  string `json:"default_version,omitempty"`
}

// IAMSummary represents the complete IAM snapshot.
type IAMSummary struct {
	Users    []UserSummary   `json:"users,omitempty"`
	Groups   []GroupSummary  `json:"groups,omitempty"`
	Roles    []RoleSummary   `json:"roles,omitempty"`
	Policies []PolicySummary `json:"policies,omitempty"`
}

// Summarise returns a summary of all IAM resources.
func (i *IAMClient) Summarise(ctx context.Context) (*IAMSummary, error) {
	summary := &IAMSummary{}

	// Fetch users, groups, roles, and policies in parallel
	var wg sync.WaitGroup
	var usersErr, groupsErr, rolesErr, policiesErr error
	var users []UserSummary
	var groups []GroupSummary
	var roles []RoleSummary
	var policies []PolicySummary

	wg.Add(4)

	go func() {
		defer wg.Done()
		users, usersErr = i.listUsers(ctx)
	}()

	go func() {
		defer wg.Done()
		groups, groupsErr = i.listGroups(ctx)
	}()

	go func() {
		defer wg.Done()
		roles, rolesErr = i.listRoles(ctx)
	}()

	go func() {
		defer wg.Done()
		policies, policiesErr = i.listPolicies(ctx)
	}()

	wg.Wait()

	// Check for errors
	if usersErr != nil {
		return nil, usersErr
	}
	if groupsErr != nil {
		return nil, groupsErr
	}
	if rolesErr != nil {
		return nil, rolesErr
	}
	if policiesErr != nil {
		return nil, policiesErr
	}

	summary.Users = users
	summary.Groups = groups
	summary.Roles = roles
	summary.Policies = policies

	return summary, nil
}

func (i *IAMClient) listUsers(ctx context.Context) ([]UserSummary, error) {
	i.status("Listing IAM users...")

	var users []userInfo
	var marker *string
	for {
		resp, err := i.client.ListUsers(ctx, &iam.ListUsersInput{
			Marker: marker,
		})
		if err != nil {
			return nil, err
		}

		for _, user := range resp.Users {
			users = append(users, userInfo{
				name: aws.ToString(user.UserName),
				arn:  aws.ToString(user.Arn),
				path: aws.ToString(user.Path),
			})
		}

		if !resp.IsTruncated {
			break
		}
		marker = resp.Marker
	}

	total := len(users)
	i.status("Found %d users, processing with concurrency %d", total, i.concurrency)

	if total == 0 {
		return []UserSummary{}, nil
	}

	summaries := make([]UserSummary, total)
	var processed atomic.Int64
	var errMu sync.Mutex
	var firstErr error

	workCh := make(chan int, total)
	for idx := range users {
		workCh <- idx
	}
	close(workCh)

	var wg sync.WaitGroup
	for w := 0; w < i.concurrency && w < total; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				user := users[idx]
				summary, err := i.describeUser(ctx, user)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					continue
				}
				summaries[idx] = summary

				n := processed.Add(1)
				i.status("[%d/%d] Processed user: %s", n, total, user.name)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	return summaries, nil
}

type userInfo struct {
	name string
	arn  string
	path string
}

func (i *IAMClient) describeUser(ctx context.Context, user userInfo) (UserSummary, error) {
	summary := UserSummary{
		Name: user.name,
		Arn:  user.arn,
		Path: user.path,
	}

	// Get groups
	groupsResp, err := i.client.ListGroupsForUser(ctx, &iam.ListGroupsForUserInput{
		UserName: &user.name,
	})
	if err != nil {
		return UserSummary{}, fmt.Errorf("list groups for user %s: %w", user.name, err)
	}
	for _, g := range groupsResp.Groups {
		summary.Groups = append(summary.Groups, aws.ToString(g.GroupName))
	}

	// Get attached policies
	attachedResp, err := i.client.ListAttachedUserPolicies(ctx, &iam.ListAttachedUserPoliciesInput{
		UserName: &user.name,
	})
	if err != nil {
		return UserSummary{}, fmt.Errorf("list attached policies for user %s: %w", user.name, err)
	}
	for _, p := range attachedResp.AttachedPolicies {
		summary.AttachedPolicies = append(summary.AttachedPolicies, aws.ToString(p.PolicyName))
	}

	// Get inline policies
	inlineResp, err := i.client.ListUserPolicies(ctx, &iam.ListUserPoliciesInput{
		UserName: &user.name,
	})
	if err != nil {
		return UserSummary{}, fmt.Errorf("list inline policies for user %s: %w", user.name, err)
	}
	summary.InlinePolicies = inlineResp.PolicyNames

	// Check MFA devices
	mfaResp, err := i.client.ListMFADevices(ctx, &iam.ListMFADevicesInput{
		UserName: &user.name,
	})
	if err != nil {
		return UserSummary{}, fmt.Errorf("list MFA devices for user %s: %w", user.name, err)
	}
	if len(mfaResp.MFADevices) > 0 {
		summary.MFAEnabled = true
	}

	// Check login profile (password) - NoSuchEntity is expected for users without console access
	_, err = i.client.GetLoginProfile(ctx, &iam.GetLoginProfileInput{
		UserName: &user.name,
	})
	if err == nil {
		summary.HasPassword = true
	}

	// Count access keys
	keysResp, err := i.client.ListAccessKeys(ctx, &iam.ListAccessKeysInput{
		UserName: &user.name,
	})
	if err != nil {
		return UserSummary{}, fmt.Errorf("list access keys for user %s: %w", user.name, err)
	}
	summary.AccessKeyCount = len(keysResp.AccessKeyMetadata)

	return summary, nil
}

func (i *IAMClient) listGroups(ctx context.Context) ([]GroupSummary, error) {
	i.status("Listing IAM groups...")

	var groups []groupInfo
	var marker *string
	for {
		resp, err := i.client.ListGroups(ctx, &iam.ListGroupsInput{
			Marker: marker,
		})
		if err != nil {
			return nil, err
		}

		for _, group := range resp.Groups {
			groups = append(groups, groupInfo{
				name: aws.ToString(group.GroupName),
				arn:  aws.ToString(group.Arn),
				path: aws.ToString(group.Path),
			})
		}

		if !resp.IsTruncated {
			break
		}
		marker = resp.Marker
	}

	total := len(groups)
	i.status("Found %d groups", total)

	if total == 0 {
		return []GroupSummary{}, nil
	}

	summaries := make([]GroupSummary, total)
	var processed atomic.Int64
	var errMu sync.Mutex
	var firstErr error

	workCh := make(chan int, total)
	for idx := range groups {
		workCh <- idx
	}
	close(workCh)

	var wg sync.WaitGroup
	for w := 0; w < i.concurrency && w < total; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				group := groups[idx]
				summary, err := i.describeGroup(ctx, group)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					continue
				}
				summaries[idx] = summary

				n := processed.Add(1)
				i.status("[%d/%d] Processed group: %s", n, total, group.name)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	return summaries, nil
}

type groupInfo struct {
	name string
	arn  string
	path string
}

func (i *IAMClient) describeGroup(ctx context.Context, group groupInfo) (GroupSummary, error) {
	summary := GroupSummary{
		Name: group.name,
		Arn:  group.arn,
		Path: group.path,
	}

	// Get attached policies
	attachedResp, err := i.client.ListAttachedGroupPolicies(ctx, &iam.ListAttachedGroupPoliciesInput{
		GroupName: &group.name,
	})
	if err != nil {
		return GroupSummary{}, fmt.Errorf("list attached policies for group %s: %w", group.name, err)
	}
	for _, p := range attachedResp.AttachedPolicies {
		summary.AttachedPolicies = append(summary.AttachedPolicies, aws.ToString(p.PolicyName))
	}

	// Get inline policies
	inlineResp, err := i.client.ListGroupPolicies(ctx, &iam.ListGroupPoliciesInput{
		GroupName: &group.name,
	})
	if err != nil {
		return GroupSummary{}, fmt.Errorf("list inline policies for group %s: %w", group.name, err)
	}
	summary.InlinePolicies = inlineResp.PolicyNames

	return summary, nil
}

func (i *IAMClient) listRoles(ctx context.Context) ([]RoleSummary, error) {
	i.status("Listing IAM roles...")

	var roles []roleInfo
	var marker *string
	for {
		resp, err := i.client.ListRoles(ctx, &iam.ListRolesInput{
			Marker: marker,
		})
		if err != nil {
			return nil, err
		}

		for _, role := range resp.Roles {
			roles = append(roles, roleInfo{
				name:                aws.ToString(role.RoleName),
				arn:                 aws.ToString(role.Arn),
				path:                aws.ToString(role.Path),
				description:         aws.ToString(role.Description),
				maxSessionDuration:  aws.ToInt32(role.MaxSessionDuration),
				hasAssumeRolePolicy: role.AssumeRolePolicyDocument != nil,
			})
		}

		if !resp.IsTruncated {
			break
		}
		marker = resp.Marker
	}

	total := len(roles)
	i.status("Found %d roles, processing with concurrency %d", total, i.concurrency)

	if total == 0 {
		return []RoleSummary{}, nil
	}

	summaries := make([]RoleSummary, total)
	var processed atomic.Int64
	var errMu sync.Mutex
	var firstErr error

	workCh := make(chan int, total)
	for idx := range roles {
		workCh <- idx
	}
	close(workCh)

	var wg sync.WaitGroup
	for w := 0; w < i.concurrency && w < total; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				role := roles[idx]
				summary, err := i.describeRole(ctx, role)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					continue
				}
				summaries[idx] = summary

				n := processed.Add(1)
				i.status("[%d/%d] Processed role: %s", n, total, role.name)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	return summaries, nil
}

type roleInfo struct {
	name                string
	arn                 string
	path                string
	description         string
	maxSessionDuration  int32
	hasAssumeRolePolicy bool
}

func (i *IAMClient) describeRole(ctx context.Context, role roleInfo) (RoleSummary, error) {
	summary := RoleSummary{
		Name:                     role.name,
		Arn:                      role.arn,
		Path:                     role.path,
		Description:              role.description,
		MaxSessionDurationSecs:   role.maxSessionDuration,
		AssumeRolePolicyDocument: role.hasAssumeRolePolicy,
	}

	// Get attached policies
	attachedResp, err := i.client.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{
		RoleName: &role.name,
	})
	if err != nil {
		return RoleSummary{}, fmt.Errorf("list attached policies for role %s: %w", role.name, err)
	}
	for _, p := range attachedResp.AttachedPolicies {
		summary.AttachedPolicies = append(summary.AttachedPolicies, aws.ToString(p.PolicyName))
	}

	// Get inline policies
	inlineResp, err := i.client.ListRolePolicies(ctx, &iam.ListRolePoliciesInput{
		RoleName: &role.name,
	})
	if err != nil {
		return RoleSummary{}, fmt.Errorf("list inline policies for role %s: %w", role.name, err)
	}
	summary.InlinePolicies = inlineResp.PolicyNames

	return summary, nil
}

func (i *IAMClient) listPolicies(ctx context.Context) ([]PolicySummary, error) {
	i.status("Listing IAM policies (customer managed)...")

	var policies []PolicySummary
	var marker *string
	for {
		resp, err := i.client.ListPolicies(ctx, &iam.ListPoliciesInput{
			Scope:  "Local", // Only customer managed policies
			Marker: marker,
		})
		if err != nil {
			return nil, err
		}

		for _, policy := range resp.Policies {
			policies = append(policies, PolicySummary{
				Name:            aws.ToString(policy.PolicyName),
				Arn:             aws.ToString(policy.Arn),
				Path:            aws.ToString(policy.Path),
				Description:     aws.ToString(policy.Description),
				IsAWSManaged:    false,
				AttachmentCount: aws.ToInt32(policy.AttachmentCount),
				DefaultVersion:  aws.ToString(policy.DefaultVersionId),
			})
		}

		if !resp.IsTruncated {
			break
		}
		marker = resp.Marker
	}

	i.status("Found %d customer managed policies", len(policies))
	return policies, nil
}
