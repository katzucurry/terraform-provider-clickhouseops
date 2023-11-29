---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "clickhouse_namedcollection Resource - terraform-provider-clickhouse"
subcategory: ""
description: |-
  Clickhouse Named Collection to store secret
---

# clickhouse_namedcollection (Resource)

Clickhouse Named Collection to store secret



<!-- schema generated by tfplugindocs -->
## Schema

### Required

- `keyvaluepairs` (Attributes List) Clickhouse Named Collection Key-Value Pairs (see [below for nested schema](#nestedatt--keyvaluepairs))
- `name` (String) Clickhouse Name Collection Name

### Optional

- `cluster_name` (String) Clickhouse Cluster Name
- `sensitive_keyvaluepairs` (Attributes List, Sensitive) Clickhouse Named Collection Sensitive Key-Value Pairs (see [below for nested schema](#nestedatt--sensitive_keyvaluepairs))

### Read-Only

- `id` (String) The ID of this resource.

<a id="nestedatt--keyvaluepairs"></a>
### Nested Schema for `keyvaluepairs`

Required:

- `key` (String) Clickhouse Named Collection Key
- `value` (String) Clickhouse Named Collection Value

Optional:

- `is_not_overridable` (Boolean) Clickhouse Named Collection Disable Overridden


<a id="nestedatt--sensitive_keyvaluepairs"></a>
### Nested Schema for `sensitive_keyvaluepairs`

Required:

- `key` (String) Clickhouse Named Collection Key
- `value` (String) Clickhouse Named Collection Value

Optional:

- `is_not_overridable` (Boolean) Clickhouse Named Collection Disable Overridden