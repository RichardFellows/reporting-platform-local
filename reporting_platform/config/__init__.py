"""The platform's configuration, and the CLI that resolves it.

The YAML here is the input; `python -m reporting_platform.config` is how you
ask what it resolves to. A package rather than a bare directory of data
precisely so that command exists -- reading the tree by eye stopped being
sufficient when the registry became three tiers across three places.
"""
