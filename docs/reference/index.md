# Reference

Generated from the code, so they always match the release:

- [Command line](cli.md): the global options and every command, from the parser.
- [craft-connector.yml](configuration.md): every section and setting the loader accepts, and
  where each is explained.
- [Task parameters](task-parameters.md): every `CFG_TASK_PARAMETERS` name, per handler.
- [Engine DB schema](engine-db-schema.md): every table and column, with allowed values and
  unique keys.
- [Python API](../api/etl_craft/index.md): scripting contracts, configuration, errors and browsable module references.

And:

- [Exit codes](exit-codes.md): what each `etl-craft` exit status means.

Project-owned commands require `--project` or `Orchestration.Project` in the loaded
`craft-connector.yml`; both must agree when present. There is no implicit `default` project.
Deployment commands (accounts, groups, pools and `project list`) need no project selection.
See [Project layout](../deploying/project-layout.md#several-projects-in-one-repository).

Audit hardening adds `token create --pool P` (required only for agent tokens), a default
`--expires 365d`, and pool/last-used metadata in `token list`. Setup accepts
`--admin-name N --password-stdin` only for first-admin provisioning; stdin must be a pipe.
