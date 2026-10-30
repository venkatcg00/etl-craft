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


Project administration also supports `--environment-type DEV|SIT|UAT|PROD|SANDBOX|CUSTOM`,
`--environment-name LABEL` and `--governance STRICT|RELAXED` on `project add|set`.
`project list` includes these fields and an informational shared-warehouse indicator.
Only deployment admins set them, with a non-empty `--reason`; `PROD` requires `STRICT`.
Intervention `--reason` is checked by the selected project's governance policy, equally through
CLI and API. See [environments and governance](../deploying/project-layout.md#environments-and-governance).

Schema checks compare a project's declared tables with its warehouse.
`--schema-check-schedule CRON` on `project add|set` has the controller run the comparison (and
rebuild the catalog) at each tick in the project's timezone; `none` stops it. Missed ticks are
not replayed. `--schema-check-on-demand N` refuses `generate-docs --with-warehouse` and the
Insights page's **Check now** for that project, so its warehouse is compared only on schedule;
the default is `Y`. Insights shows the newest stored check and its time to whole-project viewers.
