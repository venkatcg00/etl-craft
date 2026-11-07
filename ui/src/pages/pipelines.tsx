import { TagFilter } from "@/components/tag-filter";
import { useQuery } from "@tanstack/react-query";
import { createColumnHelper } from "@tanstack/react-table";
import { Pause, Play } from "lucide-react";
import * as React from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { ActionButton } from "@/components/action-button";
import { Loading, Notice, PageHeader } from "@/components/common";
import { CsvButton } from "@/components/csv-button";
import { DataTable } from "@/components/data-table";
import { NextRun, PauseState, RecentRuns, TagList } from "@/components/pipeline-parts";
import { StatusBadge } from "@/components/status";
import { Card } from "@/components/ui/card";
import { useCanRun, useProjectInfo, useRoleAtLeast, useTimezone } from "@/hooks";
import { getJson, project } from "@/lib/api";
import { pipelinePath } from "@/lib/routes";
import type { BulkOutcome, Pipeline } from "@/lib/types";
import { useDocumentTitle } from "@/title";

const columns = createColumnHelper<Pipeline>();

export const describeOutcomes = (result: { outcomes: BulkOutcome[] }): string => {
  const refused = result.outcomes.filter((o) => o.error);
  const done = result.outcomes.length - refused.length;
  return `${done} requested${refused.length ? `; ${refused.length} refused: ${refused.map((o) => `${o.pipeline_code} (${o.error})`).join("; ")}` : ""}.`;
};

export function Pipelines() {
  const { project: code = "" } = useParams();
  const [params, setParams] = useSearchParams();
  const tags = params.getAll("tag");
  const tz = useTimezone();
  const canRun = useCanRun(code);
  const developer = useRoleAtLeast("developer");
  const info = useProjectInfo(code);
  useDocumentTitle("Pipelines", code, info.project && { type: info.project.environment_type, name: info.project.environment_name });
  const list = useQuery({ queryKey: ["pipelines", code], queryFn: () => getJson<{ pipelines: Pipeline[] }>(`${project(code)}/pipelines`), refetchInterval: 15_000 });
  const all = React.useMemo(() => list.data?.pipelines ?? [], [list.data]);
  const known = React.useMemo(() => [...new Set(all.flatMap((p) => p.tags))].sort(), [all]);
  const shown = React.useMemo(() => all.filter((p) => tags.every((t) => p.tags.includes(t))), [all, tags]);
  const [chosen, setChosen] = React.useState<ReadonlySet<string>>(new Set());
  const selected = shown.filter((p) => chosen.has(p.pipeline_code)).map((p) => p.pipeline_code);

  const toggleTag = (tag: string) => {
    const next = tags.includes(tag) ? tags.filter((t) => t !== tag) : [...tags, tag];
    setParams(new URLSearchParams(next.map((t) => ["tag", t])), { replace: true });
  };
  const bulk = (action: "PAUSE" | "RESUME", verb: string) => (
    <ActionButton<{ outcomes: BulkOutcome[] }>
      code={code}
      label={`${verb} selected`}
      title={`${verb} ${selected.length} pipeline${selected.length === 1 ? "" : "s"}`}
      effect={`${action === "PAUSE" ? "Stops new automatic runs of" : "Allows automatic runs of"}: ${selected.join(", ")}. Each pipeline succeeds or is refused on its own.`}
      endpoint="/controls/bulk"
      body={(reason) => ({ requests: selected.map((pipeline_code) => ({ pipeline_code, action, reason })) })}
      describe={describeOutcomes}
      icon={action === "PAUSE" ? <Pause className="size-4" aria-hidden /> : <Play className="size-4" aria-hidden />}
      size="sm"
    />
  );

  return (
    <div className="grid grid-cols-1 gap-4">
      <PageHeader
        title="Pipelines"
        subtitle={`${code} · ${tags.length ? `tagged ${tags.join(" and ")}` : "all pipelines"} · ${shown.length}`}
        actions={
          <div className="flex flex-wrap items-start gap-2">
          {developer ? (
            <ActionButton<unknown>
              code={code}
              label="Refresh all"
              title={`Refresh every pipeline in ${code}`}
              effect="Validates each pipeline's current definition and pins the versions that pass."
              endpoint="/pipelines/refresh"
              askReason={false}
              body={() => ({})}
              describe={() => "Every pipeline was refreshed."}
              size="sm"
            />
          ) : null}
          <CsvButton
            name={`${code}-pipelines.csv`}
            header={["pipeline", "name", "tags", "schedule", "next_run", "automatic_runs", "paused_tasks"]}
            rows={(selected.length ? shown.filter((p) => chosen.has(p.pipeline_code)) : shown).map((p) => [
              p.pipeline_code, p.pipeline_name, p.tags.join(" "), p.run_schedule ?? "", p.next_run ?? "", p.paused ? `paused by ${p.paused.paused_by}: ${p.paused.reason}` : "active", p.task_pauses.length,
            ])}
          />
          </div>
        }
      />
      <TagFilter known={known} selected={tags} onToggle={toggleTag} />
      {list.isError ? <Notice>{(list.error as Error).message}</Notice> : null}
      <Card>
        {selected.length && canRun ? (
          <div className="flex flex-wrap items-center gap-3 border-b border-border px-3 py-2">
            <span className="text-sm font-medium">{selected.length} selected</span>
            {bulk("PAUSE", "Pause")}
            {bulk("RESUME", "Resume")}
          </div>
        ) : null}
        <DataTable
          label="Pipelines"
          data={shown}
          empty={list.isSuccess ? (tags.length ? "No pipeline carries every selected tag." : "This project has no active pipeline.") : undefined}
          rowId={(p) => p.pipeline_code}
          selection={
            canRun
              ? {
                  ids: chosen,
                  onToggle: (id) => setChosen((now) => (now.has(id) ? new Set([...now].filter((x) => x !== id)) : new Set([...now, id]))),
                  onToggleAll: (every) => setChosen(every ? new Set(shown.map((p) => p.pipeline_code)) : new Set()),
                }
              : undefined
          }
          initialSort={[{ id: "pipeline", desc: false }]}
          columns={[
            columns.accessor("pipeline_code", {
              id: "pipeline",
              header: "Pipeline",
              cell: (c) => (
                <span className="grid">
                  <Link className="font-medium text-accent hover:underline" to={pipelinePath(code, c.getValue())}>
                    {c.getValue()}
                  </Link>
                  {c.row.original.pipeline_name !== c.getValue() ? <span className="text-xs text-muted">{c.row.original.pipeline_name}</span> : null}
                </span>
              ),
            }),
            columns.display({ id: "tags", header: "Tags", cell: (c) => <TagList tags={c.row.original.tags} /> }),
            columns.accessor("run_schedule", { header: "Schedule", enableSorting: false, cell: (c) => (c.getValue() ? <code className="whitespace-nowrap font-mono text-xs">{c.getValue()}</code> : <span className="whitespace-nowrap text-muted">Not scheduled</span>) }),
            columns.display({ id: "next", header: "Next run", cell: (c) => <NextRun pipeline={c.row.original} timeZone={tz} /> }),
            columns.display({ id: "recent", header: "Recent runs", cell: (c) => <RecentRuns project={code} pipeline={c.row.original} /> }),
            columns.display({
              id: "sla",
              header: "SLA",
              cell: (c) => {
                const p = c.row.original;
                const latest = p.recent_runs[0]?.sla_status;
                return latest ? <StatusBadge status={latest} /> : <span className="text-muted">{p.sla_in_hours ? "No run yet" : "—"}</span>;
              },
            }),
            columns.display({ id: "auto", header: "Automatic runs", cell: (c) => <PauseState pipeline={c.row.original} /> }),
          ]}
        />
        {list.isPending ? <Loading what="pipelines" className="p-4" /> : null}
      </Card>
    </div>
  );
}
