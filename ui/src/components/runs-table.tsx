import { useInfiniteQuery } from "@tanstack/react-query";
import * as React from "react";
import { Link } from "react-router-dom";
import { Loading, Notice } from "@/components/common";
import { createColumnHelper } from "@tanstack/react-table";
import { DataTable } from "@/components/data-table";
import { StatusBadge } from "@/components/status";
import { Card } from "@/components/ui/card";
import { useTimezone } from "@/hooks";
import { getJson, project } from "@/lib/api";
import { formatDuration, formatTime } from "@/lib/format";
import { pipelineRunPath, runPath } from "@/lib/routes";
import type { History, RunView } from "@/lib/types";

const PAGE = 50;
const columns = createColumnHelper<RunView>();

/** A project's runs, or one pipeline's, newest first, loaded a page at a time. */
export interface RunFilters {
  pool?: string;
  agent?: string;
  failure_class?: string;
}

/** A project's runs, or one pipeline's, newest first, a page at a time; every view of the runs shares it. */
export function useRuns(code: string, pipeline?: string, filters: RunFilters = {}) {
  const runs = useInfiniteQuery({
    queryKey: ["runs", code, pipeline ?? "", filters.pool ?? "", filters.agent ?? "", filters.failure_class ?? ""],
    initialPageParam: undefined as number | undefined,
    queryFn: ({ pageParam }) => getJson<History>(`${project(code)}/history`, { all: true, limit: PAGE, pipeline_code: pipeline, before: pageParam, pool: filters.pool, agent: filters.agent, failure_class: filters.failure_class }),
    getNextPageParam: (last) => last.before ?? (last.entries.length >= PAGE ? last.entries[last.entries.length - 1]?.pipeline_run_id : undefined),
    refetchInterval: 15_000,
  });
  const all = React.useMemo(() => runs.data?.pages.flatMap((page) => page.entries) ?? [], [runs.data]);
  return { runs, all };
}

/** The runs of the project, or of one pipeline, as a table. A run opens its own page from a pipeline's list, and
 * its pipeline's list with the run highlighted from the project's. */
export function RunsTable({ code, pipeline, status, codes, filters = {}, highlight }: { code: string; pipeline?: string; status?: string; codes?: ReadonlySet<string>; filters?: RunFilters; highlight?: number }) {
  const tz = useTimezone();
  const { runs, all } = useRuns(code, pipeline, filters);
  const rows = React.useMemo(() => all.filter((run) => (!status || run.status === status) && (!codes || codes.has(run.pipeline_code))), [all, status, codes]);
  return (
    <>
      {runs.isError ? <Notice>{(runs.error as Error).message}</Notice> : null}
      <Card>
        <DataTable
          label="Pipeline runs"
          data={rows}
          highlight={highlight === undefined ? undefined : (run) => run.pipeline_run_id === highlight}
          initialSort={[{ id: "id", desc: true }]}
          empty={runs.isSuccess && !runs.hasNextPage ? (status || codes ? `None of the ${all.length} runs matches the filters. Clear a filter.` : "No runs yet.") : undefined}
          more={{ hasMore: Boolean(runs.hasNextPage), loading: runs.isFetchingNextPage, load: () => void runs.fetchNextPage() }}
          columns={[
            columns.accessor("pipeline_run_id", {
              id: "id",
              header: "Run",
              cell: (c) => (
                <Link
                  className="font-medium text-accent hover:underline"
                  to={pipeline ? runPath(code, c.row.original.pipeline_code, c.getValue()) : pipelineRunPath(code, c.row.original.pipeline_code, c.getValue())}
                >
                  <span className="num">#{c.getValue()}</span>
                </Link>
              ),
            }),
            columns.accessor("run_label", {
              id: "label",
              header: "Identifier",
              cell: (c) => <span className="whitespace-nowrap font-mono text-xs text-muted">{c.getValue() || "—"}</span>,
            }),
            ...(pipeline ? [] : [columns.accessor("pipeline_code", { id: "pipeline", header: "Pipeline", cell: (c) => <span className="font-medium">{c.getValue()}</span> })]),
            columns.accessor("status", { header: "Status", cell: (c) => <StatusBadge status={c.getValue()} /> }),
            columns.accessor("trigger_kind", { header: "Trigger", cell: (c) => <span className="text-muted">{c.getValue().toLowerCase()}{c.row.original.backfill ? " · backfill" : ""}</span> }),
            columns.accessor("run_date", { header: "Logical date", cell: (c) => <span className="num">{c.getValue() ?? "—"}</span> }),
            columns.accessor("start_date", { header: "Started", cell: (c) => <span className="num whitespace-nowrap">{formatTime(c.getValue(), tz)}</span> }),
            columns.display({ id: "duration", header: "Duration", cell: (c) => <span className="num">{formatDuration(c.row.original.start_date, c.row.original.end_date)}</span> }),
            columns.accessor("started_by", { header: "Started by", cell: (c) => <span className="text-muted">{c.getValue() ?? "—"}</span> }),
          ]}
        />
        {runs.isPending ? <Loading what="runs" className="p-4" /> : null}
      </Card>
    </>
  );
}
