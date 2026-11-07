import type * as React from "react";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { RunBars } from "@/components/pipeline-parts";
import { RunsTable, useRuns, type RunFilters } from "@/components/runs-table";

/**
 * The one runs layout: a strip of the latest runs' durations above their table. A pipeline's Runs tab and
 * the project's Runs page both use it, so they look and behave alike. The toolbar, the filters of the scope,
 * sits between the strip and the table.
 */
export function RunsPanel({ code, pipeline, status, codes, filters = {}, highlight, toolbar, bars = true }: { bars?: boolean; code: string; pipeline?: string; status?: string; codes?: ReadonlySet<string>; filters?: RunFilters; highlight?: number; toolbar?: React.ReactNode }) {
  const { all } = useRuns(code, pipeline, filters);
  return (
    <>
      {bars && all.length ? (
        <Card>
          <CardHeader>
            <CardTitle>Run durations</CardTitle>
            <span className="text-xs text-muted">Newest on the right; select a bar to open its run</span>
          </CardHeader>
          <div className="p-4">
            <RunBars project={code} runs={(codes ? all.filter((run) => codes.has(run.pipeline_code)) : all).slice(0, 40)} />
          </div>
        </Card>
      ) : null}
      {toolbar ? <div className="flex flex-wrap items-end gap-3">{toolbar}</div> : null}
      <RunsTable code={code} pipeline={pipeline} status={status} codes={codes} filters={filters} highlight={highlight} />
    </>
  );
}
