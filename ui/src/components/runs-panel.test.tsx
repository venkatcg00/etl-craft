import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { beforeAll, describe, expect, it } from "vitest";
import { RunsPanel } from "@/components/runs-panel";

const run = (id: number, pipeline: string) => ({
  pipeline_code: pipeline,
  pipeline_run_id: id,
  run_label: `${pipeline}-2026-11-07 02:27:37-${id}`,
  run_key: `manual:${id}`,
  trigger_kind: "MANUAL",
  run_date: "2026-11-07",
  status: "SUCCESS",
  start_date: "2026-11-07T02:27:37+00:00",
  end_date: "2026-11-07T02:30:30+00:00",
  sla_status: null,
  backfill: false,
  started_by: "demo",
  ended_by: "demo",
  parent_run_id: null,
  child_run_ids: [],
  newer_metadata_available: false,
});

function panel(pipeline: string | undefined, highlight?: number, codes?: ReadonlySet<string>) {
  const client = new QueryClient({ defaultOptions: { queries: { staleTime: Infinity, retry: false } } });
  client.setQueryData(["runs", "demo", pipeline ?? "", "", "", ""], {
    pages: [{ entries: [run(6, "REF"), run(5, "SETUP")], before: null }],
    pageParams: [undefined],
  });
  client.setQueryData(["whoami", null], { name: "owner", role: "admin", settings: { timezone: "UTC", time_display: "UTC" } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <RunsPanel code="demo" pipeline={pipeline} highlight={highlight} codes={codes} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("the shared runs layout", () => {
  beforeAll(() => {
    Element.prototype.scrollIntoView = () => undefined;
  });

  it("shows each run's string identifier beside its number", () => {
    panel(undefined);
    expect(screen.getByText("REF-2026-11-07 02:27:37-6")).toBeTruthy();
    expect(screen.getByText("SETUP-2026-11-07 02:27:37-5")).toBeTruthy();
  });

  it("opens a run in its pipeline workspace from both lists", () => {
    const { unmount } = panel(undefined);
    expect(screen.getByRole("link", { name: "#6" }).getAttribute("href")).toBe("/projects/demo/pipelines/REF?run=6");
    unmount();
    panel("REF");
    expect(screen.getByRole("link", { name: "#6" }).getAttribute("href")).toBe("/projects/demo/pipelines/REF?run=6");
  });

  it("marks the highlighted run and no other", () => {
    panel("REF", 5);
    const marked = screen.getAllByRole("row").filter((row) => row.getAttribute("aria-current") === "true");
    expect(marked).toHaveLength(1);
    expect(within(marked[0] as HTMLElement).getByText("#5")).toBeTruthy();
  });

  it("lists only the runs of pipelines that carry the selected tags", () => {
    panel(undefined, undefined, new Set(["REF"]));
    expect(screen.getByText("REF-2026-11-07 02:27:37-6")).toBeTruthy();
    expect(screen.queryByText("SETUP-2026-11-07 02:27:37-5")).toBeNull();
  });
});
