import { cn } from "@/lib/utils";

/** The pipeline tags as toggles: a pipeline, or the runs of one, must carry every tag selected. */
export function TagFilter({ known, selected, onToggle }: { known: string[]; selected: string[]; onToggle: (tag: string) => void }) {
  return (
    <div className="flex flex-wrap items-center gap-2" role="group" aria-label="Filter by tag">
      <span className="text-xs font-semibold uppercase tracking-wide text-muted">Tags</span>
      {known.length ? (
        known.map((tag) => (
          <button
            key={tag}
            type="button"
            aria-pressed={selected.includes(tag)}
            onClick={() => onToggle(tag)}
            className={cn("cursor-pointer rounded-full border px-2.5 py-1 text-xs max-sm:py-2", selected.includes(tag) ? "border-accent bg-accent text-accent-fg" : "border-border-strong bg-surface text-fg hover:bg-surface-2")}
          >
            {tag}
          </button>
        ))
      ) : (
        <span className="text-xs text-muted">No pipeline has tags; declare them in PIPELINE_PARAMETERS.TAGS.</span>
      )}
    </div>
  );
}
