import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TagFilter } from "@/components/tag-filter";

describe("the tag filter", () => {
  it("toggles a tag and shows which are selected", () => {
    const toggle = vi.fn();
    render(<TagFilter known={["kind:steward", "client:alpha"]} selected={["client:alpha"]} onToggle={toggle} />);
    expect(screen.getByRole("button", { name: "client:alpha" }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByRole("button", { name: "kind:steward" }).getAttribute("aria-pressed")).toBe("false");
    fireEvent.click(screen.getByRole("button", { name: "kind:steward" }));
    expect(toggle).toHaveBeenCalledWith("kind:steward");
  });

  it("says where tags come from when no pipeline has one", () => {
    render(<TagFilter known={[]} selected={[]} onToggle={() => undefined} />);
    expect(screen.getByText(/PIPELINE_PARAMETERS\.TAGS/)).toBeTruthy();
  });
});
