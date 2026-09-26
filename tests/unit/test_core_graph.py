import sys

import pytest

from etl_craft.core.enums import DependencyType, RunCondition, RunStatus
from etl_craft.core.errors import ExitCode, GraphError
from etl_craft.core.graph import (
    CycleError,
    DependencyGraph,
    SelfDependencyError,
    TaskEdge,
    TaskNode,
    TaskRunState,
    UnknownTaskError,
    build_graph,
)

pytestmark = pytest.mark.unit


def nodes(*ids: int) -> list[TaskNode]:
    return [TaskNode(task_id=i) for i in ids]


def edge(task_id: int, depends_on_task_id: int, dependency_type: str = "SUCCESS") -> TaskEdge:
    return TaskEdge(
        task_id=task_id, depends_on_task_id=depends_on_task_id, dependency_type=dependency_type
    )


def test_no_dependencies_is_a_single_wave():
    graph = build_graph(nodes(1, 2, 3), [])
    assert graph.waves() == [[1, 2, 3]]


def test_linear_chain_waves():
    # 1 -> 2 -> 3  (2 depends on 1, 3 depends on 2)
    graph = build_graph(nodes(1, 2, 3), [edge(2, 1), edge(3, 2)])
    assert graph.waves() == [[1], [2], [3]]


def test_diamond_shape_waves():
    #     1
    #    / \
    #   2   3
    #    \ /
    #     4
    graph = build_graph(nodes(1, 2, 3, 4), [edge(2, 1), edge(3, 1), edge(4, 2), edge(4, 3)])
    assert graph.waves() == [[1], [2, 3], [4]]


def test_self_dependency_rejected():
    with pytest.raises(SelfDependencyError):
        build_graph(nodes(1), [edge(1, 1)])


def test_direct_cycle_rejected():
    with pytest.raises(CycleError):
        build_graph(nodes(1, 2), [edge(1, 2), edge(2, 1)])


def test_indirect_cycle_rejected():
    with pytest.raises(CycleError):
        build_graph(nodes(1, 2, 3), [edge(1, 2), edge(2, 3), edge(3, 1)])


def test_unknown_task_id_in_edge_rejected():
    with pytest.raises(UnknownTaskError):
        build_graph(nodes(1, 2), [edge(1, 99)])


def test_unknown_dependency_type_rejected():
    with pytest.raises(GraphError):
        build_graph(nodes(1, 2), [edge(1, 2, dependency_type="BOGUS")])


def test_ready_with_no_edges_all_unstarted_tasks_ready():
    graph = build_graph(nodes(1, 2), [])
    assert graph.ready({}) == [1, 2]


def test_ready_excludes_terminal_tasks():
    graph = build_graph(nodes(1, 2), [])
    state = {1: TaskRunState(status="SUCCESS")}
    assert graph.ready(state) == [2]


def test_ready_success_edge_blocks_until_upstream_succeeds():
    # task 1 has no deps of its own, so its own retry-eligibility (tested
    # separately) is irrelevant here — only whether 2's edge is satisfied.
    graph = build_graph(nodes(1, 2), [edge(2, 1)])
    assert 2 not in graph.ready({})
    assert 2 not in graph.ready({1: TaskRunState(status="IN-PROGRESS")})
    assert 2 not in graph.ready({1: TaskRunState(status="FAILED")})
    assert graph.ready({1: TaskRunState(status="SUCCESS")}) == [2]


def test_ready_failure_edge_only_satisfied_by_failed_upstream():
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="FAILURE")])
    assert 2 not in graph.ready({1: TaskRunState(status="SUCCESS")})
    assert 2 in graph.ready({1: TaskRunState(status="FAILED")})


@pytest.mark.parametrize("upstream_status", ["SUCCESS", "FAILED", "SKIPPED"])
def test_ready_always_edge_satisfied_by_any_terminal_status(upstream_status):
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="ALWAYS")])
    assert 2 in graph.ready({1: TaskRunState(status=upstream_status)})


def test_ready_always_edge_not_satisfied_while_upstream_in_progress():
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="ALWAYS")])
    assert 2 not in graph.ready({1: TaskRunState(status="IN-PROGRESS")})


def test_ready_has_data_edge_requires_success_and_positive_target_count():
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="HAS_DATA")])
    assert 2 not in graph.ready({1: TaskRunState(status="SUCCESS", target_count=0)})
    assert 2 not in graph.ready({1: TaskRunState(status="SUCCESS", target_count=None)})
    assert graph.ready({1: TaskRunState(status="SUCCESS", target_count=5)}) == [2]
    assert 2 not in graph.ready({1: TaskRunState(status="FAILED", target_count=5)})


def test_ready_task_with_multiple_edges_needs_all_satisfied():
    graph = build_graph(nodes(1, 2, 3), [edge(3, 1), edge(3, 2)])
    # task 1 already SUCCESS (terminal, excluded); task 2 has no deps of its
    # own, so it's ready; task 3 still waits on task 2.
    assert graph.ready({1: TaskRunState(status="SUCCESS")}) == [2]
    assert graph.ready({1: TaskRunState(status="SUCCESS"), 2: TaskRunState(status="SUCCESS")}) == [
        3
    ]


def test_ready_reattempts_failed_task_itself():
    graph = build_graph(nodes(1), [])
    assert graph.ready({1: TaskRunState(status="FAILED")}) == [1]


def test_ready_excludes_in_progress_task_itself():
    graph = build_graph(nodes(1), [])
    assert graph.ready({1: TaskRunState(status="IN-PROGRESS")}) == []


def test_skipped_upstream_never_satisfies_success_edge():
    graph = build_graph(nodes(1, 2), [edge(2, 1)])
    assert graph.ready({1: TaskRunState(status="SKIPPED")}) == []


def test_build_graph_rejects_duplicate_task_ids():
    with pytest.raises(GraphError):
        build_graph(nodes(1, 1), [])


def test_build_graph_rejects_edge_with_unknown_task_id():
    # Distinct from test_unknown_task_id_in_edge_rejected: that one has an
    # unknown depends_on_task_id with a valid task_id; this is the other
    # side — task_id itself doesn't exist in the supplied task set.
    with pytest.raises(UnknownTaskError):
        build_graph(nodes(1, 2), [edge(99, 1)])


# RUN_CONDITION: how many of a task's dependencies must be satisfied


def test_run_condition_defaults_to_all_when_unset():
    # A NULL RUN_CONDITION means ALL: every edge, or nothing runs.
    graph = build_graph(nodes(1, 2, 3), [edge(3, 1), edge(3, 2)])
    assert graph.required_edge_count(3) == 2
    assert 3 not in graph.ready({1: TaskRunState(status="SUCCESS")})
    assert 3 in graph.ready({1: TaskRunState(status="SUCCESS"), 2: TaskRunState(status="SUCCESS")})


def test_run_condition_any_runs_once_a_single_edge_is_satisfied():
    # A task with several upstreams that can run once any one of them succeeds.
    tasks = [TaskNode(task_id=i) for i in (1, 2, 3)] + [TaskNode(task_id=4, run_condition="ANY")]
    graph = build_graph(tasks, [edge(4, 1), edge(4, 2), edge(4, 3)])
    assert graph.required_edge_count(4) == 1
    assert 4 in graph.ready({1: TaskRunState(status="SUCCESS")})


def test_run_condition_n_requires_that_many_edges():
    tasks = [TaskNode(task_id=i) for i in (1, 2, 3)] + [
        TaskNode(task_id=4, run_condition="N", run_condition_count=2)
    ]
    graph = build_graph(tasks, [edge(4, 1), edge(4, 2), edge(4, 3)])
    assert graph.required_edge_count(4) == 2
    assert 4 not in graph.ready({1: TaskRunState(status="SUCCESS")})
    assert 4 in graph.ready({1: TaskRunState(status="SUCCESS"), 2: TaskRunState(status="SUCCESS")})


def test_build_graph_rejects_an_n_count_larger_than_the_edge_count():
    # No CHECK constraint can express this: it compares a CFG_TASKS column with a count of
    # CFG_TASK_DEPENDENCY rows.
    tasks = [TaskNode(task_id=1), TaskNode(task_id=2, run_condition="N", run_condition_count=3)]
    with pytest.raises(GraphError, match="could never run"):
        build_graph(tasks, [edge(2, 1)])


@pytest.mark.parametrize(
    "condition, count",
    [("BOGUS", None), ("N", None), ("N", 0), ("ALL", 2), (None, 2)],
)
def test_build_graph_rejects_malformed_run_conditions(condition, count):
    tasks = [
        TaskNode(task_id=1),
        TaskNode(task_id=2, run_condition=condition, run_condition_count=count),
    ]
    with pytest.raises(GraphError):
        build_graph(tasks, [edge(2, 1)])


def test_run_condition_counts_cross_pipeline_edges_too():
    # Cross-pipeline edges stay out of the graph, but RUN_CONDITION ranges over all of a
    # task's dependencies, so the N check counts them too.
    tasks = [
        TaskNode(task_id=1),
        TaskNode(task_id=2, run_condition="N", run_condition_count=2, cross_pipeline_edge_count=1),
    ]
    graph = build_graph(tasks, [edge(2, 1)])
    assert graph.total_edge_count(2) == 2
    assert graph.required_edge_count(2) == 2


def test_run_condition_any_is_satisfied_by_a_same_pipeline_edge_alone():
    # One satisfied edge meets ANY, whichever pipeline it comes from.
    tasks = [
        TaskNode(task_id=1),
        TaskNode(task_id=2, run_condition="ANY", cross_pipeline_edge_count=1),
    ]
    graph = build_graph(tasks, [edge(2, 1)])
    state = {1: TaskRunState(status="SUCCESS")}
    # cross_pipeline_satisfied={2: 0} -- not one cross-pipeline edge satisfied.
    assert graph.satisfied_edge_count(2, state, 0) == 1
    assert 2 in graph.ready(state, {2: 0})


def test_ready_treats_unevaluated_cross_pipeline_edges_optimistically():
    # The wave scheduler passes no cross-pipeline counts: the real gate runs inside the
    # task's own `run --task_code` process. Counting them as unsatisfied would never start the
    # task, so the gate that settles them would never run.
    tasks = [TaskNode(task_id=1), TaskNode(task_id=2, cross_pipeline_edge_count=1)]
    graph = build_graph(tasks, [edge(2, 1)])
    state = {1: TaskRunState(status="SUCCESS")}
    assert 2 in graph.ready(state)
    assert 2 not in graph.ready(state, {2: 0})


def test_ready_holds_an_any_task_whose_same_pipeline_upstream_has_not_run_yet():
    # Unevaluated cross-pipeline edges count as satisfied, which alone would meet ANY and
    # start task 2 next to task 1, the upstream it waits on. Inside the task the cross edge
    # might be unsatisfied, and the task would settle SKIPPED just before task 1 satisfied it.
    tasks = [
        TaskNode(task_id=1),
        TaskNode(task_id=2, run_condition="ANY", cross_pipeline_edge_count=1),
    ]
    graph = build_graph(tasks, [edge(2, 1)])

    # Nothing has run: only the upstream goes.
    assert graph.ready({}) == [1]
    # Still running: still held.
    assert graph.ready({1: TaskRunState(status="IN-PROGRESS")}) == []
    # Succeeded: the same-pipeline edge alone satisfies ANY, so it goes and
    # never needs the cross edge at all.
    assert 2 in graph.ready({1: TaskRunState(status="SUCCESS")})


def test_ready_releases_an_any_task_once_its_same_pipeline_upstream_has_failed():
    # The boundary that keeps the hold from becoming its own bug. The task is
    # held only while an upstream is *not yet terminal*, not until it is
    # SETTLED: a permanently FAILED upstream has had its say, and the cross
    # edge may still satisfy ANY -- which is precisely what ANY is for.
    tasks = [
        TaskNode(task_id=1),
        TaskNode(task_id=2, run_condition="ANY", cross_pipeline_edge_count=1),
    ]
    graph = build_graph(tasks, [edge(2, 1)])

    assert 2 in graph.ready({1: TaskRunState(status="FAILED")})


def test_unsatisfiable_counts_an_unevaluated_cross_pipeline_edge_as_still_possible():
    # This module cannot see AUD_DEPENDENCY_CONSUMPTION, so a cross-pipeline edge
    # must always count as "could still be satisfied". Under ANY that is enough
    # to keep the task alive even though its one same-pipeline edge is doomed.
    tasks = [
        TaskNode(task_id=1),
        TaskNode(task_id=2, run_condition="ANY", cross_pipeline_edge_count=1),
    ]
    graph = build_graph(tasks, [edge(2, 1, dependency_type="FAILURE")])
    assert graph.unsatisfiable({1: TaskRunState(status="SUCCESS")}) == []


def test_unsatisfiable_still_dooms_an_all_task_whose_same_pipeline_edge_is_hopeless():
    # The other side of the same boundary: under ALL every edge is required, so
    # one hopeless same-pipeline edge dooms the task no matter what the
    # cross-pipeline half might eventually do.
    tasks = [TaskNode(task_id=1), TaskNode(task_id=2, cross_pipeline_edge_count=1)]
    graph = build_graph(tasks, [edge(2, 1, dependency_type="FAILURE")])
    assert graph.unsatisfiable({1: TaskRunState(status="SUCCESS")}) == [2]


# unsatisfiable(): never-run tasks that can never become ready under this run


def test_unsatisfiable_flags_a_failure_edge_whose_upstream_succeeded():
    # The recommended EMAIL_ALERT pattern, on a run where nothing failed.
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="FAILURE")])
    assert graph.unsatisfiable({1: TaskRunState(status="SUCCESS")}) == [2]


def test_unsatisfiable_ignores_an_upstream_that_merely_failed():
    # FAILED stays retry-eligible — "retry resumes" — so a SUCCESS edge on it
    # is "not yet", not "never". Converting this into a skip would report a
    # broken run as SUCCESS.
    graph = build_graph(nodes(1, 2), [edge(2, 1)])
    assert graph.unsatisfiable({1: TaskRunState(status="FAILED")}) == []


def test_unsatisfiable_treats_a_failure_as_final_when_no_retry_will_come():
    # A local run with nothing left to start: its failures stand, so what needs them to
    # succeed is doomed, transitively, and an ALWAYS dependent (an alert) may then run.
    graph = build_graph(nodes(1, 2, 3, 4), [edge(2, 1), edge(3, 2), edge(4, 3, "ALWAYS")])
    failed = {1: TaskRunState(status="FAILED")}
    assert graph.unsatisfiable(failed) == []
    assert graph.unsatisfiable(failed, failures_final=True) == [2, 3]
    skipped = {**failed, 2: TaskRunState(status="SKIPPED"), 3: TaskRunState(status="SKIPPED")}
    # The failed task stays eligible for another attempt; the run loop does not repeat it.
    assert graph.ready(skipped) == [1, 4]


def test_unsatisfiable_ignores_an_upstream_still_in_progress():
    graph = build_graph(nodes(1, 2), [edge(2, 1)])
    assert graph.unsatisfiable({1: TaskRunState(status="IN-PROGRESS")}) == []


def test_unsatisfiable_flags_a_has_data_edge_whose_upstream_wrote_nothing():
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="HAS_DATA")])
    state = {1: TaskRunState(status="SUCCESS", target_count=0)}
    assert graph.unsatisfiable(state) == [2]


def test_unsatisfiable_cascades_to_a_fixpoint():
    # 1 SUCCESS -> 2 (FAILURE) can never run -> 3 (SUCCESS on 2) can never
    # run either, once 2 is settled SKIPPED. One pass would only find 2.
    graph = build_graph(nodes(1, 2, 3), [edge(2, 1, dependency_type="FAILURE"), edge(3, 2)])
    assert graph.unsatisfiable({1: TaskRunState(status="SUCCESS")}) == [2, 3]


def test_unsatisfiable_does_not_cascade_through_an_always_edge():
    # An ALWAYS edge is satisfied by the SKIPPED that 2 is about to get, so 3
    # is genuinely still runnable — the cascade must not over-reach.
    graph = build_graph(
        nodes(1, 2, 3),
        [edge(2, 1, dependency_type="FAILURE"), edge(3, 2, dependency_type="ALWAYS")],
    )
    assert graph.unsatisfiable({1: TaskRunState(status="SUCCESS")}) == [2]


def test_unsatisfiable_under_any_needs_every_edge_to_be_hopeless():
    tasks = [TaskNode(task_id=1), TaskNode(task_id=2), TaskNode(task_id=3, run_condition="ANY")]
    graph = build_graph(
        tasks,
        [edge(3, 1, dependency_type="FAILURE"), edge(3, 2, dependency_type="FAILURE")],
    )
    # One upstream succeeded (that edge is hopeless), the other hasn't run.
    assert graph.unsatisfiable({1: TaskRunState(status="SUCCESS")}) == []
    assert graph.unsatisfiable(
        {1: TaskRunState(status="SUCCESS"), 2: TaskRunState(status="SUCCESS")}
    ) == [3]


def test_unsatisfiable_leaves_tasks_that_already_have_a_row_alone():
    # Only never-run tasks are reported. A task that already ran owns its own
    # status, whatever it is.
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="FAILURE")])
    state = {1: TaskRunState(status="SUCCESS"), 2: TaskRunState(status="FAILED")}
    assert graph.unsatisfiable(state) == []


def test_check_acyclic_handles_a_chain_deeper_than_the_recursion_limit():
    # A deep chain is a configuration, not an engine bug: it must not hit RecursionError.
    depth = sys.getrecursionlimit() + 500
    ids = list(range(1, depth + 1))
    chain = [edge(i + 1, i) for i in range(1, depth)]
    graph = build_graph(nodes(*ids), chain)
    assert len(graph.waves()) == depth


def test_check_acyclic_skips_a_root_already_visited_as_a_descendant():
    # Task ids carry no ordering guarantee relative to the dependency
    # direction — 1 depending on 2 is perfectly ordinary. Walking roots in id
    # order then reaches 2 as a descendant of 1 first, so the outer loop must
    # skip it rather than restart the traversal.
    graph = build_graph(nodes(1, 2), [edge(1, 2)])
    assert graph.waves() == [[2], [1]]


def test_check_acyclic_still_finds_a_cycle_at_depth():
    depth = 2000
    ids = list(range(1, depth + 1))
    chain = [edge(i + 1, i) for i in range(1, depth)]
    chain.append(edge(1, depth))
    with pytest.raises(CycleError):
        build_graph(nodes(*ids), chain)


def test_edge_satisfied_rejects_unknown_dependency_type():
    # build_graph already validates dependency_type before a DependencyGraph
    # is ever constructed, so this path is unreachable through the public
    # API — exercised directly against the underlying building blocks instead.
    with pytest.raises(GraphError):
        DependencyGraph._edge_satisfied(edge(2, 1, dependency_type="BOGUS"), TaskRunState())


def test_each_graph_error_has_its_own_exit_code():
    codes = {
        error.exit_code for error in (GraphError, CycleError, SelfDependencyError, UnknownTaskError)
    }
    assert codes == {
        ExitCode.GRAPH,
        ExitCode.DEPENDENCY_CYCLE,
        ExitCode.SELF_DEPENDENCY,
        ExitCode.UNKNOWN_TASK,
    }


def test_a_cycle_error_names_the_cycle():
    with pytest.raises(CycleError, match=r"dependency cycle: 1 -> 2 -> 3 -> 1") as error:
        build_graph(nodes(1, 2, 3), [edge(1, 2), edge(2, 3), edge(3, 1)])
    assert error.value.cycle == (1, 2, 3, 1)


def test_enum_members_and_raw_strings_are_interchangeable():
    tasks = [TaskNode(task_id=1), TaskNode(task_id=2, run_condition=RunCondition.ANY)]
    edges = [edge(2, 1, dependency_type=DependencyType.HAS_DATA)]
    graph = build_graph(tasks, edges)
    assert graph.ready({1: TaskRunState(status=RunStatus.SUCCESS, target_count=1)}) == [2]
    assert graph.ready({1: TaskRunState(status="SUCCESS", target_count=1)}) == [2]


def test_dependencies_of_returns_a_copy():
    graph = build_graph(nodes(1, 2), [edge(2, 1)])
    graph.dependencies_of(2).clear()
    assert graph.dependencies_of(2) == [edge(2, 1)]
    assert graph.task_ids == (1, 2)


def test_any_with_no_dependencies_needs_none():
    graph = build_graph([TaskNode(task_id=1, run_condition="ANY")], [])
    assert graph.required_edge_count(1) == 0
    assert graph.ready({}) == [1]
    assert graph.unsatisfiable({}) == []
