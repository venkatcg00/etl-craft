import pytest

from etl_craft.resolver import (
    CycleError,
    ResolverError,
    SelfDependencyError,
    TaskEdge,
    TaskNode,
    TaskRunState,
    UnknownTaskError,
    build_graph,
)


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
    with pytest.raises(ResolverError):
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
