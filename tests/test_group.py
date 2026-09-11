import pytest

from cluster_client.domain import GroupId


def test_group_id_trims_value() -> None:
    assert str(GroupId("  team-a  ")) == "team-a"


def test_group_id_rejects_empty_value() -> None:
    with pytest.raises(ValueError):
        GroupId("   ")
