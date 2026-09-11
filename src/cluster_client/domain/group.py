from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GroupId:
    """Small value object used by the application boundary."""

    value: str

    def __post_init__(self) -> None:
        normalized = self.value.strip()
        if not normalized:
            raise ValueError("group_id must not be empty")
        object.__setattr__(self, "value", normalized)

    def __str__(self) -> str:
        return self.value
