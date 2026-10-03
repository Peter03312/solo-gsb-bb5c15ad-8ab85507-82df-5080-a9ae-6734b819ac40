from typing import Any

from pydantic import BaseModel, Field, model_validator

from .hashing import TOTAL_BUCKETS


class LayerCreate(BaseModel):
    key: str = Field(min_length=1, max_length=128, examples=["checkout"])
    # Fixed salt for stable hashing in this layer; auto-generated when omitted.
    salt: str | None = Field(default=None, max_length=256)


class GroupIn(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    weight: int = Field(ge=0, le=TOTAL_BUCKETS)


class ExperimentConfig(BaseModel):
    """Half-open bucket range [bucket_start, bucket_end), full-match attribute
    conditions, and group weights summing to 10000."""

    bucket_start: int = Field(ge=0, description="range start, inclusive")
    bucket_end: int = Field(gt=0, le=TOTAL_BUCKETS, description="range end, exclusive")
    conditions: dict[str, Any] = Field(default_factory=dict)
    groups: list[GroupIn] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate(self):
        if not 0 <= self.bucket_start < self.bucket_end <= TOTAL_BUCKETS:
            raise ValueError(
                f"require 0 <= bucket_start < bucket_end <= {TOTAL_BUCKETS} "
                "(half-open interval)"
            )
        names = [g.name for g in self.groups]
        if len(names) != len(set(names)):
            raise ValueError("group names must be unique")
        total = sum(g.weight for g in self.groups)
        if total != TOTAL_BUCKETS:
            raise ValueError(
                f"group weights must sum to {TOTAL_BUCKETS}, got {total}"
            )
        return self


class ExperimentCreate(ExperimentConfig):
    key: str = Field(min_length=1, max_length=128)


class ExperimentUpdate(ExperimentConfig):
    pass


class AssignRequest(BaseModel):
    layer_key: str = Field(min_length=1, max_length=128)
    user_key: str = Field(min_length=1, max_length=256)
    attributes: dict[str, Any] = Field(default_factory=dict)


class ExposureRequest(BaseModel):
    assignment_id: int = Field(ge=1)
    event_no: int = Field(ge=0)
    content: Any = None
