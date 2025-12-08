# dags/custom_components/math_operators.py
from __future__ import annotations
from typing import Any, Dict
from airflow.models.baseoperator import BaseOperator
from custom_components.math_triggers import MathValidationTrigger
from airflow.sdk import BaseSensorOperator, Context
from airflow.triggers.base import StartTriggerArgs


class DeferrableMathOperator(BaseOperator):
    start_trigger_args = StartTriggerArgs(
        trigger_cls="custom_components.math_triggers.MathValidationTrigger",
        trigger_kwargs=None,
        # next_method=None,
        next_method="execute_complete",
        # next_kwargs=None,
        timeout=None,
    )

    def __init__(
        self,
        x: int,
        y: int,
        operation: str = "add",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.x = x
        self.y = y
        self.operation = operation
        self.start_trigger_args.trigger_kwargs = {
            "x": self.x,
            "y": self.y,
            "operation": self.operation,
            "end_from_trigger": True
        }
        self.start_from_trigger = True
  
  
    def execute_complete(
        self,
        context: Dict[str, Any],
        event: Dict[str, Any] | None = None,
    ):
        if event is None:
            self.log.error("No event received from MathValidationTrigger")
            raise ValueError("No event received from MathValidationTrigger")

        status = event.get("status")
        result = event.get("result")
        self.log.info("Received event from MathValidationTrigger: %s", event)

        if status != "success":
            msg = event.get("message", "Validation failed")
            self.log.error("Math validation failed: %s", msg)
            raise ValueError(f"Math validation failed: {msg}")

        self.log.info(
            "Math validation succeeded. %s(%s, %s) = %s",
            event.get("operation"),
            event.get("x"),
            event.get("y"),
            result,
        )

        # Final task result (also goes to XCom)
        return result