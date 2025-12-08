# dags/custom_components/math_triggers.py
from __future__ import annotations
from typing import Any, Dict
from airflow.triggers.base import BaseTrigger, TriggerEvent, TaskSuccessEvent, TaskFailedEvent

class MathValidationTrigger(BaseTrigger):


    def __init__(self, x: int, y: int, operation: str = "add", result: Any = None, end_from_trigger: bool = False):
        super().__init__()
        self.x = x
        self.y = y
        self.operation = operation
        self.result = result
        self.end_from_trigger = end_from_trigger


    def serialize(self) -> tuple[str, Dict[str, Any]]:
        return (
            "custom_components.math_triggers.MathValidationTrigger",
            {
                "x": self.x,
                "y": self.y,
                "operation": self.operation,
                "result": self.result,
                "end_from_trigger": self.end_from_trigger,
            },
        )

    async def run(self):
        # This runs in the triggerer process.
        if self.operation == "add":
            result = self.x + self.y
        elif self.operation == "subtract":
            result = self.x - self.y
        elif self.operation == "multiply":
            result = self.x * self.y
        elif self.operation == "divide":
            result = self.x / self.y
        else:
            yield TaskFailedEvent(
                {
                    "status": "error",
                    "message": f"Unsupported operation: {self.operation}",
                }
            )
            return

        is_valid = True 
        
        event_payload = {
            "status": "success" if is_valid else "failed",
            "result": result,
            "operation": self.operation,
            "x": self.x,
            "y": self.y,
        }
        if self.end_from_trigger:
            yield TaskSuccessEvent(xcoms=event_payload, task_instance_state="success")
        else: 
            yield TriggerEvent(payload=event_payload)
