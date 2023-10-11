# store abstraction on top of DynamoDB.
from uuid import UUID

import boto3

from models import Task, TaskStatus


class TaskStore:
    def __init__(self, table_name):
        self.table_name = table_name

    def add(self, task):
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(self.table_name)
        table.put_item(
            Item={
                "PK": f"#{task.owner}",
                "SK": f"#{task.id}",
                "id": str(task.id),
                "title": task.title,
                "status": task.status.value,
                "owner": task.owner,
            }
        )

    def get_by_id(self, task_id, owner):
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(self.table_name)
        record = table.get_item(
            Key={
                "PK": f"#{owner}",
                "SK": f"#{task_id}",
            },
        )
        return Task(
            id=UUID(record["Item"]["id"]),
            title=record["Item"]["title"],
            owner=record["Item"]["owner"],
            status=TaskStatus[record["Item"]["status"]],
        )
