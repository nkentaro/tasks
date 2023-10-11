import uuid

import boto3
import pytest
from fastapi import status
from moto import mock_dynamodb
from starlette.testclient import TestClient

from main import app
from models import Task
from store import TaskStore


@pytest.fixture
def client():
    return TestClient(app)


def test_health_check(client):
    """
    GIVEN
    WHEN health check endpoint is called with GET method
    THEN response with status 200 and body OK is returned
    """
    response = client.get("/api/health-check/")
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"message": "OK"}


@pytest.fixture
def dynamodb_table():
    with mock_dynamodb():
        client = boto3.client("dynamodb")
        table_name = "test-table"
        client.create_table(
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield table_name


def test_added_task_retrieved_by_id(dynamodb_table):
    repository = TaskStore(table_name=dynamodb_table)
    task = Task.create(uuid.uuid4(), "Clean your home", "ken@g3labs.net")

    repository.add(task)

    assert repository.get_by_id(task_id=task.id, owner=task.owner) == task
