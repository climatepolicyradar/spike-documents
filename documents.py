from prefect import task, flow
from pydantic import BaseModel
import pytest
from prefect.testing.utilities import prefect_test_harness



@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


class Label(BaseModel):
    preferred_label: str


class DocumentLabel(BaseModel):
    relationship: str
    label: Label


class InputDocument(BaseModel):
    url: str
    # These fields are here as an input might suggest these values
    preferred_label: str | None = None
    labels: list[DocumentLabel] = []
    content_type: str | None = None


class IDedDocument(InputDocument):
    id: str


@task
def get_document_id(document: InputDocument) -> str:
    return "123"


class CDNUrl(BaseModel):
    cdn_url: str


@task
def get_document_cdn_url(document: IDedDocument) -> CDNUrl:
    return CDNUrl(
        cdn_url="https://cdn.com/123",
    )


class ContentType(BaseModel):
    content_type: str


@task
def get_document_content_type(document: IDedDocument) -> ContentType:
    return ContentType(
        content_type="text/plain",
    )


@task
def get_document_labels(document: IDedDocument) -> list[DocumentLabel]:
    return [
        DocumentLabel(relationship="is_a", label=Label(preferred_label="Document"))
    ] + document.labels


class Passage(BaseModel):
    text: str
    start: int
    end: int


@task
def get_document_passages(document: IDedDocument) -> list[Passage]:
    return [Passage(text="Hello, world!", start=0, end=13)]

# This is what is exposed by the API / shared with the CMS
class OutputDocument(BaseModel):
    id: str
    cdn_url: CDNUrl
    content_type: ContentType
    labels: list[DocumentLabel]
    passages: list[Passage]


@task
def assemble_document(
    id: str,
    cdn_url: CDNUrl,
    content_type: ContentType,
    labels: list[DocumentLabel],
    passages: list[Passage],
) -> OutputDocument:
    return OutputDocument(
        id=id,
        cdn_url=cdn_url,
        content_type=content_type,
        labels=labels,
        passages=passages,
    )


@flow
def get_output_document(document: InputDocument) -> OutputDocument:
    # ID first
    id_obj = get_document_id.submit(document).result()
    ided = IDedDocument(
        id=id_obj,
        **document.model_dump(),
    )

    # Fanout
    # Start the tasks
    cdn_fut = get_document_cdn_url.submit(ided)
    content_fut = get_document_content_type.submit(ided)
    labels_fut = get_document_labels.submit(ided)
    passages_fut = get_document_passages.submit(ided)

    # Wait and collect
    cdn_url = cdn_fut.result()
    content_type = content_fut.result()
    labels = labels_fut.result()
    passages = passages_fut.result()

    return assemble_document.submit(
        id_obj, cdn_url, content_type, labels, passages
    ).result()


def test_get_output_document():
    assert get_output_document(InputDocument(url="https://example.com")).id == "123"


