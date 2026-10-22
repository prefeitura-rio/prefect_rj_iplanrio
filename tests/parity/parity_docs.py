"""Seed the document stores of the parity lab with the dataset of `parity_dataset.py`.

Usage: python parity_docs.py couchdb|mongodb. It reads the connection data from the
`env.json` of the lab folder, `PARITY_LAB`.
"""

import json
import socket
import sys
import time
from base64 import b64encode
from datetime import UTC, datetime
from datetime import time as day_start
from os import environ
from pathlib import Path
from urllib.request import Request, urlopen

from bson import Decimal128
from parity_dataset import rows
from pymongo import MongoClient
from pymongo.errors import PyMongoError

ENV = json.loads((Path(environ.get("PARITY_LAB", "/tmp/toolkit-parity")) / "env.json").read_text())


def couchdb() -> None:
    """Create the database, the documents, a design document and a deletion."""
    entry = ENV["couchdb"]
    base = f"http://{entry['host']}:{entry['port']}"
    token = b64encode(f"{entry['user']}:{entry['password']}".encode()).decode()

    def call(method: str, path: str, body: object | None = None) -> object:
        data = None if body is None else json.dumps(body).encode()
        request = Request(base + path, data=data, method=method)
        request.add_header("Authorization", f"Basic {token}")
        request.add_header("Content-Type", "application/json")
        with urlopen(request) as response:
            return json.loads(response.read() or b"null")

    call("PUT", "/orders")
    documents = [
        {
            "_id": f"{row['id']:011d}",
            "id": row["id"],
            "nome": row["nome"],
            "dia": None if row["dia"] is None else row["dia"].isoformat(),
            "valor": str(row["valor"]),
        }
        for row in rows()
    ]
    call("POST", "/orders/_bulk_docs", {"docs": documents})
    call("PUT", "/orders/_design/lab", {"views": {"by_nome": {"map": "function(doc){emit(doc.nome, 1)}"}}})
    created = call("PUT", "/orders/00000000400", {"nome": "deleted soon"})
    assert isinstance(created, dict)
    call("DELETE", f"/orders/00000000400?rev={created['rev']}")


def mongodb() -> None:
    """Create the login and the collection."""
    entry = ENV["mongodb"]

    for _ in range(120):
        try:
            socket.create_connection((entry["host"], entry["port"]), timeout=1).close()
            break
        except OSError:
            time.sleep(1)
    else:
        raise SystemExit("mongod did not open its port")

    if not entry.get("image"):
        anonymous = MongoClient(entry["host"], entry["port"])
        anonymous.admin.command(
            "createUser", entry["user"], pwd=entry["password"], roles=[{"role": "root", "db": "admin"}]
        )
    client = MongoClient(
        entry["host"], entry["port"], username=entry["user"], password=entry["password"], authSource="admin"
    )
    for _ in range(120):
        try:
            client.admin.command("ping")
            break
        except PyMongoError:
            time.sleep(1)
    documents = [
        {
            "id": row["id"],
            "nome": row["nome"],
            "dia": None if row["dia"] is None else datetime.combine(row["dia"], day_start.min, tzinfo=UTC),
            "valor": Decimal128(str(row["valor"])),
        }
        for row in rows()
    ]
    client["lab"]["orders"].insert_many(documents)


if __name__ == "__main__":
    {"couchdb": couchdb, "mongodb": mongodb}[sys.argv[1]]()
