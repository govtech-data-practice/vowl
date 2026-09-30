---
description: Load vowl data contracts from a local file, Git (GitHub or GitLab), or S3, and connect using the servers the contract defines.
---

# Loading Contracts

`validate_data` takes the contract as its first argument. It can be a local
path, a URL, an S3 URI, or a `Contract` you loaded yourself. vowl checks the
contract against the ODCS schema for its `apiVersion` when it loads it.

```python
from vowl import validate_data
from vowl.contracts import Contract

result = validate_data("contracts/orders.yaml", df=df)   # a local file

contract = Contract.load("contracts/orders.yaml")        # or load it first
result = validate_data(contract, df=df)
```

## Loading contracts from Git (GitHub or GitLab)

Pass the file's URL. A GitHub or GitLab `blob` URL, the one in your browser's
address bar, is turned into its raw URL for you.

```python
from vowl import validate_data

# GitHub blob URL
result = validate_data(
    "https://github.com/org/repo/blob/main/contracts/my_contract.yaml",
    df=df,
)

# GitHub raw URL
result = validate_data(
    "https://raw.githubusercontent.com/org/repo/main/contracts/my_contract.yaml",
    df=df,
)

# GitLab blob URL
result = validate_data(
    "https://gitlab.com/org/repo/-/blob/main/contracts/my_contract.yaml",
    df=df,
)
```

vowl only fetches contracts over `http` or `https` from public addresses. A
URL whose host resolves to a private, internal or loopback address (such as
`localhost`, `10.x.x.x`, or a cloud metadata address) is refused with a
`ContractURLError`. This stops a contract link from being used to reach
machines inside your network. For a contract on an internal Git server,
download the file and pass its local path.

## Loading contracts from S3

```python
from vowl import validate_data

result = validate_data("s3://my-bucket/contracts/my_contract.yaml", df=df)
```

!!! note

    Loading from S3 needs `boto3`, which the base install leaves out. Install
    it with `pip install 'vowl[all]'` or `pip install boto3`. vowl uses your
    default AWS credentials: environment variables, `~/.aws/credentials`, or an
    IAM role. For an S3-compatible store such as MinIO, set
    `AWS_ENDPOINT_URL` to its address.

## Using servers defined in the contract

An ODCS contract can list the servers its data lives on. `get_server` returns
one of them as a `dict`, so you can connect without repeating the details:

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter
from vowl.contracts import Contract

contract = Contract.load("contract.yaml")
server = contract.get_server("my-postgres-server")  # matches the server's `server` field
# contract.get_server("uat")                        # or its `environment`
# contract.get_server()                             # or the first server

con = ibis.postgres.connect(
    host=server["host"],
    port=server.get("port", 5432),
    database=server.get("database", ""),
)

result = validate_data(contract, adapter=IbisAdapter(con))
```

`get_server` looks for a server whose `server` field matches, then for one
whose `environment` matches. It raises a `ValueError` when the contract has no
servers or none match. `contract.get_servers()` returns them all.
