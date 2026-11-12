from unittest.mock import MagicMock

from .. import s3


def _mock_paginator(monkeypatch, pages):
    paginator = MagicMock()
    paginator.paginate.return_value = pages
    client = MagicMock()
    client.get_paginator.return_value = paginator
    monkeypatch.setattr(s3, "s3", client)


def test_ls_returns_keys_across_pages(monkeypatch):
    _mock_paginator(
        monkeypatch,
        [
            {"Contents": [{"Key": "a"}, {"Key": "b"}]},
            {"Contents": [{"Key": "c"}]},
        ],
    )
    assert s3.ls("bucket", "prefix/") == ["a", "b", "c"]


def test_ls_returns_empty_list_when_nothing_matches(monkeypatch):
    # list_objects_v2 omits "Contents" entirely when no keys match the prefix.
    _mock_paginator(monkeypatch, [{"KeyCount": 0}])
    assert s3.ls("bucket", "prefix/") == []
