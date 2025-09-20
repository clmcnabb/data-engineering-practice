import io
import zipfile
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from main import download_and_extract_async, download_and_extract_sync


@pytest.mark.asyncio
async def test_download_and_extract_async_success(tmp_path):
    uri = "http://example.com/test.zip"
    data_dir = tmp_path
    zip_filename = uri.split("/")[-1]
    zip_path = data_dir / zip_filename

    mem_zip = io.BytesIO()
    with zipfile.ZipFile(mem_zip, "w") as zf:
        zf.writestr("file.txt", "content")
    mem_zip.seek(0)
    zip_bytes = mem_zip.read()

    mock_response = AsyncMock()
    mock_response.__aenter__.return_value = mock_response
    mock_response.status = 200
    mock_response.read = AsyncMock(return_value=zip_bytes)

    with patch("aiohttp.ClientSession.get", return_value=mock_response):
        await download_and_extract_async(aiohttp.ClientSession(), uri, data_dir)

    extracted_file = data_dir / "file.txt"
    assert extracted_file.exists()
    assert not zip_path.exists()
    assert extracted_file.read_text() == "content"


@pytest.mark.asyncio
async def test_download_and_extract_async_failed_download(tmp_path, capsys):
    uri = "http://example.com/test.zip"
    data_dir = tmp_path

    mock_response = AsyncMock()
    mock_response.__aenter__.return_value = mock_response
    mock_response.status = 404
    mock_response.read = AsyncMock(return_value=b"")

    with patch("aiohttp.ClientSession.get", return_value=mock_response):
        await download_and_extract_async(aiohttp.ClientSession(), uri, data_dir)

    captured = capsys.readouterr()
    assert "Failed to download" in captured.out


@pytest.mark.asyncio
async def test_download_and_extract_async_exception(tmp_path, capsys):
    uri = "http://example.com/test.zip"
    data_dir = tmp_path

    with patch("aiohttp.ClientSession.get", side_effect=Exception("Network error")):
        await download_and_extract_async(aiohttp.ClientSession(), uri, data_dir)

    captured = capsys.readouterr()
    assert "Error downloading" in captured.out


def test_download_and_extract_sync_success(tmp_path):
    uri = "http://example.com/test.zip"
    data_dir = tmp_path
    zip_filename = uri.split("/")[-1]
    zip_path = data_dir / zip_filename

    mem_zip = io.BytesIO()
    with zipfile.ZipFile(mem_zip, "w") as zf:
        zf.writestr("file.txt", "content")
    mem_zip.seek(0)
    zip_bytes = mem_zip.read()

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = zip_bytes

    with patch("requests.get", return_value=mock_response):
        download_and_extract_sync(uri, data_dir)

    extracted_file = data_dir / "file.txt"
    assert extracted_file.exists()
    assert not zip_path.exists()
    assert extracted_file.read_text() == "content"


def test_download_and_extract_sync_failed_download(tmp_path, capsys):
    uri = "http://example.com/test.zip"
    data_dir = tmp_path

    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.content = b""

    with patch("requests.get", return_value=mock_response):
        download_and_extract_sync(uri, data_dir)

    captured = capsys.readouterr()
    assert "Failed to download" in captured.out
