import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from werkzeug.exceptions import NotFound

import indafoto_archive_explorer as explorer


SENSITIVE_ERROR = "database failed at /private/archive/indafoto.db"


class FailingCursor:
    def execute(self, *args, **kwargs):
        raise RuntimeError(SENSITIVE_ERROR)


class FailingConnection:
    def cursor(self):
        return FailingCursor()

    def close(self):
        pass


class SecurityBoundaryTests(unittest.TestCase):
    def setUp(self):
        explorer.app.config.update(TESTING=True)
        self.logger_disabled = explorer.logger.disabled
        explorer.logger.disabled = True
        self.client = explorer.app.test_client()

    def tearDown(self):
        explorer.logger.disabled = self.logger_disabled

    def test_image_route_serves_nested_archive_files_and_optional_prefix(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            archive_dir = Path(temp_dir) / "archive"
            image_path = archive_dir / "author" / "album" / "family..photo.jpg"
            image_path.parent.mkdir(parents=True)
            image_path.write_bytes(b"local archive image")

            with patch.dict("os.environ", {"ARCHIVE_PATH": str(archive_dir)}):
                for route in (
                    "/serve_image/author/album/family..photo.jpg",
                    "/serve_image/indafoto_archive/author/album/family..photo.jpg",
                ):
                    with self.subTest(route=route):
                        response = self.client.get(route)
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(response.data, b"local archive image")
                        response.close()

    def test_archive_path_rejects_traversal_absolute_paths_and_escaping_symlinks(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            archive_dir = root / "archive"
            archive_dir.mkdir()
            outside_file = root / "private.jpg"
            outside_file.write_bytes(b"private")
            (archive_dir / "escape.jpg").symlink_to(outside_file)

            malicious_paths = (
                "../private.jpg",
                "..\\private.jpg",
                "%2e%2e/private.jpg",
                "%2e%2e%2fprivate.jpg",
                str(outside_file),
                "escape.jpg",
            )
            with patch.dict("os.environ", {"ARCHIVE_PATH": str(archive_dir)}):
                for image_path in malicious_paths:
                    with self.subTest(image_path=image_path):
                        with self.assertRaises(NotFound):
                            explorer.resolve_archive_image_path(image_path)

                response = self.client.get("/serve_image/%252e%252e%252fprivate.jpg")
                self.assertEqual(response.status_code, 404)
                response.close()

    def test_database_api_errors_do_not_expose_exception_details(self):
        requests = (
            ("post", "/api/mark_image", {"image_id": 1}, 500, None),
            ("post", "/api/image_note", {"image_id": 1}, 500, None),
            ("get", "/api/image_note/1", None, 500, None),
            ("get", "/api/favorite_authors", None, 500, None),
            ("post", "/api/favorite_authors", {"author_name": "author"}, 500, None),
            ("delete", "/api/favorite_authors/author", None, 500, None),
            ("patch", "/api/favorite_authors/author", {"priority": 1}, 500, None),
        )

        with patch.object(explorer, "get_db", return_value=FailingConnection()):
            for method, route, payload, status, success in requests:
                with self.subTest(route=route):
                    response = getattr(self.client, method)(route, json=payload)
                    self.assertEqual(response.status_code, status)
                    self.assertEqual(response.get_json().get("success"), success)
                    self.assertEqual(response.get_json()["error"], explorer.INTERNAL_ERROR_MESSAGE)
                    self.assertNotIn(SENSITIVE_ERROR, response.get_data(as_text=True))

    def test_banned_author_api_errors_do_not_expose_exception_details(self):
        requests = (
            ("post", "/api/banned_authors", {"author": "author", "reason": "reason"}, "ban_author"),
            ("delete", "/api/banned_authors/author", None, "unban_author"),
            ("post", "/api/banned_authors/author/cleanup", None, "cleanup_banned_author_content"),
        )

        with patch.object(explorer, "indafoto_init_db", return_value=FailingConnection()):
            for method, route, payload, function_name in requests:
                with self.subTest(route=route), patch.object(
                    explorer, function_name, side_effect=RuntimeError(SENSITIVE_ERROR)
                ):
                    response = getattr(self.client, method)(route, json=payload)
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(
                        response.get_json(),
                        {"success": False, "error": explorer.INTERNAL_ERROR_MESSAGE},
                    )
                    self.assertNotIn(SENSITIVE_ERROR, response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
