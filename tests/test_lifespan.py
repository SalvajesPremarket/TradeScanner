import asyncio
import os
import unittest
from unittest.mock import patch

import app as app_module
from app import app


class LifespanShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_lifespan_releases_lease_only_after_scan_loop_finishes(self):
        async def finished_scan_loop(_stop, _worker_id):
            return None

        with patch.dict(os.environ, {
            "TRADESCANNER_SHUTDOWN_HISTORY_GRACE_SECONDS": "0",
            "TRADESCANNER_SHUTDOWN_SCAN_GRACE_SECONDS": "1",
            "TRADESCANNER_AUTO_SCAN_ENABLED": "false",
        }, clear=True), patch.object(app_module, "reset_engine_shutdown") as reset_engine, patch.object(
            app_module, "request_engine_shutdown", return_value={"completed": 0, "pending": []}
        ) as stop_history, patch.object(
            app_module, "start_schwab_stream"
        ), patch.object(app_module, "stop_schwab_stream"), patch.object(
            app_module, "_automatic_scan_loop", side_effect=finished_scan_loop
        ), patch.object(app_module, "SnapshotStore") as store_class:
            async with app_module.lifespan(app):
                task = app.state.scan_task
                await asyncio.sleep(0)

            reset_engine.assert_called_once_with()
            stop_history.assert_called_once_with(0.0)
            store_class.return_value.release_lease.assert_called_once_with(
                app_module.SCAN_LEASE_KEY, app.state.scan_worker_id
            )
            self.assertTrue(task.done())

    async def test_lifespan_retains_lease_when_scan_loop_exceeds_shutdown_grace(self):
        scan_started = asyncio.Event()
        never_release = asyncio.Event()

        async def blocked_scan_loop(_stop, _worker_id):
            scan_started.set()
            await never_release.wait()

        with patch.dict(os.environ, {
            "TRADESCANNER_SHUTDOWN_HISTORY_GRACE_SECONDS": "0",
            "TRADESCANNER_SHUTDOWN_SCAN_GRACE_SECONDS": "0",
            "TRADESCANNER_AUTO_SCAN_ENABLED": "false",
        }, clear=True), patch.object(app_module, "reset_engine_shutdown"), patch.object(
            app_module, "request_engine_shutdown", return_value={"completed": 0, "pending": []}
        ) as stop_history, patch.object(
            app_module, "start_schwab_stream"
        ), patch.object(app_module, "stop_schwab_stream"), patch.object(
            app_module, "_automatic_scan_loop", side_effect=blocked_scan_loop
        ), patch.object(app_module, "SnapshotStore") as store_class:
            try:
                async with app_module.lifespan(app):
                    task = app.state.scan_task
                    await asyncio.wait_for(scan_started.wait(), timeout=1)

                stop_history.assert_called_once_with(0.0)
                store_class.assert_not_called()
                self.assertFalse(task.done())
            finally:
                if "task" in locals() and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
