import base64
import unittest
import json
import main
import contextlib
import io
from clickstream.simulator import VisitSimulator

class TestEventRecv(unittest.TestCase):

    simulator = None

    def setUp(self):
        self.simulator = VisitSimulator()

    def test_event_recv(self):
        visit = self.simulator.new_visit(force_purchase=True)
        event = type("cloudevent", (object,), {"attributes": {}, "data": {}})
        event.data = {
            "message": {
                "data": base64.b64encode(json.dumps(visit).encode("utf-8"))
            }
        }

        with contextlib.redirect_stdout(io.StringIO()) as f:
            main.subscribe(event)
        self.assertTrue(visit["session_id"] in f.getvalue())

if __name__ == '__main__':
    unittest.main()
