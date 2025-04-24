import unittest
import json

from clickstream import simulator

class TestSimulatorSchema(unittest.TestCase):

    def test_simulator_schema(self):
        s = simulator.VisitSimulator()
        visit = s.new_visit()
        visit_json = json.dumps(visit)
        self.assertNotEqual(visit_json, "")

if __name__ == '__main__':
    unittest.main()
