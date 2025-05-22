#!/usr/bin/env python3
# load_config.py

"""
Configuration loader for dynamic topology benchmarks.
Reads JSON config with the following structure:

{
  "db_params": {"host":..., "port":..., "dbname":..., "user":..., "password":...},
  "node_count": {"drone": int, "mission": int, "execution": int, "verification": int},
  "headquarters_id": str,
  "private_key_path": str,
  "scenario_params": { ... },
  // optional legacy fields: test_name, db_type, test_case, random_seed, enable_random_binding, log_output_path
}
"""
import json
from pathlib import Path

class TestConfig:
    def __init__(self, config_path: str):
        self._load(config_path)

    def _load(self, config_path: str):
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        with path.open('r', encoding='utf-8') as f:
            data = json.load(f)

        # DB connection parameters
        self.db_params = data.get('db_params', {})
        self.db_host = self.db_params.get('host', 'localhost')
        self.db_port = self.db_params.get('port', 5433)
        self.db_name = self.db_params.get('edge', '')
        self.db_user = self.db_params.get('sam', '')
        self.db_password = self.db_params.get('dooley', '')

        # Node counts
        node_count = data.get('node_count', {})
        self.num_regions = node_count.get('region', 10)
        self.num_units = node_count.get('unit', 10)
        self.num_squads = node_count.get('squad', 10)
        self.num_drones_per_squad = node_count.get('drone_per_squad', 10)
        self.num_drones = self.num_squads * self.num_drones_per_squad

        self.num_mission = node_count.get('mission', 100)
        self.num_execution = node_count.get('execution', 100)
        self.num_verification = node_count.get('verification', 100)


        # Operation-specific settings
        self.headquarters_id = data.get('headquarters_id')
        self.private_key_path = data.get('private_key_path')
        self.data_result_path = data.get('data_result_path')
        self.scale_up_nodes = data.get('scale_up_nodes', [100, 500, 1000])
        self.depths = data.get('scale_up_nodes', [2, 4, 6, 8])
        self.iterations = data.get('iterations', 100)
        self.chunk_size = data.get('chunk_size', 500)
        
        self.scenario_params = data.get('scenario_params', {})

        # Legacy/optional fields
        self.test_name = data.get('test_name')
        self.db_type = data.get('db_type')
        self.test_case = data.get('test_case')
        self.random_seed = data.get('random_seed', 42)
        self.enable_random_binding = data.get('enable_random_binding', True)
        self.log_output_path = data.get('log_output_path', 'test_log.json')

    def __str__(self):
        return (
            f"[TestConfig: drones={self.num_drones}, missions={self.num_mission}, "
            f"execution={self.num_execution}, verification={self.num_verification}]"
        )

if __name__ == '__main__':
    # Simple test
    cfg = TestConfig('config/test_large.json')
    print(cfg)
    print('DB host:', cfg.db_host)
    print('HQ ID:', cfg.headquarters_id)
