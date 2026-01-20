from probe_manager import Probe, ProbeManager

manager = ProbeManager('data/data.json')

manager.add_state_tags()

T13_probes = manager.find_probes_by_name_substring('T13')
print(f"Найдено проб T13: {len(T13_probes)}")
manager.add_tag_to_probes('T13', T13_probes)

T15_probes = manager.find_probes_by_name_substring('T15')
print(f"Найдено проб T15: {len(T15_probes)}")
manager.add_tag_to_probes('T15', T13_probes)

T17_probes = manager.find_probes_by_name_substring('T17')
print(f"Найдено проб T17: {len(T17_probes)}")
manager.add_tag_to_probes('T17', T13_probes)
