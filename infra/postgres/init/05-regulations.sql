-- F1 2024 season regulations
INSERT INTO regulations (regulation_id, season, max_fuel_load_kg, min_weight_kg, drs_activation_gap_s, mandatory_compounds, pit_lane_speed_limit_kph, fastest_lap_bonus_point)
VALUES ('F1_2024', 2024, 110.0, 798.0, 1.0, 2, 80, true)
ON CONFLICT (regulation_id) DO NOTHING;
