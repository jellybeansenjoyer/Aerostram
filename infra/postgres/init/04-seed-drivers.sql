-- 20 F1 2024 drivers mapped to Phase 2 simulation IDs DRV_01..DRV_20
-- driver_id MUST match the driver_id field produced by TelemetrySimulator (Phase 2)
INSERT INTO drivers (driver_id, full_name, abbreviated_name, team_id, car_number, nationality, championships, race_wins) VALUES
('DRV_01', 'Max Verstappen',    'VER', 'REDBULL',     1,  'Dutch',       4,  59),
('DRV_02', 'Sergio Perez',      'PER', 'REDBULL',     11, 'Mexican',     0,   6),
('DRV_03', 'Charles Leclerc',   'LEC', 'FERRARI',     16, 'Monegasque',  0,   5),
('DRV_04', 'Carlos Sainz',      'SAI', 'FERRARI',     55, 'Spanish',     0,   3),
('DRV_05', 'Lewis Hamilton',    'HAM', 'MERCEDES',    44, 'British',     7, 103),
('DRV_06', 'George Russell',    'RUS', 'MERCEDES',    63, 'British',     0,   2),
('DRV_07', 'Lando Norris',      'NOR', 'MCLAREN',     4,  'British',     0,   3),
('DRV_08', 'Oscar Piastri',     'PIA', 'MCLAREN',     81, 'Australian',  0,   2),
('DRV_09', 'Fernando Alonso',   'ALO', 'ASTONMARTIN', 14, 'Spanish',     2,  32),
('DRV_10', 'Lance Stroll',      'STR', 'ASTONMARTIN', 18, 'Canadian',    0,   0),
('DRV_11', 'Pierre Gasly',      'GAS', 'ALPINE',      10, 'French',      0,   1),
('DRV_12', 'Esteban Ocon',      'OCO', 'ALPINE',      31, 'French',      0,   1),
('DRV_13', 'Alexander Albon',   'ALB', 'WILLIAMS',    23, 'Thai',        0,   0),
('DRV_14', 'Logan Sargeant',    'SAR', 'WILLIAMS',    2,  'American',    0,   0),
('DRV_15', 'Yuki Tsunoda',      'TSU', 'RB',          22, 'Japanese',    0,   0),
('DRV_16', 'Daniel Ricciardo',  'RIC', 'RB',          3,  'Australian',  0,   8),
('DRV_17', 'Kevin Magnussen',   'MAG', 'HAAS',        20, 'Danish',      0,   0),
('DRV_18', 'Nico Hulkenberg',   'HUL', 'HAAS',        27, 'German',      0,   0),
('DRV_19', 'Valtteri Bottas',   'BOT', 'SAUBER',      77, 'Finnish',     0,  10),
('DRV_20', 'Zhou Guanyu',       'ZHO', 'SAUBER',      24, 'Chinese',     0,   0)
ON CONFLICT (driver_id) DO NOTHING;
