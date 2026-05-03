-- 10 F1 2024 circuits
-- circuit_id MUST match the third segment of session_id from Phase 2 simulator
-- e.g. "RACE_2024_MONZA_R1" -> circuit_id = "MONZA"
INSERT INTO circuits (circuit_id, circuit_name, country, city, length_km, num_laps, drs_zones, pit_loss_time_s, overtaking_diff) VALUES
('BAHRAIN',    'Bahrain International Circuit',     'Bahrain',     'Sakhir',      5.412, 57, 3, 25.0, 'EASY'),
('JEDDAH',     'Jeddah Corniche Circuit',            'Saudi Arabia','Jeddah',      6.174, 50, 3, 26.5, 'MEDIUM'),
('MELBOURNE',  'Albert Park Circuit',                'Australia',   'Melbourne',   5.278, 58, 4, 27.0, 'EASY'),
('MONZA',      'Autodromo Nazionale Monza',          'Italy',       'Monza',       5.793, 53, 2, 22.0, 'EASY'),
('SILVERSTONE','Silverstone Circuit',                'UK',          'Silverstone', 5.891, 52, 2, 20.0, 'MEDIUM'),
('MONACO',     'Circuit de Monaco',                  'Monaco',      'Monte Carlo', 3.337, 78, 1, 23.0, 'HARD'),
('SPA',        'Circuit de Spa-Francorchamps',       'Belgium',     'Spa',         7.004, 44, 2, 21.0, 'MEDIUM'),
('ZANDVOORT',  'Circuit Zandvoort',                  'Netherlands', 'Zandvoort',   4.259, 72, 2, 22.0, 'HARD'),
('SUZUKA',     'Suzuka International Racing Course', 'Japan',       'Suzuka',      5.807, 53, 1, 20.0, 'MEDIUM'),
('COTA',       'Circuit of the Americas',            'USA',         'Austin',      5.513, 56, 2, 21.0, 'EASY')
ON CONFLICT (circuit_id) DO NOTHING;
