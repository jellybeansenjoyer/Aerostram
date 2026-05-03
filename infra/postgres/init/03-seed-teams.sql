-- 10 F1 2024 constructor teams
INSERT INTO teams (team_id, team_name, power_unit, base_country, principal) VALUES
('REDBULL',     'Oracle Red Bull Racing',         'Honda RBPT', 'Austria',     'Christian Horner'),
('FERRARI',     'Scuderia Ferrari',               'Ferrari',    'Italy',       'Frederic Vasseur'),
('MERCEDES',    'Mercedes-AMG Petronas',          'Mercedes',   'Germany',     'Toto Wolff'),
('MCLAREN',     'McLaren Formula 1 Team',         'Mercedes',   'UK',          'Andrea Stella'),
('ASTONMARTIN', 'Aston Martin Aramco',            'Mercedes',   'UK',          'Mike Krack'),
('ALPINE',      'BWT Alpine F1 Team',             'Renault',    'France',      'Bruno Famin'),
('WILLIAMS',    'Williams Racing',                'Mercedes',   'UK',          'James Vowles'),
('HAAS',        'MoneyGram Haas F1 Team',         'Ferrari',    'USA',         'Guenther Steiner'),
('RB',          'Visa Cash App RB',               'Honda RBPT', 'Italy',       'Laurent Mekies'),
('SAUBER',      'Stake F1 Team Kick Sauber',      'Ferrari',    'Switzerland', 'Alessandro Alunni Bravi')
ON CONFLICT (team_id) DO NOTHING;
