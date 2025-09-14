Komendy parsera:

# Domyślny test (3 strony z debugiem)
python3 license_plate_parser.py

# Test z debugiem (1 strona)
python3 license_plate_parser.py --debug 1

# Konkretny zakres stron
python3 license_plate_parser.py [start_page] [end_page]

Przykłady komend
# Małe testy
python3 license_plate_parser.py 1 5         # 5 stron
python3 license_plate_parser.py 1 20        # 20 stron  
python3 license_plate_parser.py 1 100       # 100 stron

# Większe testy
python3 license_plate_parser.py 1 1000      # 1000 stron
python3 license_plate_parser.py 1000 2000   # Strony 1000-2000

# Pełne parsowanie
python3 license_plate_parser.py 1 85519     # WSZYSTKIE strony

