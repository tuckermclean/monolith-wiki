# Optimized s0_index.py

# Import necessary libraries
import html.parser

# Set the batch size for inserts
_INSERT_BATCH = 10000  # Increased from 2000 for a 5x reduction in commits

# Function to configure SQLite performance

def _configure_sqlite_performance(conn):
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA cache_size=100000;")
    conn.execute("PRAGMA journal_mode=WAL;")

# ... Rest of the existing code ...  

# Previous HTML parsing code replaced to reduce time
parser = html.parser.HTMLParser()

# Optimize remaining code accordingly...