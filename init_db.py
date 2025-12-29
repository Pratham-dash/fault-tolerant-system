# init_db.py - create db 1 time only
from Dedup import init_dedup_table
from Events import init_events_table

print("🗄️ Initializing Database...")

# Create dedup table
init_dedup_table()
print("✅ Dedup table created!")

# Create events table
init_events_table()
print("✅ Events table created!")

print("✅ Database fully ready!")
print("Next: Run python3 Input_JSON.py")
