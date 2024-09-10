import sqlite3
from datetime import datetime

# Create Users Table
def create_table():
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("""--sql
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT NOT NULL,  -- Store user_id as unique identifier
                real_name TEXT,  -- Might remove
                last_date_picked DATE,
                last_movie_picked TEXT,
                current_movie_picked TEXT,
                active_picker TEXT NOT NULL,
                guild_id TEXT NOT NULL,  -- server ID
                PRIMARY KEY (user_id, guild_id)  
            )
        """)

        # Create Watched Movies Table
        c.execute("""--sql
            CREATE TABLE IF NOT EXISTS watched_movies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_name TEXT NOT NULL,
                picked_by_user TEXT NOT NULL,  -- user_id instead of username
                date_watched DATE,
                guild_id TEXT NOT NULL,  -- Track server ID
                FOREIGN KEY (picked_by_user, guild_id) REFERENCES users(user_id, guild_id)
            )
        """)
        c.execute("""--sql
            CREATE TABLE IF NOT EXISTS session (
                guild_id TEXT NOT NULL,
                host_id TEXT,  
                channel_id TEXT, --We'll try reusing the same channel and message on restarts and keep it updated
                message_id TEXT,  
                PRIMARY KEY (guild_id)
            )
        """);

# Pass in the member ID- Not the member itself
def add_new_picker(member, guild_id):
    user_id = str(member)  # Use the unique user ID
    guild_id = str(guild_id)
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        # Check if the user already exists in the given guild
        c.execute("SELECT 1 FROM users WHERE user_id = ? AND guild_id = ?", (user_id, guild_id))
        exists = c.fetchone()
        if not exists:
            c.execute("INSERT INTO users(user_id, active_picker, guild_id) VALUES (?,?,?)",
                      (user_id, "True", guild_id))
            print(f"Added {user_id} in guild {guild_id}")
        else:
            print(f"{user_id} already exists in guild {guild_id}")
        conn.commit()

# Check if a user exists using their user_id
def check_user_exists(user_id, guild_id):
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM users WHERE user_id = ? AND guild_id = ?", (user_id, guild_id))
        return c.fetchone() is not None

# Update user's status using their user_id
def update_user_status(user_id, guild_id, status):
    try:
        normalized_status = normalize_status(status)

        with sqlite3.connect('movienight.db') as conn:
            c = conn.cursor()

            # Update the user's status
            c.execute("UPDATE users SET active_picker = ? WHERE user_id = ? AND guild_id = ?", (normalized_status, user_id, guild_id))

            if c.rowcount == 0:
                print(f"User '{user_id}' not found.")
            else:
                print(f"User '{user_id}' status updated to {normalized_status}.")

            conn.commit()
    except ValueError as ve:
        print(f"ValueError: {ve}")
    except sqlite3.DatabaseError as db_err:
        print(f"Database error: {db_err}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Set the user's picked movie using their user_id
def set_user_picked_movie(user_id, guild_id, movie):
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("""--sql
                UPDATE users
                SET current_movie_picked = ?
                WHERE user_id = ? AND guild_id = ?
                    """, (movie, user_id, guild_id))
        if c.rowcount == 0:
            print("User not found while setting picked movie")
            return False
        else:
            print(f"Updated picked movie for '{user_id}' to '{movie}'")
            return True

# Normalize status incase of capitalization typos, or allow "yes"
def normalize_status(status):
    """takes a string of true/false,yes/no,1/0 or a bool"""
    true_variations = {"true", "yes", "1"}
    false_variations = {"false", "no", "0"}
    #If we pass in a bool it will convert it
    if isinstance(status, bool):
        return "True" if status else "False"
    # Set to lowercase
    status_lower = status.strip().lower()

    if status_lower in true_variations:
        return "True"
    elif status_lower in false_variations:
        return "False"
    else:
        raise ValueError("Invalid status value. Use true,yes,1 or false,no,0 ")

# Add a watched movie using the user's user_id
def add_watched_movie(movie_name, picked_by_user, date_watched, guild_id):
    if check_if_watched(movie_name, guild_id):
        print("Movie was already watched - ###INSERT DATE###")
        return False

    # Ensure the date is in the correct format
    if isinstance(date_watched, datetime):
        date_watched = date_watched.strftime('%Y-%m-%d')

    with sqlite3.connect("movienight.db") as conn:
        c = conn.cursor()
        # Make sure the picked_by_user exists
        c.execute("SELECT 1 FROM users WHERE user_id = ? AND guild_id = ?", (picked_by_user, guild_id))
        user_exists = c.fetchone()
        if not user_exists:
            print(f"User '{picked_by_user}' does not exist in guild {guild_id}. - Check why we're getting here")
            return

        # Add movie and make sure to use the guild_id
        c.execute("""INSERT INTO watched_movies (movie_name, picked_by_user, date_watched, guild_id)
                     VALUES (?, ?, ?, ?)""", (movie_name, picked_by_user, date_watched, guild_id))
        print(f"Added new watched movie to DB for guild {guild_id}")
        conn.commit()

# Check if a movie has been watched in a specific guild
def check_if_watched(movie_name, guild_id):
    """returns movie_name,"""
    with sqlite3.connect("movienight.db") as conn:
        c = conn.cursor()
        c.execute("""SELECT movie_name, picked_by_user, date_watched
                   FROM watched_movies WHERE movie_name = ? AND guild_id = ?
                  """, (movie_name, guild_id))
        details = c.fetchone()
        if details:
            return True, details
        else:
            return False, None

# Get the last active picker by user_id
def get_last_active_picker(guild_id):
    """returns user_id, current_movie_picked"""
    with sqlite3.connect("movienight.db") as conn:
        c = conn.cursor()
        c.execute("""SELECT user_id, current_movie_picked FROM users
                  WHERE active_picker = "True" AND guild_id = ?
                  ORDER BY last_date_picked DESC
                  LIMIT 1
                  """, (guild_id,))
        user = c.fetchone()
        if user:
            return user
        else:
            print("No active users :(")
            return None

# Get a list of pickers and their details by user_id
def get_pickers(amount, guild_id):
    """returns user_id, last pick date, current movie, and if they are active - Sorted by last to pick"""
    with sqlite3.connect("movienight.db") as conn:
        c = conn.cursor()
        c.execute("""--sql
                  SELECT user_id, last_date_picked, current_movie_picked, active_picker FROM users
                  WHERE guild_id = ?
                  ORDER BY last_date_picked DESC
                  LIMIT ?
                  """, (guild_id, amount,))
        picker_data = c.fetchall()
        return picker_data 

### Session functions
def update_or_create_session_data(guild_id, host_id, channel_id, message_id):
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("""--sql
            INSERT INTO session (guild_id, host_id, channel_id, message_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                host_id = excluded.host_id,
                channel_id = excluded.channel_id,
                message_id = excluded.message_id
        """, (guild_id, host_id, channel_id, message_id))
        conn.commit()
def get_session_data(guild_id):
    """returns host_id, channel_id, message_id"""
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("SELECT host_id, channel_id, message_id FROM session WHERE guild_id = ?", (guild_id,))
        return c.fetchone()
def remove_session_data(guild_id):
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("DELETE FROM session WHERE guild_id = ?", (guild_id,))
        conn.commit()

def session_exists(guild_id):
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        
        c.execute("""--sql
            SELECT 1 FROM session WHERE guild_id = ?
        """, (guild_id,))
        
        return c.fetchone() is not None
