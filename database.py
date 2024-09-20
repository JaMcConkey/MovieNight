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
                current_movie_tmdb_id INTEGER,
                active_picker TEXT NOT NULL,
                guild_id TEXT NOT NULL,  -- server ID
                PRIMARY KEY (user_id, guild_id)  
            )
        """)

        # Create Watched Movies Table
        c.execute("""--sql
            CREATE TABLE IF NOT EXISTS watched_movies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tmdb_id INTEGER NOT NULL,  -- Store TheMovieDB ID
                movie_name TEXT,  -- Optionally keep this for reference
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
                lock_in_status TEXT,
                picker TEXT,  
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

# Updates users active status
def update_user_status(user_id, guild_id, status):
    """updates the users active status"""
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
    
# Add a watched movie using TMDb ID
def add_watched_movie(tmdb_id, movie_name, picked_by_user, date_watched, guild_id):
    if check_if_watched(tmdb_id, guild_id):
        print("Movie was already watched - ###INSERT DATE###")
        return False

    # Ensure the date is in the correct format
    if isinstance(date_watched, datetime):
        date_watched = date_watched.strftime('%Y-%m-%d')

    with sqlite3.connect("movienight.db") as conn:
        c = conn.cursor()
        # Grab last date picked to ensure the user exists and update their last pick date
        c.execute("SELECT last_date_picked FROM users WHERE user_id = ? AND guild_id = ?", (picked_by_user, guild_id))
        user_exists = c.fetchone()
        if not user_exists:
            print(f"User '{picked_by_user}' does not exist in guild {guild_id}.")
            return

        last_date_picked = user_exists[0]

        # Add movie using TMDb ID and guild_id
        c.execute("""INSERT INTO watched_movies (tmdb_id, movie_name, picked_by_user, date_watched, guild_id)
                     VALUES (?, ?, ?, ?, ?)
                  """, (tmdb_id, movie_name, picked_by_user, date_watched, guild_id))

        # Update user's last pick date if the new movie is more recent
        if last_date_picked is None or date_watched > last_date_picked:
            c.execute("""UPDATE users
                         SET last_date_picked = ?, last_movie_picked = ?
                         WHERE user_id = ? AND guild_id = ?
                      """, (date_watched, movie_name, picked_by_user, guild_id))
        print(f"Added new watched movie with TMDb ID '{tmdb_id}' to DB for guild {guild_id}")
        conn.commit()
    return True
# Check if a movie has been watched in a specific guild
def check_if_watched(tmdb_id, guild_id):
    with sqlite3.connect("movienight.db") as conn:
        c = conn.cursor()

        c.execute("""SELECT tmdb_id FROM watched_movies WHERE tmdb_id = ? AND guild_id = ?""", (tmdb_id, guild_id))
        details = c.fetchone()
        return details is not None
    

def get_last_active_picker(guild_id):
    """Returns user_id"""
    with sqlite3.connect("movienight.db") as conn:
        c = conn.cursor()
        
        # Check if you've never picked just return whoever. idc
        c.execute("""SELECT user_id, current_movie_picked
                      FROM users
                      WHERE active_picker = "True" AND last_date_picked IS NULL AND guild_id = ?
                      LIMIT 1
                   """, (guild_id,))
        user = c.fetchone()
        
        if user:
            return user
        
        #Get the longest stretch between picks and now - ASCEND
        c.execute("""SELECT user_id, current_movie_picked, last_date_picked
                      FROM users
                      WHERE active_picker = "True" AND guild_id = ?
                      ORDER BY last_date_picked ASC
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
def toggle_session_lockin(guild_id, bool_val):
    if not session_exists(guild_id):
        print("No session exists")
        return False
    val = normalize_status(bool_val)
    with sqlite3.connect('movienight.db') as conn:   
        c = conn.cursor()
        c.execute("""--sql
            UPDATE session
            SET lock_in_status = ?
            WHERE guild_id = ?
        """, (val, guild_id))
        if c.rowcount == 0:
            print("Failed to update lock_in_status or no rows were affected.")
            return False
        conn.commit()
    return True
def update_or_create_session_data(guild_id, host_id, channel_id, message_id, lock_in_status, picker):
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("""--sql
            INSERT INTO session (guild_id, host_id, channel_id, message_id, lock_in_status, picker)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                host_id = excluded.host_id,
                channel_id = excluded.channel_id,
                message_id = excluded.message_id,
                lock_in_status = excluded.lock_in_status,
                picker = excluded.picker
        """, (guild_id, host_id, channel_id, message_id, lock_in_status, picker))
        conn.commit()


def set_current_picker(guild_id, picker_id):
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("""UPDATE session SET picker = ? WHERE guild_id = ?""", (picker_id, guild_id))
        conn.commit()

def get_current_picker(guild_id):
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("""SELECT picker FROM session WHERE guild_id = ?""", (guild_id,))
        picker = c.fetchone()
        return picker[0] if picker else None
    

def get_session_data(guild_id):
    """returns host_id, channel_id, message_id,lock_in_status,current_user_picking"""
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("SELECT host_id, channel_id, message_id, lock_in_status,picker FROM session WHERE guild_id = ?", (guild_id,))
        return c.fetchone()
def remove_session_data(guild_id):
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("DELETE FROM session WHERE guild_id = ?", (guild_id,))
        conn.commit()

def session_exists(guild_id):
    """returns bool"""
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        
        c.execute("""--sql
            SELECT 1 FROM session WHERE guild_id = ?
        """, (guild_id,))
        
        return c.fetchone() is not None

def set_user_picked_movie(user_id, guild_id, tmdb_id, movie_name):
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("""--sql
                UPDATE users
                SET current_movie_tmdb_id = ?, current_movie_picked = ?
                WHERE user_id = ? AND guild_id = ?
                    """, (tmdb_id, movie_name, user_id, guild_id))
        if c.rowcount == 0:
            print("User not found while setting picked movie")
            return False
        else:
            print(f"Updated picked movie for '{user_id}' to TMDb ID '{tmdb_id}' with movie name '{movie_name}'")
            return True
def get_user_picked_movie(guild_id, user_id):
    """returns tmdb_id, movie_name"""
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()

        c.execute("""--sql
                  SELECT current_movie_tmdb_id, current_movie_picked FROM users
                  WHERE guild_id = ? AND user_id = ?                  
                  """, (guild_id, user_id))
        res = c.fetchone()
        return (res[0], res[1]) if res else (None, None)

def get_lock_in_status(guild_id):
    """return a BOOL of the status"""
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()

        c.execute("""--sql
                  SELECT lock_in_status
                  FROM session
                  WHERE guild_id = ?
        """,(guild_id,))
        res = c.fetchone()
        return res[0] == 'True' if res else False
    
#Maybe not use this
def reset_all_lockins():
    """The bot should only call this when it starts. Fixing lazy issues"""
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()

        c.execute("""--sql
                  UPDATE session
                  SET lock_in_status = "False"
                  """)
        conn.commit()
def get_all_sessions():
    """get's the guild_id of all sessions"""
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()

        c.execute("""--sql
                SELECT guild_id
                FROM session
                  """)
        
        sessions = c.fetchall()
        return [ses[0] for ses in sessions]