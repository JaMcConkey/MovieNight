import sqlite3
from datetime import datetime

#Connect to DB


# Create Users Table
def create_table():
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("""--sql
            CREATE TABLE IF NOT EXISTS users (
                username TEXT NOT NULL,
                real_name TEXT, -- Might remove
                last_date_picked DATE,
                last_movie_picked TEXT,
                current_movie_picked TEXT,
                active_picker TEXT NOT NULL,
                guild_id TEXT NOT NULL,  -- server ID
                PRIMARY KEY (username, guild_id)  -- Unique per guild - Just future proofing and for testing
            )
        """)

        # Create Watched Movies Table
        c.execute("""--sql
            CREATE TABLE IF NOT EXISTS watched_movies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_name TEXT NOT NULL,
                picked_by_user TEXT NOT NULL,
                date_watched DATE,
                guild_id TEXT NOT NULL,  -- Track server ID
                FOREIGN KEY (picked_by_user, guild_id) REFERENCES users(username, guild_id)
            )
        """)

#Passing in a member object, extract the username
def add_new_picker(member, guild_id):
    username = member.name
    guild_id = str(guild_id)
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        # Check if the user already exists in the given guild
        c.execute("SELECT 1 FROM users WHERE username = ? AND guild_id = ?", (username, guild_id))
        exists = c.fetchone()
        if not exists:
            c.execute("INSERT INTO users(username, active_picker, guild_id) VALUES (?,?,?)",
                      (username, "True", guild_id))
            print(f"Added {username} in guild {guild_id}")
        else:
            print(f"{username} already exists in guild {guild_id}")
        conn.commit()
def check_user_exists(username, guild_id):
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM users WHERE username = ? AND guild_id = ?", (username, guild_id))
        return c.fetchone() is not None
#User status is for weather or not they are active to pick
def update_user_status(username,guild_id,status):
    try:
        normalized_status = normalize_status(status)

        with sqlite3.connect('movienight.db') as conn:
            c = conn.cursor()

            # Update the user's status
            c.execute("UPDATE users SET active_picker = ? WHERE username = ? AND guild_id = ?", (normalized_status, username,guild_id))

            if c.rowcount == 0:
                print(f"User '{username}' not found.")
            else:
                print(f"User '{username}' status updated to {normalized_status}.")

            conn.commit()
    except ValueError as ve:
        print(f"ValueError: {ve}")
    except sqlite3.DatabaseError as db_err:
        print(f"Database error: {db_err}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def set_user_picked_movie(username,guild_id,movie):
    with sqlite3.connect('movienight.db') as conn:
        c = conn.cursor()
        c.execute("""--sql
                UPDATE users
                SET current_movie_picked = ?
                WHERE username = ? AND guild_id = ?
                    """,(movie,username,guild_id))
        if c.rowcount == 0:
            print("User not found while setting picked movie")
        else:
            print("Updated picked movie for '{username}' to '{movie}'" )
#Normalize status incase of capitalization typos, or allow "yes"
def normalize_status(status):

    true_variations = {"true", "yes", "1"}
    false_variations = {"false", "no", "0"}

    # Set to lowercase
    status_lower = status.strip().lower()

    if status_lower in true_variations:
        return "True"
    elif status_lower in false_variations:
        return "False"
    else:
        raise ValueError("Invalid status value. Use true,yes,1 or false,no,0 ")
    
def add_watched_movie(movie_name, picked_by_user, date_watched, guild_id):
    if check_if_watched(movie_name, guild_id):
        print("Movie was already watched - ###INSERT DATE###")
        return
    
    #make sure we use a datetime datetime object
    if isinstance(date_watched, datetime):
        date_watched = date_watched.strftime('%Y-%m-%d')

    with sqlite3.connect("movienight.db") as conn:
        c = conn.cursor()
        # Make sure the picked_by_user exists
        c.execute("SELECT 1 FROM users WHERE username = ? AND guild_id = ?", (picked_by_user, guild_id))
        user_exists = c.fetchone()
        if not user_exists:
            print(f"User '{picked_by_user}' does not exist in guild {guild_id}. - Check why we're getting here")
            return
        
        # Add movie and make sure to use the guild_id
        c.execute("""INSERT INTO watched_movies (movie_name, picked_by_user, date_watched, guild_id)
                     VALUES (?, ?, ?, ?)""", (movie_name, picked_by_user, date_watched, guild_id))
        print("Added new watched movie to DB for guild", guild_id)
        conn.commit()

def check_if_watched(movie_name, guild_id):
    with sqlite3.connect("movienight.db") as conn:
        c = conn.cursor()
        # Check if the movie has been watched in the specific guild
        c.execute("SELECT 1 FROM watched_movies WHERE movie_name = ? AND guild_id = ?", (movie_name, guild_id))
        details = c.fetchone()
        if details:
            return True, details
        else:
            return False, None

def get_last_active_picker(guild_id):
    with sqlite3.connect("movienight.db") as conn:
        c = conn.cursor()
        c.execute("""SELECT username FROM users
                  WHERE active_picker = "True" AND guild_id = ?
                  ORDER BY last_date_picked DESC
                  LIMIT 1
                  """, (guild_id,))
        user = c.fetchone()
        if user:
            username = user
            return username
        else:
            print("No active users :()")
            return None
def get_pickers(amount,guild_id):
    """returns username, last pick date, current movie, and if they are active"""
    with sqlite3.connect("movienight.db") as conn:
        c = conn.cursor()
        c.execute("""--sql
                  SELECT username,last_date_picked,current_movie_picked,active_picker from users
                  WHERE guild_id = ?
                  ORDER BY last_date_picked DESC
                  LIMIT ?
                  """,(amount,guild_id,))
        picker_data = c.fetchall()
        return picker_data
