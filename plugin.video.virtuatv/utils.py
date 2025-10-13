import xbmcaddon
import xbmcvfs
import os
try:
    import mysql.connector as mysql
except ImportError:
    mysql = None
import sqlite3

ADDON_ID = 'plugin.video.virtuatv'
ADDON = xbmcaddon.Addon(ADDON_ID)
SETTINGS_DIR = xbmcvfs.translatePath(ADDON.getAddonInfo('profile'))

def get_database_connection():
    try:
        if ADDON.getSettingBool('mysql_enabled'):
            try:
                conn = mysql.connect(
                    host=ADDON.getSetting('db_host'),
                    user=ADDON.getSetting('db_user'),
                    password=ADDON.getSetting('db_pass'),
                    database=ADDON.getSetting('db_name')
                )
                cursor = conn.cursor()
                cursor.execute("SELECT version FROM version")
                db_version = cursor.fetchone()[0]
                return conn, cursor, 'mysql', db_version
            except Exception as e:
                virtu_log(f"Failed to connect to MySQL: {str(e)}", virtu_logERROR)
                return None, None, None, None
        else:
            db_path = xbmcvfs.translatePath("special://database/MyVideos*.db")
            files = xbmcvfs.glob(db_path)
            if not files:
                virtu_log("No SQLite database found", virtu_logERROR)
                return None, None, None, None
            db_path = files[-1]
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT idVersion FROM Version")
            db_version = cursor.fetchone()[0]
            return conn, cursor, 'sqlite', db_version
    except Exception as e:
        virtu_log(f"Error connecting to database: {str(e)}", virtu_logERROR)
        return None, None, None, None