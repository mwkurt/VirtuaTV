import xbmc
import xbmcgui
import xbmcplugin
import xbmcvfs
import xbmcaddon
import os
import sys
import json
import random
import threading
import xml.etree.ElementTree as ET
import sqlite3
import tempfile
import subprocess
import re
import uuid
import datetime
import time
import traceback
import hashlib
import xml.dom.minidom  # Added for pretty-printing XMLTV
try:
    import mysql.connector
except ImportError:
    mysql = None

# Logging level
LOG_LEVEL_MAP = {'0': 'verbose', '1': 'info', '2': 'none'}
LOG_LEVEL = LOG_LEVEL_MAP.get(xbmcaddon.Addon('plugin.video.virtuatv').getSetting('log_level'), 'info')
LOG_VERBOSE = LOG_LEVEL == 'verbose'
LOG_INFO = LOG_LEVEL == 'info' or LOG_VERBOSE
virtu_logDEBUG = 0
virtu_logINFO = 1
virtu_logWARNING = 2
virtu_logERROR = 3

def virtu_log(message, level=virtu_logINFO):
    if level == virtu_logDEBUG and not LOG_VERBOSE:
        return
    if level == virtu_logINFO and not LOG_INFO:
        return
    xbmc_level = (
        xbmc.LOGINFO if level in (virtu_logDEBUG, virtu_logINFO)
        else xbmc.LOGWARNING if level == virtu_logWARNING
        else xbmc.LOGERROR
    )
    xbmc.log(f"VirtuaTV: {message}", xbmc_level)

# Constants and initial setup
ADDON_ID = 'plugin.video.virtuatv'
ADDON = xbmcaddon.Addon(ADDON_ID)
ADDON_URL = sys.argv[0]
try:
    ADDON_HANDLE = int(sys.argv[1])
except IndexError:
    ADDON_HANDLE = -1
#playback_monitor = PlaybackMonitor()  # Instantiate PlaybackMonitor
SETTINGS_DIR = xbmcvfs.translatePath(ADDON.getAddonInfo('profile'))
CHANNELS_FILE = os.path.join(SETTINGS_DIR, 'channels.json')
INSTANCE_FILE = os.path.join(SETTINGS_DIR, 'instance.json')
INSTANCE_ID = None
SHARED_FOLDER_LOCK = threading.Lock()
shared_folder = None
shared_folder_notified = False  # Global flag for shared folder errors
instance_lock_file = os.path.join(SETTINGS_DIR, 'instance.lock')

# Add/replace these constants after your existing constants (e.g., after instance_lock_file)
SETTINGS_DIR = xbmcvfs.translatePath(ADDON.getAddonInfo('profile'))
CHANNELS_FILE = os.path.join(SETTINGS_DIR, 'channels.json')
LAST_CHANNEL_JSON = os.path.join(SETTINGS_DIR, 'last_channel.json')
MEDIA_PATH = xbmcvfs.translatePath(os.path.join(ADDON.getAddonInfo('path'), 'resources', 'media'))
ACTION_PREVIOUS_MENU = (9, 10, 92, 216, 247, 257, 275, 61467, 61448)
ACTION_SELECT_ITEM = 7
ACTION_MOVE_UP = 3
ACTION_MOVE_DOWN = 4
ACTION_MOVE_LEFT = 1
ACTION_MOVE_RIGHT = 2
ACTION_SHOW_INFO = 11
ACTION_CONTEXT_MENU = 117

# Initialize instance ID
def init_instance_id():
    global INSTANCE_ID
    if xbmcvfs.exists(INSTANCE_FILE):
        try:
            with xbmcvfs.File(INSTANCE_FILE, 'r') as f:
                data = json.load(f)
                INSTANCE_ID = data.get('instance_id')
        except Exception as e:
            virtu_log(f"Failed to load instance ID: {str(e)}", virtu_logERROR)
    if not INSTANCE_ID:
        INSTANCE_ID = str(uuid.uuid4())
        try:
            with xbmcvfs.File(INSTANCE_FILE, 'w') as f:
                f.write(json.dumps({'instance_id': INSTANCE_ID}))
            virtu_log(f"Generated new instance ID: {INSTANCE_ID}", virtu_logINFO)
        except Exception as e:
            virtu_log(f"Failed to save instance ID: {str(e)}", virtu_logERROR)
            
# Replace your existing check_settings_lock function
def check_settings_lock():
    """Check virtuatv_settings.lock for conflicting auto_regen settings from other clients."""
    global shared_folder
    storage_path = debug_settings()  # Get validated storage path
    try:
        lock_file = os.path.join(storage_path, 'virtuatv_settings.lock')
        if not xbmcvfs.exists(lock_file):
            virtu_log("check_settings_lock: No settings lock file found", virtu_logDEBUG)
            return None
        with xbmcvfs.File(lock_file, 'r') as f:
            data = json.load(f)
        client_id = data.get('client_id')
        auto_regen = data.get('auto_regen', False)
        timestamp = data.get('timestamp')
        lock_time = datetime.datetime.fromisoformat(timestamp)
        time_diff = (datetime.datetime.now(datetime.timezone.utc) - lock_time).total_seconds()
        if time_diff > 24 * 3600:
            virtu_log(f"check_settings_lock: Stale lock file found (age: {time_diff/3600:.1f} hours), ignoring", virtu_logINFO)
            return None
        if client_id != INSTANCE_ID:
            return {
                'client_id': client_id,
                'auto_regen': auto_regen
            }
        virtu_log("check_settings_lock: Lock file belongs to this client", virtu_logDEBUG)
        return None
    except Exception as e:
        virtu_log(f"check_settings_lock: Error reading settings lock file: {str(e)}", virtu_logERROR)
        return None

def update_settings_lock(auto_regen=None):
    """Update virtuatv_settings.lock with current client auto_regen setting."""
    global shared_folder, INSTANCE_ID
    storage_path = debug_settings()  # Get validated storage path
    try:
        lock_file = os.path.join(storage_path, 'virtuatv_settings.lock')
        current_data = {}
        if xbmcvfs.exists(lock_file):
            try:
                with xbmcvfs.File(lock_file, 'r') as f:
                    current_data = json.load(f)
            except Exception as e:
                virtu_log(f"update_settings_lock: Error reading existing lock file: {str(e)}", virtu_logWARNING)
        if current_data.get('client_id') == INSTANCE_ID or not current_data:
            data = {
                'client_id': INSTANCE_ID,
                'auto_regen': ADDON.getSettingBool('auto_regen') if auto_regen is None else auto_regen,
                'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            temp_lock = os.path.join(xbmcvfs.translatePath("special://temp"), f"virtuatv_settings_lock_{os.getpid()}.json")
            with xbmcvfs.File(temp_lock, 'w') as f:
                f.write(json.dumps(data, indent=2))
            if xbmcvfs.copy(temp_lock, lock_file):
                xbmcvfs.delete(temp_lock)
                virtu_log(f"update_settings_lock: Updated settings lock file for client {INSTANCE_ID}", virtu_logINFO)
            else:
                xbmcvfs.delete(temp_lock)
                virtu_log(f"update_settings_lock: Failed to update settings lock file", virtu_logERROR)
                return False
        else:
            virtu_log(f"update_settings_lock: Skipped update, lock file owned by client {current_data.get('client_id')}", virtu_logDEBUG)
        return True
    except Exception as e:
        virtu_log(f"update_settings_lock: Error updating settings lock file: {str(e)}", virtu_logERROR)
        return False

# Debug settings and validate shared_folder
def debug_settings():
    """Log all addon settings and validate shared_folder."""
    global ADDON, shared_folder, shared_folder_notified
    ADDON = xbmcaddon.Addon(ADDON_ID)  # Refresh ADDON for fresh settings
    virtu_log("Refreshed ADDON in debug_settings", virtu_logINFO)
    pid = os.getpid()
    thread_id = threading.current_thread().ident
    virtu_log(f"debug_settings called in PID {pid}, Thread {thread_id}", virtu_logINFO)
    shared_folder = ADDON.getSetting('shared_folder').replace('\\', '/')
    shared_folder_norm = xbmcvfs.translatePath(shared_folder).replace('\\', '/') if shared_folder else SETTINGS_DIR
    settings = {
        'shared_folder': shared_folder,
        'shared_folder_normalized': shared_folder_norm,
        'log_level': ADDON.getSetting('log_level'),
        'sync_interval': ADDON.getSetting('sync_interval'),
        'number_of_channels': ADDON.getSetting('number_of_channels'),
        'auto_regen': ADDON.getSettingBool('auto_regen'),
        'service_mode': ADDON.getSetting('service_mode')
    }
    virtu_log(f"Debug settings: {json.dumps(settings, indent=2)}", virtu_logINFO)
    # Validate shared_folder
    if shared_folder and not xbmcvfs.exists(shared_folder_norm):
        virtu_log(f"Shared folder inaccessible: {shared_folder_norm}, resetting to addon_data folder", virtu_logWARNING)
        if not shared_folder_notified:
            xbmcgui.Dialog().notification("VirtuaTV", f"Shared folder inaccessible: {shared_folder_norm}. Using addon_data folder.", xbmcgui.NOTIFICATION_ERROR, 1000)
            shared_folder_notified = True
        ADDON.setSetting('shared_folder', '')
        shared_folder = ''
        shared_folder_norm = SETTINGS_DIR
    else:
        shared_folder_notified = False
    return shared_folder_norm

# Ensure single instance of auto_regen_loop
def acquire_instance_lock():
    """Acquire a lock to prevent multiple auto_regen_loop instances."""
    global instance_lock_file
    if xbmcvfs.exists(instance_lock_file):
        virtu_log("Instance lock already exists, skipping auto_regen_loop", virtu_logWARNING)
        return False
    try:
        with xbmcvfs.File(instance_lock_file, 'w') as f:
            f.write(str(os.getpid()))
        virtu_log("Acquired instance lock", virtu_logDEBUG)
        return True
    except Exception as e:
        virtu_log(f"Failed to acquire instance lock: {str(e)}", virtu_logERROR)
        return False

# Wait for Kodi to initialize settings
# ... (existing code before this point remains unchanged)
monitor = xbmc.Monitor()
monitor.waitForAbort(2) # Wait 2 seconds to ensure settings are loaded
init_instance_id()
debug_settings()
virtu_log(f"VirtuaTV: Channels file path: {CHANNELS_FILE}", virtu_logINFO)

# ... (rest of the code remains unchanged)

class SettingsMonitor(xbmc.Monitor):
    def __init__(self):
        super(SettingsMonitor, self).__init__()
        self.last_notified_path = None
    def onSettingsChanged(self):
        global shared_folder, LOG_LEVEL, LOG_VERBOSE, LOG_INFO, shared_folder_notified
        new_path = ADDON.getSetting('shared_folder').replace('\\', '/')
        new_path_norm = xbmcvfs.translatePath(new_path).replace('\\', '/') if new_path else SETTINGS_DIR
        if new_path != shared_folder and new_path != self.last_notified_path:
            if new_path:
                if not xbmcvfs.exists(new_path_norm):
                    if xbmcvfs.mkdirs(new_path_norm):
                        virtu_log(f"Created new shared folder: {new_path_norm}", virtu_logINFO)
                        xbmcgui.Dialog().notification("VirtuaTV", f"Created shared folder: {new_path_norm}", xbmcgui.NOTIFICATION_INFO, 1000)
                    else:
                        xbmcgui.Dialog().notification("VirtuaTV", f"Failed to create folder: {new_path_norm}. Using addon_data folder.", xbmcgui.NOTIFICATION_ERROR, 1000)
                        virtu_log(f"Failed to create folder: {new_path_norm}", virtu_logERROR)
                        ADDON.setSetting('shared_folder', '')
                        new_path = ''
                        new_path_norm = SETTINGS_DIR
            shared_folder = new_path
            self.last_notified_path = new_path
            shared_folder_notified = False
            xbmcgui.Dialog().notification("VirtuaTV", f"M3U storage folder set to: {new_path_norm}", xbmcgui.NOTIFICATION_INFO, 1000)
            virtu_log(f"Updated M3U storage path: {shared_folder}", virtu_logINFO)
        LOG_LEVEL = LOG_LEVEL_MAP.get(ADDON.getSetting('log_level'), 'info')
        LOG_VERBOSE = LOG_LEVEL == 'verbose'
        LOG_INFO = LOG_LEVEL == 'info' or LOG_VERBOSE

settings_monitor = SettingsMonitor()  # Instantiate SettingsMonitor to enable onSettingsChanged callback
        
def do_nothing():
    """No-op action for status item to prevent playback errors."""
    virtu_log("do_nothing: Status item selected, no action performed", virtu_logDEBUG)
    xbmcgui.Dialog().notification("VirtuaTV", "Status item selected (no action)", xbmcgui.NOTIFICATION_INFO, 1000)

# Replace your existing main_menu function
def main_menu():
    virtu_log("main_menu", virtu_logINFO)
    xbmcplugin.setContent(ADDON_HANDLE, 'files')
    try:
        # Check for valid channels
        channels = load_channels()
        storage_path = debug_settings()  # Get validated storage path
        if not channels:
            if xbmcgui.Dialog().yesno("VirtuaTV", "No channels found. Create channels or open settings?", yeslabel="Create", nolabel="Settings"):
                xbmc.executebuiltin('RunPlugin(plugin://plugin.video.virtuatv/?action=create_channel)')
            else:
                xbmc.executebuiltin('Addon.OpenSettings(plugin.video.virtuatv)')
            return
        # Check and update channels
        pre_load_channels()
        # Initialize valid channels
        valid_channels = []
        for ch in channels:
            channel = VirtuaTVChannel()
            channel.name = ch['name']
            channel.number = ch['number']
            channel.m3u_path = os.path.join(storage_path, f"VirtuaTV_Channel_{ch['number']}_{''.join(c for c in ch['name'] if c.isalnum() or c in (' ', '_', '-')).replace(' ', '_')}.m3u")
            if channel.set_playlist(channel.m3u_path):
                channel.is_valid = True
                valid_channels.append(channel)
        if not valid_channels:
            if xbmcgui.Dialog().yesno("VirtuaTV", "No valid channels found. Create channels or open settings?", yeslabel="Create", nolabel="Settings"):
                xbmc.executebuiltin('RunPlugin(plugin://plugin.video.virtuatv/?action=create_channel)')
            else:
                xbmc.executebuiltin('Addon.OpenSettings(plugin.video.virtuatv)')
            return
        # Load last channel
        last_channel = None
        if xbmcvfs.exists(LAST_CHANNEL_JSON):
            try:
                with xbmcvfs.File(LAST_CHANNEL_JSON, 'r') as f:
                    last_channel = json.load(f)
            except Exception as e:
                virtu_log(f"Error loading last_channel.json: {str(e)}", virtu_logERROR)
        # Play channel based on settings (last channel or first valid)
        valid_channels.sort(key=lambda x: x.number)
        if last_channel and any(ch.number == last_channel['number'] for ch in valid_channels):
            play_channel(last_channel['number'], valid_channels)
        else:
            play_channel(valid_channels[0].number, valid_channels)
        # Add menu items
        sync_status = "Synced" if shared_folder and xbmcvfs.exists(xbmcvfs.translatePath(shared_folder)) else "No Sync"
        list_item = xbmcgui.ListItem(label=f"[COLOR blue]Status: Sync {sync_status}[/COLOR]")
        list_item.setProperty('IsPlayable', 'false')
        list_item.addContextMenuItems([('Refresh Status', f'RunPlugin({ADDON_URL}?action=do_nothing)')])
        xbmcplugin.addDirectoryItem(
            ADDON_HANDLE,
            f"{ADDON_URL}?action=do_nothing",
            list_item,
            isFolder=False
        )
        list_item = xbmcgui.ListItem(label="Settings")
        list_item.addContextMenuItems([('Open Settings', f'RunPlugin({ADDON_URL}?action=open_settings)')])
        xbmcplugin.addDirectoryItem(ADDON_HANDLE, f"{ADDON_URL}?action=open_settings", list_item, isFolder=False)
        xbmcplugin.endOfDirectory(ADDON_HANDLE)
    except Exception as e:
        virtu_log(f"Error in main_menu: {str(e)}", virtu_logERROR)
        xbmcgui.Dialog().ok("VirtuaTV", "Error displaying menu.")
    
def get_used_channel_numbers():
    used_numbers = []
    if xbmcvfs.exists(CHANNELS_FILE):
        with xbmcvfs.File(CHANNELS_FILE, 'r') as f:
            try:
                channels = json.load(f)
                for channel in channels:
                    used_numbers.append(channel['number'])
            except:
                pass
    return used_numbers
    
def suggest_channel_number():
    used_numbers = get_used_channel_numbers()
    number = 1
    while number in used_numbers:
        number += 1
    return number
    
def load_channels():
    """Load channels from channels.json, handling errors robustly."""
    channels = []
    channels_file = xbmcvfs.translatePath("special://profile/addon_data/plugin.video.virtuatv/channels.json")
    if not xbmcvfs.exists(channels_file):
        virtu_log(f"VirtuaTV: channels.json does not exist at {channels_file}, starting with empty list", virtu_logINFO)
        return channels
    try:
        with xbmcvfs.File(channels_file, 'r') as f:
            content = f.read()
            virtu_log(f"VirtuaTV: Raw channels.json content: {content}", virtu_logDEBUG)
            if not content.strip():
                virtu_log(f"VirtuaTV: channels.json is empty at {channels_file}", virtu_logWARNING)
                return channels
            channels = json.loads(content)
            if not isinstance(channels, list):
                virtu_log(f"VirtuaTV: Invalid channels.json: not a list, content: {content}", virtu_logERROR)
                xbmcgui.Dialog().ok("Error", "Invalid channels.json format. Starting with empty channel list.")
                return []
            modified = False
            for channel in channels:
                if not isinstance(channel, dict) or 'playlists' not in channel:
                    virtu_log(f"VirtuaTV: Invalid channel data: {channel}", virtu_logERROR)
                    xbmcgui.Dialog().ok("Error", f"Invalid channel data in channels.json: {channel}")
                    return []
                if 'id' not in channel:
                    channel['id'] = f"{channel['name'].replace(' ', '_')}@VirtuaTV"
                    channel['is_new'] = True
                    modified = True
                if 'limit_type' not in channel:
                    channel['limit_type'] = 'time'
                for playlist in channel.get('playlists', []):
                    if 'random_order' not in playlist:
                        playlist['random_order'] = {}
                    if 'last_index' not in playlist:
                        playlist['last_index'] = {}
            if modified:
                save_channels(channels)
                virtu_log("VirtuaTV: Added missing channel IDs, flagged for regeneration, and saved channels.json", virtu_logINFO)
            virtu_log(f"VirtuaTV: Loaded {len(channels)} channels from {channels_file}: {json.dumps(channels, indent=2)}", virtu_logDEBUG)
    except json.JSONDecodeError as e:
        virtu_log(f"VirtuaTV: JSON error in channels.json: {str(e)}, content: {content}", virtu_logERROR)
        if xbmcvfs.exists(channels_file + '.bak'):
            if xbmcgui.Dialog().yesno("Error", "channels.json is corrupted. Restore from backup?"):
                xbmcvfs.copy(channels_file + '.bak', channels_file)
                virtu_log(f"VirtuaTV: Restored channels.json from backup", virtu_logINFO)
                return load_channels()
        xbmcgui.Dialog().ok("Error", f"Corrupted channels.json: {str(e)}. Starting with empty channel list.")
    except Exception as e:
        virtu_log(f"VirtuaTV: Error loading channels.json: {str(e)}", virtu_logERROR)
        xbmcgui.Dialog().ok("Error", f"Could not load channels: {str(e)}. Starting with empty channel list.")
    return channels
    
def save_channels(channels):
    """Save channels to channels.json with horizontal formatting for random_order and last_index lists."""
    def compact_random_order(json_str):
        lines = json_str.splitlines()
        output = []
        i = 0
        in_random_order = False
        in_list = False
        list_buffer = []
        key_line = None
        has_comma = False
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            if '"random_order": {' in stripped:
                in_random_order = True
            if in_random_order and '}' in stripped:
                in_random_order = False
            if in_list:
                if stripped.startswith(']'):
                    compact_list = '[' + ','.join(list_buffer) + ']'
                    if stripped.endswith(','):
                        compact_list += ','
                        has_comma = True
                    else:
                        has_comma = False
                    output.append(key_line.replace('[', compact_list))
                    in_list = False
                    list_buffer = []
                    key_line = None
                    i += 1
                    continue
                else:
                    num = stripped.rstrip(',')
                    list_buffer.append(num)
                i += 1
                continue
            else:
                if in_random_order and stripped.endswith('": ['):
                    in_list = True
                    key_line = line
                    i += 1
                    continue
                output.append(line)
            i += 1
        return '\n'.join(output)
    def compact_last_index(json_str):
        lines = json_str.splitlines()
        output = []
        i = 0
        in_last_index = False
        in_list = False
        list_buffer = []
        key_line = None
        has_comma = False
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            if '"last_index": {' in stripped:
                in_last_index = True
            if in_last_index and '}' in stripped:
                in_last_index = False
            if in_list:
                if stripped.startswith(']'):
                    compact_list = '[' + ','.join(list_buffer) + ']'
                    if stripped.endswith(','):
                        compact_list += ','
                        has_comma = True
                    else:
                        has_comma = False
                    output.append(key_line.replace('[', compact_list))
                    in_list = False
                    list_buffer = []
                    key_line = None
                    i += 1
                    continue
                else:
                    num = stripped.rstrip(',')
                    list_buffer.append(num)
                i += 1
                continue
            else:
                if in_last_index and stripped.endswith('": ['):
                    in_list = True
                    key_line = line
                    i += 1
                    continue
                output.append(line)
            i += 1
        return '\n'.join(output)
    try:
        virtu_log(f"VirtuaTV: Attempting to save {len(channels)} channels: {json.dumps(channels, indent=2)}", virtu_logDEBUG)
        if not isinstance(channels, list):
            virtu_log(f"VirtuaTV: Invalid channels data: not a list", virtu_logERROR)
            return False
        for channel in channels:
            if 'id' not in channel:
                channel['id'] = f"{channel['name'].replace(' ', '_')}@VirtuaTV"
                virtu_log(f"VirtuaTV: Added missing id {channel['id']} to channel {channel['name']}", virtu_logINFO)
            required_fields = ['name', 'number', 'id', 'playlists']
            for field in required_fields:
                if field not in channel or not isinstance(channel[field], (str, int, list) if field != 'playlists' else list):
                    virtu_log(f"VirtuaTV: Invalid channel data, missing or invalid {field}: {channel}", virtu_logERROR)
                    return False
            for playlist in channel.get('playlists', []):
                if not isinstance(playlist, dict) or 'path' not in playlist or 'type' not in playlist or 'playlist_type' not in playlist:
                    virtu_log(f"VirtuaTV: Invalid playlist data in channel {channel['name']}: {playlist}", virtu_logERROR)
                    return False
                random_order = playlist.get('random_order', {})
                if not isinstance(random_order, dict):
                    virtu_log(f"VirtuaTV: Invalid random_order in playlist {playlist['path']}: {random_order}", virtu_logERROR)
                    return False
                for key, value in random_order.items():
                    if not isinstance(value, list):
                        virtu_log(f"VirtuaTV: Invalid random_order value for {key} in playlist {playlist['path']}: {value}", virtu_logERROR)
                        return False
                last_index = playlist.get('last_index', {})
                if not isinstance(last_index, dict):
                    virtu_log(f"VirtuaTV: Invalid last_index in playlist {playlist['path']}: {last_index}", virtu_logERROR)
                    return False
                for key, value in last_index.items():
                    if not isinstance(value, int):
                        virtu_log(f"VirtuaTV: Invalid last_index value for {key} in playlist {playlist['path']}: {value}", virtu_logERROR)
                        return False
        channels_file = xbmcvfs.translatePath("special://profile/addon_data/plugin.video.virtuatv/channels.json")
        lock_file = channels_file + '.lock'
        if xbmcvfs.exists(lock_file):
            stat = xbmcvfs.Stat(lock_file)
            lock_mtime = stat.st_mtime()
            if time.time() - lock_mtime > 60: # If lock is older than 60 seconds, consider stale
                xbmcvfs.delete(lock_file)
                virtu_log("VirtuaTV: Removed stale lock file", virtu_logWARNING)
            else:
                virtu_log("VirtuaTV: channels.json is locked, retrying later", virtu_logWARNING)
                return False
        with xbmcvfs.File(lock_file, 'w') as f:
            f.write('lock')
        try:
            if xbmcvfs.exists(channels_file):
                backup_file = channels_file + '.bak'
                xbmcvfs.copy(channels_file, backup_file)
                virtu_log(f"VirtuaTV: Backed up channels.json to {backup_file}", virtu_logINFO)
            profile_dir = os.path.dirname(channels_file)
            if not xbmcvfs.exists(profile_dir):
                xbmcvfs.mkdirs(profile_dir)
                virtu_log(f"VirtuaTV: Created profile directory: {profile_dir}", virtu_logINFO)
            temp_file = os.path.join(xbmcvfs.translatePath("special://temp"), f"virtuatv_channels_{os.getpid()}.json")
            content = json.dumps(channels, indent=2)
            content = compact_random_order(content)
            content = compact_last_index(content)
            with xbmcvfs.File(temp_file, 'w') as f:
                f.write(content)
            if xbmcvfs.copy(temp_file, channels_file):
                xbmcvfs.delete(temp_file)
                virtu_log(f"VirtuaTV: Saved {len(channels)} channels to {channels_file}", virtu_logINFO)
                return True
            else:
                virtu_log(f"VirtuaTV: Failed to copy temp file to {channels_file}", virtu_logERROR)
                xbmcvfs.delete(temp_file)
                return False
        finally:
            xbmcvfs.delete(lock_file)
    except Exception as e:
        virtu_log(f"VirtuaTV: Failed to save channels: {str(e)}", virtu_logERROR)
        xbmcgui.Dialog().ok("Error", f"Failed to save channels: {str(e)}")
        return False


# Replace your existing generate_channel_files function with this version
def generate_channel_files(channel_number):
    """Generate M3U for the specified channel with optimized non-blocking notifications."""
    def get_episode_count(filename):
        match = re.search(r'S(\d{2})E(\d{2})-?E?(\d{2})?', filename, re.IGNORECASE)
        if match and match.group(3):
            start_ep = int(match.group(2))
            end_ep = int(match.group(3))
            return end_ep - start_ep + 1
        return 1
    dialog = xbmcgui.Dialog()
    channels = load_channels()
    virtu_log(f"VirtuaTV: Loaded {len(channels)} channels for file generation", virtu_logDEBUG)
    if not channels:
        virtu_log(f"VirtuaTV: No channels to process for file generation", virtu_logERROR)
        dialog.notification("VirtuaTV", "No channels to process for file generation", xbmcgui.NOTIFICATION_ERROR, 3000)
        time.sleep(0.1)
        return False
    channel = next((ch for ch in channels if ch['number'] == channel_number), None)
    if not channel:
        virtu_log(f"VirtuaTV: No channel found with number {channel_number}", virtu_logERROR)
        dialog.notification("VirtuaTV", f"No channel found with number {channel_number}", xbmcgui.NOTIFICATION_ERROR, 3000)
        time.sleep(0.1)
        return False
    try:
        storage_path = debug_settings()  # Get validated storage path
        if not xbmcvfs.exists(storage_path):
            if xbmcvfs.mkdirs(storage_path):
                virtu_log(f"VirtuaTV: Created storage path: {storage_path}", virtu_logINFO)
            else:
                dialog.notification("VirtuaTV", f"Could not create storage path: {storage_path}", xbmcgui.NOTIFICATION_ERROR, 3000)
                virtu_log(f"Could not create storage path: {storage_path}", virtu_logERROR)
                time.sleep(0.1)
                return False
        max_items = int(ADDON.getSetting('max_playlist_items') or 1000)
        max_duration = int(ADDON.getSetting('max_playlist_duration') or 24) * 3600
        virtu_log(f"VirtuaTV: Using storage_path={storage_path}, max_playlist_items={max_items}, max_playlist_duration={max_duration}", virtu_logINFO)
        channel_name = channel['name']
        playlists = channel.get('playlists', [])
        limit_type = channel.get('limit_type', 'time')
        is_new_channel = channel.get('is_new', False)
        virtu_log(f"VirtuaTV: Processing channel {channel_name} ({channel_number}), is_new={is_new_channel}, {len(playlists)} playlists, limit_type={limit_type}", virtu_logINFO)
        dialog.notification("VirtuaTV", f"Processing channel {channel_name} ({channel_number})", xbmcgui.NOTIFICATION_INFO, 1500)
        time.sleep(0.1)
        for playlist in playlists:
            if 'last_index' not in playlist or not isinstance(playlist['last_index'], dict):
                playlist['last_index'] = {}
            if 'random_order' not in playlist or not isinstance(playlist['random_order'], dict):
                playlist['random_order'] = {}
            virtu_log(f"VirtuaTV: Initialized last_index and random_order for playlist {playlist['path']} in channel {channel_name}", virtu_logDEBUG)
            if 'playlist_type' not in playlist:
                virtu_log(f"VirtuaTV: No playlist_type specified for {playlist['path']} in channel {channel_name}", virtu_logERROR)
                dialog.notification("VirtuaTV", f"No playlist_type specified for playlist {playlist['path']}", xbmcgui.NOTIFICATION_ERROR, 3000)
                time.sleep(0.1)
                return False
            if playlist.get('type') == 'base' and 'interleave' in playlist:
                virtu_log(f"VirtuaTV: Warning: Interleave settings ignored for base playlist {playlist['path']} in channel {channel_name}", virtu_logWARNING)
                playlist.pop('interleave', None)
        virtu_log(f"VirtuaTV: Saving full channels list before file generation: {json.dumps(channels, indent=2)}", virtu_logDEBUG)
        save_channels(channels)
        virtu_log(f"VirtuaTV: Saved channels.json with initial last_index and random_order for channel {channel_name}", virtu_logINFO)
        safe_channel_name = ''.join(c for c in channel_name if c.isalnum() or c in (' ', '_', '-')).replace(' ', '_')
        m3u_filename = os.path.join(storage_path, f'VirtuaTV_Channel_{channel_number}_{safe_channel_name}.m3u')
        virtu_log(f"VirtuaTV: Generating M3U file at {m3u_filename}", virtu_logINFO)
        all_playlists = []
        for playlist_idx, playlist in enumerate(playlists):
            playlist_type = playlist.get('playlist_type')
            source = playlist.get('source', 'playlist')
            virtu_log(f"VirtuaTV: Processing Playlist {playlist_idx} as {playlist_type} (source: {source})", virtu_logDEBUG)
            items, rule_order, is_random = get_playlist_items_with_durations(playlist['path'], playlist_type, source=source)
            if not items:
                virtu_log(f"VirtuaTV: No items found for Playlist {playlist['path']} in channel {channel_name}", virtu_logERROR)
                dialog.notification("VirtuaTV", f"No items found for playlist {playlist['path']} in channel {channel_name}", xbmcgui.NOTIFICATION_ERROR, 3000)
                time.sleep(0.1)
                continue
            is_one_match = False
            if source == 'playlist':
                try:
                    with xbmcvfs.File(playlist['path'], 'r') as f:
                        xml_content = f.read()
                    tree = ET.fromstring(xml_content)
                    match_elem = tree.find('match')
                    is_one_match = match_elem.text == 'one' if match_elem is not None else False
                except Exception as e:
                    virtu_log(f"VirtuaTV: Error parsing playlist for match type: {str(e)}", virtu_logERROR)
            item_groups = {}
            conn, cursor, db_type, _ = get_database_connection()
            if conn is None or cursor is None:
                continue
            try:
                for item in items:
                    if not isinstance(item, dict) or 'file' not in item:
                        virtu_log(f"VirtuaTV: Invalid item in Playlist {playlist_idx}: {item}", virtu_logWARNING)
                        continue
                    try:
                        filename = os.path.basename(item['file'])
                        path = os.path.dirname(item['file']) + '/'
                        item_name = item.get('showtitle', item.get('title', os.path.basename(item['file'])))
                        item_description = item.get('plot', '')
                        item_categories = item.get('genre', [])
                        item_date = item.get('year', '')
                        item_icon = item.get('thumbnail', '')
                        if source == 'playlist':
                            if db_type == 'mysql':
                                query = (
                                    """
                                    SELECT tvshow.c00, tvshow.c01, tvshow.c08, tvshow.c05
                                    FROM episode
                                    JOIN tvshow ON episode.idShow = tvshow.idShow
                                    JOIN files ON episode.idFile = files.idFile
                                    JOIN path ON files.idPath = path.idPath
                                    WHERE files.strFilename = %s AND path.strPath = %s
                                    """ if playlist_type == 'episodes' else """
                                    SELECT movie.c00, movie.c01, movie.c06, movie.c07
                                    FROM movie
                                    JOIN files ON movie.idFile = files.idFile
                                    JOIN path ON files.idPath = path.idPath
                                    WHERE files.strFilename = %s AND path.strPath = %s
                                    """
                                )
                                params = (filename, path)
                            else:
                                query = (
                                    """
                                    SELECT tvshow.c00, tvshow.c01, tvshow.c08, tvshow.c05
                                    FROM episode
                                    JOIN tvshow ON episode.idShow = tvshow.idShow
                                    JOIN files ON episode.idFile = files.idFile
                                    JOIN path ON files.idPath = path.idPath
                                    WHERE files.strFilename = ? AND path.strPath = ?
                                    """ if playlist_type == 'episodes' else """
                                    SELECT movie.c00, movie.c01, movie.c06, movie.c07
                                    FROM movie
                                    JOIN files ON movie.idFile = files.idFile
                                    JOIN path ON files.idPath = path.idPath
                                    WHERE files.strFilename = ? AND path.strPath = ?
                                    """
                                )
                                params = (filename, path)
                            cursor.execute(query, params)
                            results = cursor.fetchall()
                            if results:
                                item_name, item_description, item_categories, item_date = results[0]
                                item_categories = item_categories.split(',') if item_categories else ['Unknown']
                            else:
                                virtu_log(f"VirtuaTV: No {'TV show' if playlist_type == 'episodes' else 'movie'} information found for {filename} in Playlist {playlist_idx}. Using title or filename: {item_name}", virtu_logWARNING)
                        else:
                            item_name = os.path.basename(playlist['path']) if playlist_type == 'episodes' else item['title']
                            item_categories = ['Unknown'] if not item_categories else item_categories
                        if item_name not in item_groups:
                            item_groups[item_name] = {'items': [], 'description': item_description, 'categories': item_categories, 'date': item_date, 'icon': item_icon}
                        if item_name not in playlist['last_index']:
                            playlist['last_index'][item_name] = -1
                        item_groups[item_name]['items'].append(item)
                        virtu_log(f"VirtuaTV: Added item {filename} to Playlist {playlist_idx} item {item_name}", virtu_logDEBUG)
                    except Exception as e:
                        virtu_log(f"VirtuaTV: Error processing item {item} in Playlist {playlist_idx}: {str(e)}", virtu_logERROR)
                        continue
                cursor.close()
                conn.close()
            except Exception as e:
                virtu_log(f"VirtuaTV: Error grouping Playlist {playlist_idx} items: {str(e)}", virtu_logERROR)
                dialog.notification("VirtuaTV", f"Failed to group items for playlist {playlist['path']}: {str(e)}", xbmcgui.NOTIFICATION_ERROR, 3000)
                time.sleep(0.1)
                continue
            if not item_groups:
                virtu_log(f"VirtuaTV: No valid {'episodes' if playlist_type == 'episodes' else 'movies'} found for Playlist {playlist['path']} in channel {channel_name}", virtu_logERROR)
                dialog.notification("VirtuaTV", f"No valid items found for playlist {playlist['path']} in channel {channel_name}", xbmcgui.NOTIFICATION_ERROR, 3000)
                time.sleep(0.1)
                continue
            sorted_item_groups = {}
            ordered_shows = rule_order if rule_order else list(item_groups.keys())
            if source == 'playlist' and is_random and playlist_type == 'episodes' and is_one_match:
                random.shuffle(ordered_shows)
                virtu_log(f"VirtuaTV: Randomized show order for Playlist {playlist_idx} (<match>one</match>): {ordered_shows}", virtu_logDEBUG)
            elif playlist.get('fixed_show_order'):
                ordered_shows = [show for show in playlist['fixed_show_order'] if show in item_groups]
                virtu_log(f"VirtuaTV: Using fixed show order for Playlist {playlist_idx}: {ordered_shows}", virtu_logINFO)
            elif playlist.get('shuffle_shows', False):
                random.shuffle(ordered_shows)
                virtu_log(f"VirtuaTV: Shuffled show order for Playlist {playlist_idx}: {ordered_shows}", virtu_logDEBUG)
            for item_name in ordered_shows:
                if item_name in item_groups:
                    sorted_item_groups[item_name] = item_groups[item_name]
                    if playlist_type == 'episodes' and source == 'playlist' and is_random:
                        if item_name not in playlist['random_order'] or not playlist['random_order'][item_name]:
                            playlist['random_order'][item_name] = list(range(len(item_groups[item_name]['items'])))
                            random.shuffle(playlist['random_order'][item_name])
                            virtu_log(f"VirtuaTV: Generated random order for {item_name} in Playlist {playlist_idx}: {playlist['random_order'][item_name]}", virtu_logINFO)
                        sorted_item_groups[item_name]['items'] = [
                            sorted_item_groups[item_name]['items'][i] for i in playlist['random_order'][item_name]
                        ]
                    elif playlist_type == 'episodes' and source == 'playlist':
                        sorted_item_groups[item_name]['items'].sort(key=lambda x: (int(x.get('season', 1)), int(x.get('episode', 1))))
                    virtu_log(f"VirtuaTV: Playlist {playlist_idx} item {item_name} has {len(sorted_item_groups[item_name]['items'])} {'episodes' if playlist_type == 'episodes' else 'movies'}", virtu_logINFO)
            if playlist['random_order']:
                virtu_log(f"VirtuaTV: Saving full channels list after random_order update: {json.dumps(channels, indent=2)}", virtu_logDEBUG)
                save_channels(channels)
                virtu_log(f"VirtuaTV: Saved channels.json with updated random_order for Playlist {playlist_idx} in channel {channel_name}", virtu_logINFO)
            all_playlists.append({
                'items': sorted_item_groups,
                'interleave': playlist.get('interleave', {'low': 1, 'high': 1, 'count': 1}),
                'last_index': playlist['last_index'],
                'random_order': playlist['random_order'],
                'type': playlist_type,
                'path': playlist['path'],
                'is_random': is_random,
                'shuffle_shows': playlist.get('shuffle_shows', False)
            })
        if not all_playlists:
            virtu_log(f"VirtuaTV: No valid playlists for channel {channel_name}", virtu_logWARNING)
            dialog.notification("VirtuaTV", f"No valid playlists found for channel {channel_name}", xbmcgui.NOTIFICATION_ERROR, 3000)
            time.sleep(0.1)
            return False
        indices = {
            playlist_idx: {item_name: playlist['last_index'].get(item_name, -1) + 1 for item_name in playlist['items']}
            for playlist_idx, playlist in enumerate(all_playlists)
        }
        item_counts = {
            playlist_idx: {item_name: len(item_data['items']) for item_name, item_data in playlist['items'].items()}
            for playlist_idx, playlist in enumerate(all_playlists)
        }
        interleaved_track = []
        total_items = 0
        total_duration = 0
        base_playlist_idx = 0
        base_items = list(all_playlists[base_playlist_idx]['items'].keys()) if all_playlists else []
        cycle_length = len(base_items) if base_items else 1
        add_item_counters = {add_playlist_idx: 0 for add_playlist_idx in range(1, len(all_playlists))}
        try:
            cycle_count = 0
            while (limit_type == 'time' and total_duration < max_duration) or (limit_type == 'items' and total_items < max_items):
                cycle_count += 1
                base_items = list(all_playlists[base_playlist_idx]['items'].keys())
                if all_playlists[base_playlist_idx].get('shuffle_shows', False):
                    random.shuffle(base_items)
                    virtu_log(f"VirtuaTV: Shuffled base items for cycle: {base_items}", virtu_logDEBUG)
                base_cycle = []
                for item_name in base_items:
                    if (limit_type == 'time' and total_duration >= max_duration) or (limit_type == 'items' and total_items >= max_items):
                        break
                    item_data = all_playlists[base_playlist_idx]['items'][item_name]
                    items = item_data['items']
                    num_items = item_counts[base_playlist_idx][item_name]
                    item_idx = indices[base_playlist_idx][item_name] % num_items
                    item = items[item_idx]
                    base_cycle.append((base_playlist_idx, item_name, item, item_idx))
                    episode_count = get_episode_count(os.path.basename(item['file'])) if all_playlists[base_playlist_idx]['type'] == 'episodes' else 1
                    indices[base_playlist_idx][item_name] = (indices[base_playlist_idx][item_name] + episode_count) % num_items
                    virtu_log(f"VirtuaTV: Prepared base item {item_name} index {item_idx + 1}/{num_items} for cycle", virtu_logDEBUG)
                if len(all_playlists) == 1:
                    for _, item_name, item, item_idx in base_cycle:
                        interleaved_track.append((item_name, item))
                        episode_count = get_episode_count(os.path.basename(item['file'])) if all_playlists[base_playlist_idx]['type'] == 'episodes' else 1
                        all_playlists[base_playlist_idx]['last_index'][item_name] = (item_idx + episode_count - 1) % item_counts[base_playlist_idx][item_name]
                        virtu_log(
                            f"VirtuaTV: Added base item {item_name} index {item_idx + 1}/{item_counts[base_playlist_idx][item_name]}: {item['title']} (episodes: {episode_count}) (single playlist), updated last_index to {(item_idx + episode_count - 1) % item_counts[base_playlist_idx][item_name]}",
                            virtu_logDEBUG
                        )
                        total_items += 1
                        total_duration += item['duration']
                    continue
                new_cycle_length = cycle_length
                for add_playlist_idx, add_playlist in enumerate(all_playlists[1:], 1):
                    count = max(1, add_playlist['interleave'].get('count', 1))
                    new_cycle_length += count
                    virtu_log(f"VirtuaTV: New cycle length for channel {channel_name}: {new_cycle_length} (base: {cycle_length}, additional: {new_cycle_length - cycle_length})", virtu_logDEBUG)
                insert_positions = {}
                fixed_playlists = [(idx, p) for idx, p in enumerate(all_playlists[1:], 1) if p['interleave'].get('low') == p['interleave'].get('high')]
                for add_playlist_idx, add_playlist in fixed_playlists:
                    low = add_playlist['interleave'].get('low', 1)
                    count = max(1, add_playlist['interleave'].get('count', 1))
                    if low < 1:
                        virtu_log(f"VirtuaTV: Invalid low value {low} for Playlist {add_playlist_idx} ({add_playlist['path']}) in channel {channel_name}. Must be >= 1. Defaulting to 1.", virtu_logWARNING)
                        dialog.notification("VirtuaTV", f"Invalid low value {low} for playlist {add_playlist['path']}", xbmcgui.NOTIFICATION_WARNING, 3000)
                        time.sleep(0.1)
                        low = 1
                    if low > new_cycle_length:
                        virtu_log(f"VirtuaTV: Invalid low value {low} for Playlist {add_playlist_idx} ({add_playlist['path']}) in channel {channel_name}. Must be <= {new_cycle_length}. Defaulting to {new_cycle_length}.", virtu_logWARNING)
                        dialog.notification("VirtuaTV", f"Invalid low value {low} for playlist {add_playlist['path']}", xbmcgui.NOTIFICATION_WARNING, 3000)
                        time.sleep(0.1)
                        low = new_cycle_length
                    pos = low
                    insert_positions[add_playlist_idx] = pos
                    virtu_log(f"VirtuaTV: Assigned fixed position {pos} with count {count} to Playlist {add_playlist_idx} ({add_playlist['path']})", virtu_logDEBUG)
                random_playlists = [(idx, p) for idx, p in enumerate(all_playlists[1:], 1) if p['interleave'].get('low') != p['interleave'].get('high')]
                for add_playlist_idx, add_playlist in random_playlists:
                    low = add_playlist['interleave'].get('low', 1)
                    high = add_playlist['interleave'].get('high', 1)
                    count = max(1, add_playlist['interleave'].get('count', 1))
                    if low < 1:
                        virtu_log(f"VirtuaTV: Invalid low value {low} for Playlist {add_playlist_idx} ({add_playlist['path']}) in channel {channel_name}. Must be >= 1. Defaulting to 1.", virtu_logWARNING)
                        dialog.notification("VirtuaTV", f"Invalid low value {low} for playlist {add_playlist['path']}", xbmcgui.NOTIFICATION_WARNING, 3000)
                        time.sleep(0.1)
                        low = 1
                    if high < low:
                        virtu_log(f"VirtuaTV: Invalid range: low ({low}) > high ({high}) for Playlist {add_playlist_idx} ({add_playlist['path']}) in channel {channel_name}. Swapping values.", virtu_logWARNING)
                        dialog.notification("VirtuaTV", f"Invalid interleave range for playlist {add_playlist['path']}", xbmcgui.NOTIFICATION_WARNING, 3000)
                        time.sleep(0.1)
                        low, high = high, low
                    if high > new_cycle_length:
                        virtu_log(f"VirtuaTV: Invalid high value {high} for Playlist {add_playlist_idx} ({add_playlist['path']}) in channel {channel_name}. Must be <= {new_cycle_length}. Defaulting to {new_cycle_length}.", virtu_logWARNING)
                        dialog.notification("VirtuaTV", f"Invalid high value {high} for playlist {add_playlist['path']}", xbmcgui.NOTIFICATION_WARNING, 3000)
                        time.sleep(0.1)
                        high = new_cycle_length
                    valid_positions = [p for p in range(1, new_cycle_length + 1) if low <= p <= min(high, new_cycle_length - count + 1)]
                    pos = random.choice(valid_positions) if valid_positions else 1
                    insert_positions[add_playlist_idx] = pos
                    virtu_log(f"VirtuaTV: Assigned random position {pos} with count {count} to Playlist {add_playlist_idx} ({add_playlist['path']})", virtu_logDEBUG)
                shuffled_available = {}
                for add_playlist_idx in range(1, len(all_playlists)):
                    add_playlist = all_playlists[add_playlist_idx]
                    avail = list(add_playlist['items'].keys())
                    if add_playlist.get('shuffle_shows', False):
                        random.shuffle(avail)
                        virtu_log(f"VirtuaTV: Shuffled additional items for cycle in Playlist {add_playlist_idx}: {avail}", virtu_logDEBUG)
                    shuffled_available[add_playlist_idx] = avail
                current_cycle = []
                current_position = 0
                base_idx = 0
                while current_position < new_cycle_length and ((limit_type == 'time' and total_duration < max_duration) or (limit_type == 'items' and total_items < max_items)):
                    current_position += 1
                    inserted = False
                    for add_playlist_idx in sorted(insert_positions.keys()):
                        add_playlist = all_playlists[add_playlist_idx]
                        count = max(1, add_playlist['interleave'].get('count', 1))
                        if insert_positions[add_playlist_idx] <= current_position < insert_positions[add_playlist_idx] + count:
                            available_items = shuffled_available[add_playlist_idx]
                            if available_items:
                                item_idx = add_item_counters[add_playlist_idx] % len(available_items)
                                item_name = available_items[item_idx]
                                if item_name in item_counts[add_playlist_idx]:
                                    item_data = add_playlist['items'][item_name]
                                    items = item_data['items']
                                    num_items = item_counts[add_playlist_idx][item_name]
                                    item_idx_episode = indices[add_playlist_idx][item_name] % num_items
                                    item = items[item_idx_episode]
                                    current_cycle.append((item_name, item))
                                    episode_count = get_episode_count(os.path.basename(item['file'])) if add_playlist['type'] == 'episodes' else 1
                                    add_playlist['last_index'][item_name] = (item_idx_episode + episode_count - 1) % num_items
                                    virtu_log(
                                        f"VirtuaTV: Adding Playlist {add_playlist_idx} {item_name} item {item_idx_episode + 1}/{num_items}: {item['title']} (episodes: {episode_count}) at position {current_position}, updated last_index to {(item_idx_episode + episode_count - 1) % num_items}",
                                        virtu_logDEBUG
                                    )
                                    total_items += 1
                                    total_duration += item['duration']
                                    indices[add_playlist_idx][item_name] = (indices[add_playlist_idx][item_name] + episode_count) % num_items
                                    inserted = True
                                    add_item_counters[add_playlist_idx] += 1
                                else:
                                    virtu_log(f"VirtuaTV: Skipping additional item {item_name} due to missing item_count in Playlist {add_playlist_idx}", virtu_logWARNING)
                            else:
                                virtu_log(f"VirtuaTV: No available items for Playlist {add_playlist_idx} ({add_playlist['path']})", virtu_logWARNING)
                    if not inserted and base_idx < len(base_cycle) and ((limit_type == 'time' and total_duration < max_duration) or (limit_type == 'items' and total_items < max_items)):
                        playlist_idx, item_name, item, item_idx = base_cycle[base_idx]
                        if item_name in item_counts[playlist_idx]:
                            current_cycle.append((item_name, item))
                            episode_count = get_episode_count(os.path.basename(item['file'])) if all_playlists[playlist_idx]['type'] == 'episodes' else 1
                            all_playlists[playlist_idx]['last_index'][item_name] = (item_idx + episode_count - 1) % item_counts[playlist_idx][item_name]
                            virtu_log(
                                f"VirtuaTV: Adding Playlist {playlist_idx} {item_name} item {item_idx + 1}/{item_counts[playlist_idx][item_name]}: {item['title']} (episodes: {episode_count}) at position {current_position}, updated last_index to {(item_idx + episode_count - 1) % item_counts[playlist_idx][item_name]}",
                                virtu_logDEBUG
                            )
                            total_items += 1
                            total_duration += item['duration']
                        else:
                            virtu_log(f"VirtuaTV: Skipping base item {item_name} due to missing item_count in Playlist {playlist_idx}", virtu_logWARNING)
                        base_idx += 1
                interleaved_track.extend(current_cycle)
                virtu_log(f"VirtuaTV: Completed cycle {cycle_count} with {len(current_cycle)} items, total_items={total_items}, total_duration={total_duration}", virtu_logDEBUG)
        except Exception as e:
            virtu_log(f"VirtuaTV: Error building schedule for channel {channel_name}: {str(e)}", virtu_logERROR)
            dialog.notification("VirtuaTV", f"Error building schedule for channel {channel_name}: {str(e)}", xbmcgui.NOTIFICATION_ERROR, 3000)
            time.sleep(0.1)
            return False
        try:
            for playlist_idx, playlist in enumerate(all_playlists):
                channel['playlists'][playlist_idx]['last_index'] = {
                    item_name: all_playlists[playlist_idx]['last_index'][item_name]
                    for item_name in playlist['items'] if item_name in item_counts[playlist_idx]
                }
                channel['playlists'][playlist_idx]['random_order'] = {
                    item_name: all_playlists[playlist_idx]['random_order'][item_name]
                    for item_name in playlist['items'] if item_name in all_playlists[playlist_idx]['random_order']
                    and isinstance(all_playlists[playlist_idx]['random_order'][item_name], list)
                }
                virtu_log(f"VirtuaTV: Updated last_index and random_order for Playlist {playlist_idx}: {channel['playlists'][playlist_idx]['last_index']}, random_order: {channel['playlists'][playlist_idx]['random_order']}", virtu_logDEBUG)
            virtu_log(f"VirtuaTV: Saving full channels list after updating last_index and random_order: {json.dumps(channels, indent=2)}", virtu_logDEBUG)
            save_channels(channels)
            virtu_log(f"VirtuaTV: Saved channels.json with updated last_index and random_order for channel {channel_name}", virtu_logINFO)
        except Exception as e:
            virtu_log(f"VirtuaTV: Error saving channels.json for channel {channel_name}: {str(e)}", virtu_logERROR)
            dialog.notification("VirtuaTV", f"Error saving channels for channel {channel_name}: {str(e)}", xbmcgui.NOTIFICATION_ERROR, 3000)
            time.sleep(0.1)
            return False
        try:
            dialog.notification("VirtuaTV", f"Writing M3U file for channel {channel_name}...", xbmcgui.NOTIFICATION_INFO, 1500)
            time.sleep(0.1)
            with xbmcvfs.File(m3u_filename, 'w') as f:
                f.write('#EXTM3U\n')
                for item_name, item in interleaved_track:
                    if not isinstance(item, dict) or 'title' not in item or 'file' not in item:
                        virtu_log(f"VirtuaTV: Invalid item in interleaved track for channel {channel_name}: {item}", virtu_logWARNING)
                        continue
                    file_path = item['file']
                    if not xbmcvfs.exists(file_path):
                        virtu_log(f"VirtuaTV: Inaccessible file path: {file_path}", virtu_logWARNING)
                        dialog.notification("VirtuaTV", f"Inaccessible file path: {file_path}", xbmcgui.NOTIFICATION_WARNING, 3000)
                        time.sleep(0.1)
                        continue
                    duration = item.get('duration', 0)
                    if duration <= 0:
                        virtu_log(f"VirtuaTV: Skipping item {item['title']} with invalid duration {duration}", virtu_logWARNING)
                        continue
                    title = item['title']
                    description = all_playlists[0]['items'].get(item_name, {}).get('description', '')
                    season_episode = f"S{item.get('season', 0):02d}E{item.get('episode', 0):02d}" if all_playlists[0]['type'] == 'episodes' and 'season' in item and 'episode' in item else ''
                    entry_title = f"{item_name}//{title} ({season_episode})//{description}" if season_episode else f"{item_name}//{title}//{description}"
                    f.write(f'#EXTINF:{int(duration)},{entry_title}\n')
                    f.write(f'{file_path}\n')
            virtu_log(f"VirtuaTV: Added {len(interleaved_track)} entries for channel {channel_name} in {m3u_filename}", virtu_logINFO)
        except Exception as e:
            virtu_log(f"VirtuaTV: Error writing M3U file for channel {channel_name}: {str(e)}", virtu_logERROR)
            dialog.notification("VirtuaTV", f"Error writing M3U file for channel {channel_name}: {str(e)}", xbmcgui.NOTIFICATION_ERROR, 3000)
            time.sleep(0.1)
            return False
        try:
            if is_new_channel:
                channel['is_new'] = False
            channel['last_gen_time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            channel['total_gen_duration'] = total_duration
            virtu_log(f"VirtuaTV: Saving full channels list after updating is_new flag and generation info: {json.dumps(channels, indent=2)}", virtu_logDEBUG)
            save_channels(channels)
            dialog.notification("VirtuaTV", f"Successfully generated files for channel {channel_name}", xbmcgui.NOTIFICATION_INFO, 1500)
            virtu_log(f"VirtuaTV: Generated files for channel {channel_number}", virtu_logINFO)
            time.sleep(0.1)
            return True
        except Exception as e:
            virtu_log(f"VirtuaTV: Error updating is_new flag and generation info for channel {channel_name}: {str(e)}", virtu_logERROR)
            dialog.notification("VirtuaTV", f"Error updating channel info for {channel_name}: {str(e)}", xbmcgui.NOTIFICATION_ERROR, 3000)
            time.sleep(0.1)
            return False
    except Exception as e:
        virtu_log(f"VirtuaTV: Error generating files: {str(e)}\n{traceback.format_exc()}", virtu_logERROR)
        dialog.notification("VirtuaTV", f"Error generating files: {str(e)}", xbmcgui.NOTIFICATION_ERROR, 3000)
        time.sleep(0.1)
        return False

def update_channel_files(channel_number):
    """Update M3U by removing expired items and appending new ones."""
    def get_episode_count(filename):
        match = re.search(r'S(\d{2})E(\d{2})-?E?(\d{2})?', filename, re.IGNORECASE)
        if match and match.group(3):
            start_ep = int(match.group(2))
            end_ep = int(match.group(3))
            return end_ep - start_ep + 1
        return 1
    channels = load_channels()
    virtu_log(f"Loaded {len(channels)} channels for file update", virtu_logDEBUG)
    if not channels:
        virtu_log("No channels to process for file update", virtu_logERROR)
        return False
    channel = next((ch for ch in channels if ch['number'] == channel_number), None)
    if not channel:
        virtu_log(f"No channel found with number {channel_number}", virtu_logERROR)
        return False
    try:
        storage_path = debug_settings()  # Get validated storage path
        if not xbmcvfs.exists(storage_path):
            if xbmcvfs.mkdirs(storage_path):
                virtu_log(f"Created storage path: {storage_path}", virtu_logINFO)
            else:
                xbmcgui.Dialog().ok("Error", f"Could not create storage path: {storage_path}. Please check the path.")
                virtu_log(f"Failed to create storage path: {storage_path}", virtu_logERROR)
                return False
        max_items = int(ADDON.getSetting('max_playlist_items') or 1000)
        max_duration = int(ADDON.getSetting('max_playlist_duration') or 24) * 3600
        virtu_log(f"Using storage_path={storage_path}, max_playlist_items={max_items}, max_playlist_duration={max_duration}", virtu_logINFO)
        progress_dialog = xbmcgui.DialogProgress()
        progress_dialog.create("VirtuaTV", f"Updating files for channel {channel_number}...")
        try:
            channel_name = channel['name']
            channel_id = channel.get('id', f"{channel_name}@VirtuaTV")
            if 'id' not in channel:
                channel['id'] = channel_id
                save_channels(channels)
                virtu_log(f"VirtuaTV: Added missing channel_id {channel_id} to channel {channel_name}", virtu_logINFO)
            safe_channel_name = ''.join(c for c in channel_name if c.isalnum() or c in (' ', '_', '-')).replace(' ', '_')
            m3u_filename = os.path.join(storage_path, f'VirtuaTV_Channel_{channel_number}_{safe_channel_name}.m3u')
            virtu_log(f"Updating M3U file at {m3u_filename}", virtu_logINFO)
            if channel.get('is_new', False):
                virtu_log(f"Channel {channel_name} flagged for full regeneration", virtu_logINFO)
                progress_dialog.close()
                return generate_channel_files(channel_number)
            if not xbmcvfs.exists(m3u_filename):
                virtu_log(f"Files do not exist, falling back to full generation for channel {channel_number}", virtu_logINFO)
                progress_dialog.close()
                return generate_channel_files(channel_number)
            with xbmcvfs.File(m3u_filename, 'r') as f:
                m3u_content = f.read()
            if not m3u_content.strip():
                virtu_log(f"Empty M3U, falling back to full generation", virtu_logINFO)
                progress_dialog.close()
                return generate_channel_files(channel_number)
            m3u_lines = m3u_content.splitlines()
            durations = []
            for line in m3u_lines:
                if line.startswith('#EXTINF:'):
                    match = re.search(r'#EXTINF:(\d+)', line)
                    if match:
                        durations.append(int(match.group(1)))
            if not durations:
                virtu_log(f"No durations found in M3U, falling back to full generation", virtu_logERROR)
                progress_dialog.close()
                return generate_channel_files(channel_number)
            last_gen_time = datetime.datetime.fromisoformat(channel.get('last_gen_time', datetime.datetime.now(datetime.timezone.utc).isoformat()))
            now = datetime.datetime.now(datetime.timezone.utc)
            time_passed = (now - last_gen_time).total_seconds()
            cum = 0
            removed_k = 0
            for removed_k in range(len(durations)):
                cum += max(durations[removed_k], 0)
                if cum > time_passed:
                    break
            else:
                removed_k = len(durations)
            if removed_k < len(durations):
                removed_duration = cum - max(durations[removed_k], 0)
            else:
                removed_duration = cum
            remaining_durations = durations[removed_k:]
            remaining_duration = sum(max(d, 0) for d in remaining_durations)
            limit_type = channel.get('limit_type', 'time')
            if limit_type == 'time':
                to_add_duration = max_duration - remaining_duration
                to_add_items = 0
            else:
                to_add_items = max_items - len(remaining_durations)
                to_add_duration = 0
            if (limit_type == 'time' and to_add_duration <= 0) or (limit_type == 'items' and to_add_items <= 0):
                virtu_log(f"No need to add items for channel {channel_name}", virtu_logINFO)
                temp_m3u = os.path.join(xbmcvfs.translatePath("special://temp"), f"virtuatv_m3u_{os.getpid()}.m3u")
                with xbmcvfs.File(temp_m3u, 'w') as f:
                    f.write('\n'.join(m3u_lines[:1] + m3u_lines[removed_k*2+1:]) + '\n')
                if not xbmcvfs.copy(temp_m3u, m3u_filename):
                    virtu_log(f"Failed to update M3U file for channel {channel_name}", virtu_logERROR)
                    xbmcvfs.delete(temp_m3u)
                    progress_dialog.close()
                    return False
                xbmcvfs.delete(temp_m3u)
                channel['last_gen_time'] = now.isoformat()
                channel['total_gen_duration'] = remaining_duration
                channel['is_new'] = False
                save_channels(channels)
                virtu_log(f"Updated files for channel {channel_number}: removed {removed_k} items, no new items added, total_duration={channel['total_gen_duration']}", virtu_logINFO)
                progress_dialog.close()
                return True
            virtu_log(f"Adding {'duration: ' + str(to_add_duration) if limit_type == 'time' else 'items: ' + str(to_add_items)} for channel {channel_name}", virtu_logINFO)
            playlists = channel.get('playlists', [])
            for playlist in playlists:
                if 'last_index' not in playlist or not isinstance(playlist['last_index'], dict):
                    playlist['last_index'] = {}
                if 'random_order' not in playlist or not isinstance(playlist['random_order'], dict):
                    playlist['random_order'] = {}
                if playlist.get('type') == 'base' and 'interleave' in playlist:
                    virtu_log(f"Warning: Interleave settings ignored for base playlist {playlist['path']} in channel {channel_name}", virtu_logWARNING)
                    playlist.pop('interleave', None)
            save_channels(channels)
            all_playlists = []
            for playlist_idx, playlist in enumerate(playlists):
                playlist_type = playlist.get('playlist_type')
                source = playlist.get('source', 'playlist')
                progress_dialog.update(int(playlist_idx / len(playlists) * 100), f"Loading Playlist {playlist_idx}: {os.path.basename(playlist['path'])}")
                virtu_log(f"Processing playlist {playlist['path']} (type: {playlist_type}, source: {source}) for channel {channel_name}", virtu_logDEBUG)
                items, rule_order, is_random = get_playlist_items_with_durations(playlist['path'], playlist_type, source=source)
                if not items:
                    virtu_log(f"No items found for playlist {playlist['path']} in channel {channel_name}", virtu_logERROR)
                    continue
                virtu_log(f"Retrieved {len(items)} items for playlist {playlist['path']}, rule_order: {rule_order}, is_random: {is_random}", virtu_logDEBUG)
                item_groups = {}
                conn, cursor, db_type, _ = get_database_connection()
                if conn is None or cursor is None:
                    virtu_log(f"Failed to connect to database for playlist {playlist['path']}", virtu_logERROR)
                    continue
                try:
                    for item in items:
                        if not isinstance(item, dict) or 'file' not in item:
                            virtu_log(f"Invalid item in playlist {playlist['path']}: {item}", virtu_logWARNING)
                            continue
                        filename = os.path.basename(item['file'])
                        path = os.path.dirname(item['file']) + '/'
                        item_name = item.get('showtitle', item.get('title', os.path.basename(item['file'])))
                        item_description = item.get('plot', '')
                        item_categories = item.get('genre', [])
                        item_date = item.get('year', '')
                        item_icon = item.get('thumbnail', '')
                        if source == 'playlist':
                            if db_type == 'mysql':
                                query = (
                                    """
                                    SELECT tvshow.c00, tvshow.c01, tvshow.c08, tvshow.c05
                                    FROM episode
                                    JOIN tvshow ON episode.idShow = tvshow.idShow
                                    JOIN files ON episode.idFile = files.idFile
                                    JOIN path ON files.idPath = path.idPath
                                    WHERE files.strFilename = %s AND path.strPath = %s
                                    """ if playlist_type == 'episodes' else """
                                    SELECT movie.c00, movie.c01, movie.c06, movie.c07
                                    FROM movie
                                    JOIN files ON movie.idFile = files.idFile
                                    JOIN path ON files.idPath = path.idPath
                                    WHERE files.strFilename = %s AND path.strPath = %s
                                    """
                                )
                                params = (filename, path)
                            else:
                                query = (
                                    """
                                    SELECT tvshow.c00, tvshow.c01, tvshow.c08, tvshow.c05
                                    FROM episode
                                    JOIN tvshow ON episode.idShow = tvshow.idShow
                                    JOIN files ON episode.idFile = files.idFile
                                    JOIN path ON files.idPath = path.idPath
                                    WHERE files.strFilename = ? AND path.strPath = ?
                                    """ if playlist_type == 'episodes' else """
                                    SELECT movie.c00, movie.c01, movie.c06, movie.c07
                                    FROM movie
                                    JOIN files ON movie.idFile = files.idFile
                                    JOIN path ON files.idPath = path.idPath
                                    WHERE files.strFilename = ? AND path.strPath = ?
                                    """
                                )
                                params = (filename, path)
                            cursor.execute(query, params)
                            results = cursor.fetchall()
                            if results:
                                item_name, item_description, item_categories, item_date = results[0]
                                item_categories = item_categories.split(',') if item_categories else ['Unknown']
                            else:
                                virtu_log(f"No {'TV show' if playlist_type == 'episodes' else 'movie'} information found for {filename} in playlist {playlist['path']}. Using title or filename: {item_name}", virtu_logWARNING)
                        else:
                            item_name = os.path.basename(playlist['path']) if playlist_type == 'episodes' else item['title']
                            item_categories = ['Unknown'] if not item_categories else item_categories
                        if item_name not in item_groups:
                            item_groups[item_name] = {'items': [], 'description': item_description, 'categories': item_categories, 'date': item_date, 'icon': item_icon}
                        if item_name not in playlist['last_index']:
                            playlist['last_index'][item_name] = -1
                        item_groups[item_name]['items'].append(item)
                        virtu_log(f"Added item {filename} to group {item_name} in playlist {playlist['path']}", virtu_logDEBUG)
                    cursor.close()
                    conn.close()
                except Exception as e:
                    virtu_log(f"Error grouping items for playlist {playlist['path']}: {str(e)}", virtu_logERROR)
                    if conn:
                        cursor.close()
                        conn.close()
                    continue
                if not item_groups:
                    virtu_log(f"No valid {'episodes' if playlist_type == 'episodes' else 'movies'} found for playlist {playlist['path']} in channel {channel_name}", virtu_logERROR)
                    continue
                sorted_item_groups = {}
                ordered_shows = rule_order if rule_order else list(item_groups.keys())
                if source == 'playlist' and is_random and playlist_type == 'episodes':
                    random.shuffle(ordered_shows)
                    virtu_log(f"Randomized show order for playlist {playlist['path']}: {ordered_shows}", virtu_logDEBUG)
                elif playlist.get('fixed_show_order'):
                    ordered_shows = [show for show in playlist['fixed_show_order'] if show in item_groups]
                    virtu_log(f"Using fixed show order for playlist {playlist['path']}: {ordered_shows}", virtu_logINFO)
                elif playlist.get('shuffle_shows', False):
                    random.shuffle(ordered_shows)
                    virtu_log(f"Shuffled show order for playlist {playlist['path']}: {ordered_shows}", virtu_logDEBUG)
                for item_name in ordered_shows:
                    if item_name in item_groups:
                        sorted_item_groups[item_name] = item_groups[item_name]
                        if playlist_type == 'episodes' and source == 'playlist':
                            if is_random and (item_name not in playlist['random_order'] or not playlist['random_order'][item_name]):
                                playlist['random_order'][item_name] = list(range(len(item_groups[item_name]['items'])))
                                random.shuffle(playlist['random_order'][item_name])
                                virtu_log(f"Generated random order for {item_name} in playlist {playlist['path']}: {playlist['random_order'][item_name]}", virtu_logINFO)
                            if is_random:
                                sorted_item_groups[item_name]['items'] = [
                                    sorted_item_groups[item_name]['items'][i] for i in playlist['random_order'][item_name]
                                ]
                            else:
                                sorted_item_groups[item_name]['items'].sort(key=lambda x: (int(x.get('season', 1)), int(x.get('episode', 1))))
                        virtu_log(f"Playlist {playlist['path']} group {item_name} has {len(sorted_item_groups[item_name]['items'])} {'episodes' if playlist_type == 'episodes' else 'movies'}", virtu_logINFO)
                all_playlists.append({
                    'items': sorted_item_groups,
                    'interleave': playlist.get('interleave', {'low': 1, 'high': 1, 'count': 1}),
                    'last_index': playlist['last_index'],
                    'random_order': playlist['random_order'],
                    'type': playlist_type,
                    'path': playlist['path'],
                    'is_random': is_random,
                    'shuffle_shows': playlist.get('shuffle_shows', False)
                })
            if not all_playlists:
                virtu_log(f"No valid playlists for channel {channel_name}", virtu_logERROR)
                xbmcgui.Dialog().ok("Error", f"No valid playlists found for channel {channel_name}.")
                progress_dialog.close()
                return False
            indices = {
                playlist_idx: {item_name: max(0, playlist['last_index'].get(item_name, -1) + 1) for item_name in playlist['items']}
                for playlist_idx, playlist in enumerate(all_playlists)
            }
            item_counts = {
                playlist_idx: {item_name: len(item_data['items']) for item_name, item_data in playlist['items'].items()}
                for playlist_idx, playlist in enumerate(all_playlists)
            }
            added_duration = 0
            added_items = 0
            interleaved_track_new = []
            add_item_counters = {add_playlist_idx: 0 for add_playlist_idx in range(1, len(all_playlists))}
            try:
                while (limit_type == 'time' and added_duration < to_add_duration) or (limit_type == 'items' and added_items < to_add_items):
                    if limit_type == 'items':
                        progress = int(added_items / to_add_items * 100) if to_add_items > 0 else 0
                        message = f"Channel {channel_number}: Adding {added_items}/{to_add_items} items"
                    else:
                        progress = int(added_duration / to_add_duration * 100) if to_add_duration > 0 else 0
                        added_hours = added_duration / 3600
                        to_add_hours = to_add_duration / 3600
                        message = f"Channel {channel_number}: Adding {added_hours:.2f}/{to_add_hours:.2f} hours"
                    progress_dialog.update(progress, message)
                    base_items = list(all_playlists[0]['items'].keys())
                    if all_playlists[0].get('shuffle_shows', False):
                        random.shuffle(base_items)
                        virtu_log(f"Shuffled base items for cycle: {base_items}", virtu_logDEBUG)
                    base_cycle = []
                    for item_name in base_items:
                        if (limit_type == 'time' and added_duration >= to_add_duration) or (limit_type == 'items' and added_items >= to_add_items):
                            break
                        item_data = all_playlists[0]['items'].get(item_name, {'items': []})
                        items = item_data['items']
                        num_items = item_counts[0].get(item_name, 0)
                        if num_items == 0:
                            virtu_log(f"No items for base playlist group {item_name} in {all_playlists[0]['path']}", virtu_logWARNING)
                            continue
                        item_idx = indices[0][item_name] % num_items
                        item = items[item_idx]
                        base_cycle.append((0, item_name, item, item_idx))
                        episode_count = get_episode_count(os.path.basename(item['file'])) if all_playlists[0]['type'] == 'episodes' else 1
                        indices[0][item_name] = (indices[0][item_name] + episode_count) % num_items
                        virtu_log(f"Prepared base item {item_name} index {item_idx + 1}/{num_items} for cycle", virtu_logDEBUG)
                    if len(all_playlists) == 1:
                        for _, item_name, item, item_idx in base_cycle:
                            interleaved_track_new.append((item_name, item))
                            episode_count = get_episode_count(os.path.basename(item['file'])) if all_playlists[0]['type'] == 'episodes' else 1
                            all_playlists[0]['last_index'][item_name] = (item_idx + episode_count - 1) % item_counts[0][item_name]
                            virtu_log(
                                f"Added base item {item_name} index {item_idx + 1}/{item_counts[0][item_name]}: {item['title']} (episodes: {episode_count}) (single playlist), updated last_index to {(item_idx + episode_count - 1) % item_counts[0][item_name]}",
                                virtu_logDEBUG
                            )
                            added_items += 1
                            added_duration += item['duration']
                        continue
                    new_cycle_length = len(base_items)
                    for add_playlist_idx, add_playlist in enumerate(all_playlists[1:], 1):
                        count = max(1, add_playlist['interleave'].get('count', 1))
                        new_cycle_length += count
                        virtu_log(f"New cycle length for channel {channel_name}: {new_cycle_length} (base: {len(base_items)}, additional: {count})", virtu_logDEBUG)
                    insert_positions = {}
                    fixed_playlists = [(idx, p) for idx, p in enumerate(all_playlists[1:], 1) if p['interleave'].get('low') == p['interleave'].get('high')]
                    for add_playlist_idx, add_playlist in fixed_playlists:
                        low = add_playlist['interleave'].get('low', 1)
                        count = max(1, add_playlist['interleave'].get('count', 1))
                        if low < 1:
                            virtu_log(f"Invalid low value {low} for playlist {add_playlist['path']} in channel {channel_name}. Defaulting to 1.", virtu_logWARNING)
                            xbmcgui.Dialog().ok("Invalid Low Value", f"Low value {low} for playlist {add_playlist['path']} is invalid. Must be >= 1. Defaulting to 1.")
                            low = 1
                        if low > new_cycle_length:
                            virtu_log(f"Invalid low value {low} for playlist {add_playlist['path']} in channel {channel_name}. Defaulting to {new_cycle_length}.", virtu_logWARNING)
                            xbmcgui.Dialog().ok("Invalid Low Value", f"Low value {low} for playlist {add_playlist['path']} is invalid. Must be <= {new_cycle_length}. Defaulting to {new_cycle_length}.")
                            low = new_cycle_length
                        pos = low
                        insert_positions[add_playlist_idx] = pos
                        virtu_log(f"Assigned fixed position {pos} with count {count} to playlist {add_playlist['path']}", virtu_logDEBUG)
                    random_playlists = [(idx, p) for idx, p in enumerate(all_playlists[1:], 1) if p['interleave'].get('low') != p['interleave'].get('high')]
                    for add_playlist_idx, add_playlist in random_playlists:
                        low = add_playlist['interleave'].get('low', 1)
                        high = add_playlist['interleave'].get('high', 1)
                        count = max(1, add_playlist['interleave'].get('count', 1))
                        if low < 1:
                            virtu_log(f"Invalid low value {low} for playlist {add_playlist['path']} in channel {channel_name}. Defaulting to 1.", virtu_logWARNING)
                            xbmcgui.Dialog().ok("Invalid Low Value", f"Low value {low} for playlist {add_playlist['path']} is invalid. Must be >= 1. Defaulting to 1.")
                            low = 1
                        if high < low:
                            virtu_log(f"Invalid range: low ({low}) > high ({high}) for playlist {add_playlist['path']} in channel {channel_name}. Swapping values.", virtu_logWARNING)
                            xbmcgui.Dialog().ok("Invalid Interleave Range", f"Low value ({low}) > high value ({high}) for playlist {add_playlist['path']}. Swapping values.")
                            low, high = high, low
                        if high > new_cycle_length:
                            virtu_log(f"Invalid high value {high} for playlist {add_playlist['path']} in channel {channel_name}. Defaulting to {new_cycle_length}.", virtu_logWARNING)
                            xbmcgui.Dialog().ok("Invalid High Value", f"High value {high} for playlist {add_playlist['path']} is invalid. Must be <= {new_cycle_length}. Defaulting to {new_cycle_length}.")
                            high = new_cycle_length
                        valid_positions = [p for p in range(1, new_cycle_length + 1) if low <= p <= min(high, new_cycle_length - count + 1)]
                        pos = random.choice(valid_positions) if valid_positions else 1
                        insert_positions[add_playlist_idx] = pos
                        virtu_log(f"Assigned random position {pos} with count {count} to playlist {add_playlist['path']}", virtu_logDEBUG)
                    shuffled_available = {}
                    for add_playlist_idx in range(1, len(all_playlists)):
                        add_playlist = all_playlists[add_playlist_idx]
                        avail = list(add_playlist['items'].keys())
                        if not avail:
                            virtu_log(f"No available items for playlist {add_playlist['path']} in channel {channel_name}", virtu_logWARNING)
                            continue
                        if add_playlist.get('shuffle_shows', False):
                            random.shuffle(avail)
                            virtu_log(f"Shuffled additional items for cycle in playlist {add_playlist['path']}: {avail}", virtu_logDEBUG)
                        shuffled_available[add_playlist_idx] = avail
                    current_cycle = []
                    current_position = 0
                    base_idx = 0
                    while current_position < new_cycle_length and ((limit_type == 'time' and added_duration < to_add_duration) or (limit_type == 'items' and added_items < to_add_items)):
                        current_position += 1
                        inserted = False
                        for add_playlist_idx in sorted(insert_positions.keys()):
                            add_playlist = all_playlists[add_playlist_idx]
                            count = max(1, add_playlist['interleave'].get('count', 1))
                            if insert_positions[add_playlist_idx] <= current_position < insert_positions[add_playlist_idx] + count:
                                available_items = shuffled_available.get(add_playlist_idx, [])
                                if not available_items:
                                    virtu_log(f"No available items for playlist {add_playlist['path']} at position {current_position}", virtu_logWARNING)
                                    continue
                                item_idx = add_item_counters.get(add_playlist_idx, 0) % len(available_items)
                                item_name = available_items[item_idx]
                                item_data = add_playlist['items'].get(item_name, {'items': []})
                                items = item_data['items']
                                num_items = item_counts[add_playlist_idx].get(item_name, 0)
                                if num_items == 0:
                                    virtu_log(f"No items for playlist {add_playlist['path']} group {item_name}", virtu_logWARNING)
                                    continue
                                item_idx_episode = indices[add_playlist_idx][item_name] % num_items
                                item = items[item_idx_episode]
                                current_cycle.append((item_name, item))
                                episode_count = get_episode_count(os.path.basename(item['file'])) if add_playlist['type'] == 'episodes' else 1
                                add_playlist['last_index'][item_name] = (item_idx_episode + episode_count - 1) % num_items
                                virtu_log(
                                    f"Adding playlist {add_playlist_idx} {item_name} item {item_idx_episode + 1}/{num_items}: {item['title']} (episodes: {episode_count}) at position {current_position}, updated last_index to {(item_idx_episode + episode_count - 1) % num_items}",
                                    virtu_logDEBUG
                                )
                                added_items += 1
                                added_duration += item['duration']
                                indices[add_playlist_idx][item_name] = (indices[add_playlist_idx][item_name] + episode_count) % num_items
                                inserted = True
                                add_item_counters[add_playlist_idx] = add_item_counters.get(add_playlist_idx, 0) + 1
                                break
                        if not inserted and base_idx < len(base_cycle) and ((limit_type == 'time' and added_duration < to_add_duration) or (limit_type == 'items' and added_items < to_add_items)):
                            playlist_idx, item_name, item, item_idx = base_cycle[base_idx]
                            if item_name in item_counts[playlist_idx]:
                                current_cycle.append((item_name, item))
                                episode_count = get_episode_count(os.path.basename(item['file'])) if all_playlists[playlist_idx]['type'] == 'episodes' else 1
                                all_playlists[playlist_idx]['last_index'][item_name] = (item_idx + episode_count - 1) % item_counts[playlist_idx][item_name]
                                virtu_log(
                                    f"Adding playlist {playlist_idx} {item_name} item {item_idx + 1}/{item_counts[playlist_idx][item_name]}: {item['title']} (episodes: {episode_count}) at position {current_position}, updated last_index to {(item_idx + episode_count - 1) % item_counts[playlist_idx][item_name]}",
                                    virtu_logDEBUG
                                )
                                added_items += 1
                                added_duration += item['duration']
                            else:
                                virtu_log(f"Skipping base item {item_name} due to missing item_count in playlist {all_playlists[playlist_idx]['path']}", virtu_logWARNING)
                            base_idx += 1
                    interleaved_track_new.extend(current_cycle)
                    virtu_log(f"Completed cycle with {len(current_cycle)} items, total_items={added_items}, total_duration={added_duration}", virtu_logDEBUG)
            except Exception as e:
                virtu_log(f"Error adding new cycle for channel {channel_name}: {str(e)}", virtu_logERROR)
                progress_dialog.close()
                return False
            try:
                for playlist_idx, playlist in enumerate(all_playlists):
                    channel['playlists'][playlist_idx]['last_index'] = {
                        item_name: all_playlists[playlist_idx]['last_index'][item_name]
                        for item_name in playlist['items'] if item_name in item_counts[playlist_idx]
                    }
                    channel['playlists'][playlist_idx]['random_order'] = {
                        item_name: all_playlists[playlist_idx]['random_order'][item_name]
                        for item_name in playlist['items'] if item_name in all_playlists[playlist_idx]['random_order']
                        and isinstance(all_playlists[playlist_idx]['random_order'][item_name], list)
                    }
                    virtu_log(f"Updated last_index for playlist {playlist['path']}: {channel['playlists'][playlist_idx]['last_index']}", virtu_logDEBUG)
                save_channels(channels)
                virtu_log(f"Saved channels.json after updating last_index for channel {channel_name}", virtu_logINFO)
            except Exception as e:
                virtu_log(f"Error saving updated last_index for channel {channel_name}: {str(e)}", virtu_logERROR)
                progress_dialog.close()
                return False
            existing_m3u_lines = m3u_lines  # Use original m3u_lines from file
            m3u_lines = ['#EXTM3U']
            for line in existing_m3u_lines[removed_k*2+1:]:
                m3u_lines.append(line)
            playlist_type = all_playlists[0]['type'] if all_playlists else 'movies'
            for item_name, item in interleaved_track_new:
                duration = item.get('duration', 0)
                if duration <= 0:
                    virtu_log(f"Skipping item {item['title']} with invalid duration {duration}", virtu_logWARNING)
                    continue
                file_path = item['file']
                if not xbmcvfs.exists(file_path):
                    virtu_log(f"Inaccessible file path: {file_path}", virtu_logWARNING)
                    xbmcgui.Dialog().notification("VirtuaTV", f"Inaccessible file path: {file_path}", xbmcgui.NOTIFICATION_WARNING, 3000)
                    continue
                title = item['title']
                description = all_playlists[0]['items'].get(item_name, {}).get('description', '')
                season_episode = f"S{item.get('season', 0):02d}E{item.get('episode', 0):02d}" if playlist_type == 'episodes' and 'season' in item and 'episode' in item else ''
                entry_title = f"{item_name}//{title} ({season_episode})//{description}" if season_episode else f"{item_name}//{title}//{description}"
                m3u_lines.append(f'#EXTINF:{int(duration)},{entry_title}')
                m3u_lines.append(file_path)
            temp_m3u = os.path.join(xbmcvfs.translatePath("special://temp"), f"virtuatv_m3u_{os.getpid()}.m3u")
            with xbmcvfs.File(temp_m3u, 'w') as f:
                f.write('\n'.join(m3u_lines) + '\n')
            if not xbmcvfs.copy(temp_m3u, m3u_filename):
                virtu_log(f"Failed to update M3U file for channel {channel_name}", virtu_logERROR)
                xbmcvfs.delete(temp_m3u)
                progress_dialog.close()
                return False
            xbmcvfs.delete(temp_m3u)
            channel['last_gen_time'] = now.isoformat()
            channel['total_gen_duration'] = remaining_duration + added_duration
            channel['is_new'] = False
            save_channels(channels)
            virtu_log(f"Updated files for channel {channel_number}: removed {removed_k} items, added {added_items} items, total_duration={channel['total_gen_duration']}", virtu_logINFO)
            progress_dialog.close()
            return True
        except Exception as e:
            virtu_log(f"Error updating files for channel {channel_name}: {str(e)}", virtu_logERROR)
            progress_dialog.close()
            return False
    except Exception as e:
        virtu_log(f"Error updating files for channel {channel_number}: {str(e)}\n{traceback.format_exc()}", virtu_logERROR)
        if 'progress_dialog' in locals():
            progress_dialog.close()
        return False
        
def pre_load_channels():
    """Check and update channels on startup if outdated or new."""
    virtu_log("pre_load_channels: Starting channel update check", virtu_logINFO)
    channels = load_channels()
    if not channels:
        virtu_log("pre_load_channels: No channels exist, skipping update", virtu_logINFO)
        xbmcgui.Dialog().notification("VirtuaTV", "No channels found, skipping update", xbmcgui.NOTIFICATION_INFO, 3000, sound=False)
        return True
    xbmcgui.Dialog().notification("VirtuaTV", "Checking channels for updates...", xbmcgui.NOTIFICATION_INFO, 3000, sound=False)
    virtu_log("pre_load_channels: Displayed checking notification", virtu_logINFO)
    threshold_sec = int(ADDON.getSetting('auto_regen_threshold') or 24) * 3600
    successful = 0
    total_channels = len(channels)
    try:
        for idx, channel in enumerate(channels):
            channel_name = channel.get('name', 'Unknown')
            channel_number = channel.get('number', -1)
            mode = channel.get('mode', ADDON.getSetting('channel_mode').lower())
            try:
                if mode == 'resume' and channel.get('is_paused', False):
                    virtu_log(f"pre_load_channels: Channel {channel_name} (number {channel_number}) is paused in Resume Mode, skipping update", virtu_logINFO)
                    continue
                if 'last_gen_time' in channel and 'total_gen_duration' in channel:
                    last_gen = datetime.datetime.fromisoformat(channel['last_gen_time'])
                    time_passed = (datetime.datetime.now(datetime.timezone.utc) - last_gen).total_seconds()
                    time_left = channel['total_gen_duration'] - time_passed
                    virtu_log(f"pre_load_channels: Channel {channel_name} (number {channel_number}): time_left={time_left}, threshold={threshold_sec}, is_new={channel.get('is_new', False)}", virtu_logINFO)
                    if time_left < threshold_sec or channel.get('is_new', False):
                        virtu_log(f"pre_load_channels: Updating channel {channel_name} (time_left={time_left} < {threshold_sec} or is_new={channel.get('is_new', False)})", virtu_logINFO)
                        if update_channel_files(channel_number):
                            successful += 1
                            channel['is_new'] = False
                            virtu_log(f"pre_load_channels: Successfully updated channel {channel_name} ({channel_number})", virtu_logINFO)
                        else:
                            virtu_log(f"pre_load_channels: Failed to update channel {channel_name} ({channel_number})", virtu_logERROR)
                            xbmcgui.Dialog().notification("VirtuaTV", f"Failed to update channel {channel_name} ({channel_number})", xbmcgui.NOTIFICATION_ERROR, 3000, sound=False)
                    else:
                        virtu_log(f"pre_load_channels: Channel {channel_name} does not need update (time_left={time_left} >= {threshold_sec})", virtu_logINFO)
                else:
                    virtu_log(f"pre_load_channels: Channel {channel_name} missing last_gen_time or total_gen_duration, forcing update", virtu_logWARNING)
                    if update_channel_files(channel_number):
                        successful += 1
                        channel['is_new'] = False
                        virtu_log(f"pre_load_channels: Successfully updated channel {channel_name} ({channel_number})", virtu_logINFO)
                    else:
                        virtu_log(f"pre_load_channels: Failed to update channel {channel_name} ({channel_number})", virtu_logERROR)
                        xbmcgui.Dialog().notification("VirtuaTV", f"Failed to update channel {channel_name} ({channel_number})", xbmcgui.NOTIFICATION_ERROR, 3000, sound=False)
            except Exception as e:
                virtu_log(f"pre_load_channels: Error processing channel {channel_name}: {str(e)}", virtu_logERROR)
                xbmcgui.Dialog().notification("VirtuaTV", f"Error updating channel {channel_name}: {str(e)}", xbmcgui.NOTIFICATION_ERROR, 3000, sound=False)
        save_channels(channels)
        if successful > 0:
            if sync_files():
                xbmcgui.Dialog().notification("VirtuaTV", f"Updated {successful}/{total_channels} channels", xbmcgui.NOTIFICATION_INFO, 3000, sound=False)
                virtu_log(f"pre_load_channels: Successfully updated {successful}/{total_channels} channels", virtu_logINFO)
            else:
                xbmcgui.Dialog().notification("VirtuaTV", f"Updated {successful}/{total_channels} channels, but file sync failed", xbmcgui.NOTIFICATION_WARNING, 3000, sound=False)
                virtu_log(f"pre_load_channels: Updated {successful}/{total_channels} channels, but file sync failed", virtu_logWARNING)
        else:
            xbmcgui.Dialog().notification("VirtuaTV", "No channels needed update", xbmcgui.NOTIFICATION_INFO, 3000, sound=False)
            virtu_log("pre_load_channels: No channels needed update", virtu_logINFO)
        virtu_log("pre_load_channels: Displayed completion notification", virtu_logINFO)
        return True
    except Exception as e:
        virtu_log(f"pre_load_channels: Error during channel updates: {str(e)}", virtu_logERROR)
        xbmcgui.Dialog().notification("VirtuaTV", f"Channel update error: {str(e)}", xbmcgui.NOTIFICATION_ERROR, 3000, sound=False)
        return False

def select_playlist():
    playlist_dir = xbmcvfs.translatePath("special://profile/playlists/video/")
    if not xbmcvfs.exists(playlist_dir):
        xbmcgui.Dialog().ok("Error", "No playlists found in Kodi’s video playlist folder!")
        return None
    dirs, files = xbmcvfs.listdir(playlist_dir)
    playlists = [f for f in files if f.endswith('.xsp')]
    if not playlists:
        xbmcgui.Dialog().ok("Error", "No Smart Playlists found!")
        return None
    dialog = xbmcgui.Dialog()
    selected = dialog.select("Choose a Smart Playlist", playlists)
    if selected == -1:
        return None
    playlist_path = os.path.join(playlist_dir, playlists[selected])
    try:
        with xbmcvfs.File(playlist_path, 'r') as f:
            xml_content = f.read()
        tree = ET.fromstring(xml_content)
        playlist_type = tree.attrib.get('type')
        if playlist_type is None or playlist_type not in ['episodes', 'movies']:
            xbmcgui.Dialog().ok("Error", "Selected playlist must be of type 'episodes' or 'movies'!")
            return None
        return {'path': playlist_path, 'type': playlist_type}
    except Exception as e:
        xbmcgui.Dialog().ok("Error", f"Could not read playlist: {str(e)}")
        return None

def get_latest_db_version():
    db_dir = xbmcvfs.translatePath('special://database/')
    db_files = [f for f in xbmcvfs.listdir(db_dir)[1] if f.startswith('MyVideos') and f.endswith('.db')]
    if db_files:
        versions = [int(re.search(r'MyVideos(\d+).db', f).group(1)) for f in db_files]
        return max(versions)
    return 131  # Default to latest known if no files found

def get_database_connection():
    db_host = ADDON.getSetting('db_host')
    db_port = int(ADDON.getSetting('db_port') or 3306)
    db_user = ADDON.getSetting('db_user')
    db_pass = ADDON.getSetting('db_pass')
    db_name = ADDON.getSetting('db_name') or 'MyVideos'
    conn = None
    cursor = None
    db_type = None
    db_version = ''
    latest_version = get_latest_db_version()
    if db_host:
        if mysql is None:
            virtu_log("VirtuaTV: MySQL connector not available, cannot connect to remote DB", virtu_logERROR)
            xbmcgui.Dialog().ok("DB Error", "MySQL connector not imported, cannot connect to remote database.")
            return None, None, None, ''
        if not re.match(r'.*\d+$', db_name.lower()):
            db_name += str(latest_version)
        try:
            conn = mysql.connector.connect(host=db_host, port=db_port, user=db_user, password=db_pass, database=db_name)
            cursor = conn.cursor(buffered=True)
            db_type = 'mysql'
            db_version = re.search(r'\d+$', db_name).group(0) if re.search(r'\d+$', db_name) else ''
            virtu_log(f"VirtuaTV: Connected to MySQL DB using settings with database {db_name}", virtu_logINFO)
            return conn, cursor, db_type, db_version
        except Exception as e:
            virtu_log(f"VirtuaTV: MySQL connection error using settings: {str(e)}", virtu_logERROR)
            xbmcgui.Dialog().ok("DB Error", f"Failed to connect to MySQL using settings: {str(e)}")
    else:
        adv_path = xbmcvfs.translatePath('special://profile/advancedsettings.xml')
        if xbmcvfs.exists(adv_path):
            try:
                with xbmcvfs.File(adv_path, 'r') as f:
                    content = f.read()
                tree = ET.fromstring(content)
                videodb = tree.find('./videodatabase')
                if videodb is not None:
                    db_type_elem = videodb.find('type')
                    if db_type_elem is not None and db_type_elem.text == 'mysql':
                        if mysql is None:
                            virtu_log("VirtuaTV: MySQL connector not available for advancedsettings.xml config", virtu_logERROR)
                            return None, None, None, ''
                        db_host = videodb.find('host').text if videodb.find('host') is not None else None
                        db_port = int(videodb.find('port').text) if videodb.find('port') is not None else 3306
                        db_user = videodb.find('user').text if videodb.find('user') is not None else None
                        db_pass = videodb.find('pass').text if videodb.find('pass') is not None else None
                        db_name = videodb.find('name').text if videodb.find('name') is not None else 'myvideos'
                        if not re.match(r'.*\d+$', db_name.lower()):
                            db_name += str(latest_version)
                        if db_host and db_user:
                            try:
                                conn = mysql.connector.connect(host=db_host, port=db_port, user=db_user, password=db_pass, database=db_name)
                                cursor = conn.cursor(buffered=True)
                                db_type = 'mysql'
                                db_version = re.search(r'\d+$', db_name).group(0) if re.search(r'\d+$', db_name) else ''
                                virtu_log(f"VirtuaTV: Connected to MySQL DB using advancedsettings.xml with database {db_name}", virtu_logINFO)
                                return conn, cursor, db_type, db_version
                            except Exception as e:
                                virtu_log(f"VirtuaTV: MySQL connection error using advancedsettings.xml: {str(e)}", virtu_logERROR)
                                xbmcgui.Dialog().ok("DB Error", f"Failed to connect to MySQL using advancedsettings.xml: {str(e)}")
            except Exception as e:
                virtu_log(f"VirtuaTV: Error parsing advancedsettings.xml: {str(e)}", virtu_logERROR)
        # Fallback to local SQLite if no MySQL or error
        db_dir = xbmcvfs.translatePath('special://database/')
        db_files = [f for f in xbmcvfs.listdir(db_dir)[1] if f.startswith('MyVideos') and f.endswith('.db')]
        if db_files:
            db_files.sort(key=lambda f: int(re.search(r'MyVideos(\d+).db', f).group(1)), reverse=True)
            db_path = os.path.join(db_dir, db_files[0])
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                db_type = 'sqlite'
                db_version = re.search(r'MyVideos(\d+).db', db_path).group(1) if re.search(r'MyVideos(\d+).db', db_path) else ''
                virtu_log(f"VirtuaTV: Connected to local SQLite DB: {db_path}", virtu_logINFO)
            except Exception as e:
                virtu_log(f"VirtuaTV: SQLite connection error: {str(e)}", virtu_logERROR)
                xbmcgui.Dialog().ok("DB Error", f"Failed to connect to local SQLite: {str(e)}")
        else:
            virtu_log("VirtuaTV: No MyVideos database found", virtu_logERROR)
            xbmcgui.Dialog().ok("DB Error", "No video database found. Ensure your library is scanned.")
    return conn, cursor, db_type, db_version

def get_playlist_items_with_durations(playlist_path, playlist_type, source='playlist'):
    if playlist_type not in ['episodes', 'movies']:
        virtu_log(f"Invalid playlist_type '{playlist_type}' for {playlist_path}", virtu_logERROR)
        xbmcgui.Dialog().ok("Error", f"Invalid playlist type '{playlist_type}'. Must be 'episodes' or 'movies'.")
        return [], [], False
    log_dir = xbmcvfs.translatePath("special://profile/addon_data/plugin.video.virtuatv/")
    log_file = os.path.join(log_dir, "missing_durations.log")
    if not xbmcvfs.exists(log_dir):
        xbmcvfs.mkdirs(log_dir)
    rule_order = []
    is_random = False
    items = []
    if source == 'folder':
        # Handle folder-based sources
        durations_file = xbmcvfs.translatePath("special://profile/addon_data/plugin.video.virtuatv/durations.json")
        durations = {}
        if xbmcvfs.exists(durations_file):
            try:
                with xbmcvfs.File(durations_file, 'r') as f:
                    data = json.loads(f.read())
                    durations = data.get('durations', {})
            except Exception as e:
                virtu_log(f"Error loading durations from {durations_file}: {str(e)}", virtu_logERROR)
                xbmcgui.Dialog().ok("Error", f"Failed to load durations: {str(e)}")
                return [], [], False
        video_extensions = ('.mp4', '.mkv', '.avi', '.m4v', '.ts', '.mov')
        video_files = []
        try:
            dirs, files = xbmcvfs.listdir(playlist_path)
            files.sort()
            for file in files:
                if file.lower().endswith(video_extensions):
                    video_files.append(os.path.join(playlist_path, file))
            dirs.sort()
            for dir in dirs:
                subfolder = os.path.join(playlist_path, dir)
                sub_files = get_video_files_from_folder(subfolder)
                sub_files.sort()
                video_files.extend(sub_files)
        except Exception as e:
            virtu_log(f"Error listing files in folder {playlist_path}: {str(e)}", virtu_logERROR)
            xbmcgui.Dialog().ok("Error", f"Failed to list files in folder {playlist_path}: {str(e)}")
            return [], [], False
        if not video_files:
            virtu_log(f"No video files found in folder {playlist_path}", virtu_logERROR)
            xbmcgui.Dialog().ok("Error", f"No video files found in folder {playlist_path}.")
            return [], [], False
        for file in video_files:
            duration = durations.get(file, 0)
            if duration <= 0:
                log_entry = f"{playlist_type.upper()} SKIPPED: File='{file}', Reason='Missing or invalid duration'\n"
                with xbmcvfs.File(log_file, 'a') as f:
                    f.write(log_entry)
                virtu_log(f"Skipped {file} due to missing/invalid duration", virtu_logWARNING)
                continue
            item_dict = {
                'title': os.path.basename(file),
                'file': file,
                'duration': int(duration),
                'season': -1,
                'episode': -1,
                'studio': [],
                'showtitle': os.path.basename(playlist_path) if playlist_type == 'episodes' else ''
            }
            items.append(item_dict)
            virtu_log(f"Added folder item {file} with duration {duration} seconds", virtu_logDEBUG)
        if not items:
            virtu_log(f"No valid items with durations found in folder {playlist_path}", virtu_logERROR)
            xbmcgui.Dialog().ok("Error", f"No valid video files with durations found in folder {playlist_path}. Check missing_durations.log for details.")
            return [], [], False
        virtu_log(f"Finalized {len(items)} valid items for folder {playlist_path}", virtu_logINFO)
        return items, rule_order, is_random
    # Smart Playlist handling
    try:
        playlist_path = xbmcvfs.translatePath(playlist_path).replace('\\', '/')
        if not playlist_path.startswith('special://'):
            playlist_path = f"special://profile/playlists/video/{os.path.basename(playlist_path)}"
        virtu_log(f"Normalized playlist path: {playlist_path}", virtu_logDEBUG)
        if not xbmcvfs.exists(playlist_path):
            xbmcgui.Dialog().ok("Error", f"Playlist file {playlist_path} does not exist. Ensure it is in special://profile/playlists/video/.")
            virtu_log(f"Playlist file {playlist_path} not found", virtu_logERROR)
            return [], rule_order, is_random
        with xbmcvfs.File(playlist_path, 'r') as f:
            xml_content = f.read()
        tree = ET.fromstring(xml_content)
        xsp_type = tree.attrib.get('type')
        order_element = tree.find('.//order')
        if order_element is not None and order_element.text == 'random':
            is_random = True
            virtu_log(f"Detected random order for playlist {playlist_path}", virtu_logINFO)
        else:
            virtu_log(f"No random order detected for playlist {playlist_path}, using default order", virtu_logDEBUG)
        for smartplaylist in tree.findall('.//smartplaylist'):
            for rule in smartplaylist.findall('.//rule'):
                if rule.get('field') in ['title', 'tvshow', 'studio']:
                    value = rule.find('value').text if rule.find('value') is not None else rule.text
                    if value and isinstance(value, str):
                        if rule.get('field') == 'tvshow' and playlist_type == 'episodes':
                            rule_order.append(value)
                        elif rule.get('field') == 'title' and playlist_type == 'movies':
                            rule_order.append(value)
                        elif rule.get('field') == 'studio' and value not in rule_order:
                            rule_order.append(value)
        if xsp_type != playlist_type:
            xbmcgui.Dialog().ok("Error", f"Playlist {playlist_path} type '{xsp_type}' does not match specified type '{playlist_type}'.")
            virtu_log(f"Playlist type mismatch: .xsp type '{xsp_type}' vs specified '{playlist_type}'", virtu_logERROR)
            return [], rule_order, is_random
    except Exception as e:
        virtu_log(f"Error loading or parsing playlist {playlist_path}: {str(e)}", virtu_logERROR)
        xbmcgui.Dialog().ok("Error", f"Failed to load or parse playlist {playlist_path}: {str(e)}")
        return [], rule_order, is_random
    virtu_log(f"Resolving {playlist_path} via Files.GetDirectory", virtu_logDEBUG)
    json_query = {
        "jsonrpc": "2.0",
        "method": "Files.GetDirectory",
        "params": {
            "directory": playlist_path,
            "media": "video",
            "properties": ["file", "title", "runtime", "showtitle", "season", "episode", "genre", "year", "director", "playcount", "rating", "studio"]
        },
        "id": 1
    }
    try:
        result = json.loads(xbmc.executeJSONRPC(json.dumps(json_query)))
        if "result" not in result or "files" not in result["result"]:
            xbmcgui.Dialog().ok("Error", f"No {'movie' if playlist_type == 'movies' else 'TV show'} items found in {playlist_path}. Ensure the playlist is valid and media is indexed in the Kodi library.")
            virtu_log(f"No items found via Files.GetDirectory for {playlist_path}: {result.get('error', 'No result')}", virtu_logERROR)
            return [], rule_order, is_random
        files = result["result"]["files"]
        virtu_log(f"Files.GetDirectory returned {len(files)} files for {playlist_path}", virtu_logDEBUG)
        for item in files:
            if (playlist_type == 'episodes' and item.get('type') == 'episode') or (playlist_type == 'movies' and item.get('type') == 'movie'):
                file_path = item['file'].replace('\\', '/')
                item_dict = {
                    'title': item.get('title', os.path.basename(file_path)),
                    'file': file_path,
                    'season': item.get('season', -1) if playlist_type == 'episodes' else -1,
                    'episode': item.get('episode', -1) if playlist_type == 'episodes' else -1,
                    'studio': item.get('studio', []),
                    'showtitle': item.get('showtitle', '') if playlist_type == 'episodes' else ''
                }
                items.append(item_dict)
        virtu_log(f"Retrieved {len(items)} {'movies' if playlist_type == 'movies' else 'episodes'} via Files.GetDirectory for {playlist_path}", virtu_logINFO)
    except Exception as e:
        virtu_log(f"Files.GetDirectory error for {playlist_path}: {str(e)}", virtu_logERROR)
        xbmcgui.Dialog().ok("Error", f"Failed to resolve playlist {playlist_path}: {str(e)}. Ensure the playlist is valid and media is indexed in the Kodi library.")
        return [], rule_order, is_random
    if not items:
        xbmcgui.Dialog().ok("Error", f"No {'movie' if playlist_type == 'movies' else 'TV show'} items found in {playlist_path}. Check missing_durations.log for details.")
        virtu_log(f"No {'movies' if playlist_type == 'movies' else 'episodes'} found for {playlist_path}", virtu_logERROR)
        return [], rule_order, is_random
    if is_random and playlist_type == 'episodes':
        random.shuffle(items)
        virtu_log(f"Randomized {len(items)} episodes for playlist {playlist_path}", virtu_logINFO)
    conn, cursor, db_type, db_version = get_database_connection()
    if conn is None or cursor is None:
        virtu_log(f"Failed to connect to database for {playlist_path}", virtu_logERROR)
        return [], rule_order, is_random
    final_items = []
    for item in items:
        filename = os.path.basename(item['file'])
        path = os.path.dirname(item['file']) + '/'
        try:
            if db_type == 'mysql':
                query = (
                    """
                    SELECT streamdetails.iVideoDuration, tvshow.c12
                    FROM streamdetails
                    JOIN files ON streamdetails.idFile = files.idFile
                    JOIN path ON files.idPath = path.idPath
                    JOIN episode ON files.idFile = episode.idFile
                    JOIN tvshow ON episode.idShow = tvshow.idShow
                    WHERE files.strFilename = %s AND path.strPath = %s AND streamdetails.iStreamType = 0
                    LIMIT 1
                    """ if playlist_type == 'episodes' else """
                    SELECT streamdetails.iVideoDuration, movie.c12
                    FROM streamdetails
                    JOIN files ON streamdetails.idFile = files.idFile
                    JOIN path ON files.idPath = path.idPath
                    JOIN movie ON files.idFile = movie.idFile
                    WHERE files.strFilename = %s AND path.strPath = %s AND streamdetails.iStreamType = 0
                    LIMIT 1
                    """
                )
                params = (filename, path)
            else:  # sqlite
                query = (
                    """
                    SELECT streamdetails.iVideoDuration, tvshow.c12
                    FROM streamdetails
                    JOIN files ON streamdetails.idFile = files.idFile
                    JOIN path ON files.idPath = path.idPath
                    JOIN episode ON files.idFile = episode.idFile
                    JOIN tvshow ON episode.idShow = tvshow.idShow
                    WHERE files.strFilename = ? AND path.strPath = ? AND streamdetails.iStreamType = 0
                    LIMIT 1
                    """ if playlist_type == 'episodes' else """
                    SELECT streamdetails.iVideoDuration, movie.c12
                    FROM streamdetails
                    JOIN files ON streamdetails.idFile = files.idFile
                    JOIN path ON files.idPath = path.idPath
                    JOIN movie ON files.idFile = movie.idFile
                    WHERE files.strFilename = ? AND path.strPath = ? AND streamdetails.iStreamType = 0
                    LIMIT 1
                    """
                )
                params = (filename, path)
            cursor.execute(query, params)
            result = cursor.fetchone()
            if result and result[0] and result[0] > 0:
                item['duration'] = int(result[0])
                item['studio'] = result[1] if result[1] else item.get('studio', [])
                final_items.append(item)
                virtu_log(f"Found duration {item['duration']} seconds and studio {item['studio']} for {item['title']}", virtu_logDEBUG)
            else:
                log_entry = f"{playlist_type.upper()} SKIPPED: Title='{item['title']}', File='{item['file']}', Reason='Missing or invalid duration in database'\n"
                with xbmcvfs.File(log_file, 'a') as f:
                    f.write(log_entry)
                virtu_log(f"Skipped {item['title']} due to missing/invalid duration in database for {playlist_path}", virtu_logWARNING)
        except Exception as e:
            log_entry = f"{playlist_type.upper()} SKIPPED: Title='{item['title']}', File='{item['file']}', Reason='Database error: {str(e)}'\n"
            with xbmcvfs.File(log_file, 'a') as f:
                f.write(log_entry)
            virtu_log(f"Error fetching duration for {item['title']} in {playlist_path}: {str(e)}", virtu_logERROR)
    if conn:
        conn.close()
    if not final_items:
        xbmcgui.Dialog().ok("Error", f"No valid {'movie' if playlist_type == 'movies' else 'TV show'} items with durations found in {playlist_path}. Check missing_durations.log for details.")
        virtu_log(f"No valid {'movies' if playlist_type == 'movies' else 'episodes'} with durations found for {playlist_path}", virtu_logERROR)
        return [], rule_order, is_random
    virtu_log(f"Finalized {len(final_items)} valid items for {playlist_path}", virtu_logINFO)
    return final_items, rule_order, is_random

def get_interleave_values(dialog):
    """Get interleave values (low, high, count) with validation and swapping if low > high."""
    while True:
        low_str = dialog.input("Enter Low value for interleave (default 1)", "1")
        if low_str is None:
            virtu_log("get_interleave_values: User cancelled low value input", virtu_logINFO)
            return None
        high_str = dialog.input("Enter High value for interleave (default 1)", "1")
        if high_str is None:
            virtu_log("get_interleave_values: User cancelled high value input", virtu_logINFO)
            return None
        count_str = dialog.input("Enter Count value for interleave (default 1)", "1")
        if count_str is None:
            virtu_log("get_interleave_values: User cancelled count value input", virtu_logINFO)
            return None
        try:
            low = int(low_str) if low_str else 1
            high = int(high_str) if high_str else 1
            count = int(count_str) if count_str else 1
            if low < 1 or high < 1 or count < 1:
                raise ValueError("Values must be at least 1")
            if low > high:
                virtu_log(f"get_interleave_values: Swapping low ({low}) and high ({high}) as low > high", virtu_logINFO)
                low, high = high, low
            virtu_log(f"get_interleave_values: Returning low={low}, high={high}, count={count}", virtu_logDEBUG)
            return {'low': low, 'high': high, 'count': count}
        except ValueError as e:
            virtu_log(f"get_interleave_values: Invalid input: {str(e)}", virtu_logERROR)
            dialog.ok("Invalid Input", f"{str(e)}. Try again.")

def create_channel():
    dialog = xbmcgui.Dialog()
    virtu_log("Starting channel creation", virtu_logINFO)
    conn, cursor, db_type, db_version = get_database_connection()
    if conn:
        if db_type == 'mysql':
            db_info = f"Using MySQL Database MyVideos{db_version}"
        else:
            db_info = f"Using Local SQLite Database MyVideos{db_version}"
        dialog.notification("VirtuaTV", db_info, xbmcgui.NOTIFICATION_INFO, 5000)
        conn.close()
    else:
        dialog.notification("Database Error", "Unable to connect to database. Channel creation may fail.", xbmcgui.NOTIFICATION_WARNING, 5000)
    channel_name = dialog.input("Enter Channel Name")
    if not channel_name:
        xbmcgui.Dialog().ok("Error", "You must enter a channel name!")
        virtu_log("Channel creation aborted: no name provided", virtu_logINFO)
        return
    max_channels = int(ADDON.getSetting('number_of_channels') or 50)
    channels = load_channels()
    if LOG_VERBOSE:
        virtu_log(f"Loaded {len(channels)} existing channels before creating new channel: {json.dumps(channels, indent=2)}", virtu_logDEBUG)
    if len(channels) >= max_channels:
        xbmcgui.Dialog().ok("Error", f"Cannot create new channel: limit of {max_channels} channels reached!")
        virtu_log(f"Channel creation aborted: max channels ({max_channels}) reached", virtu_logINFO)
        return
    used_numbers = get_used_channel_numbers()
    channel_options = [f"{ch['name']} ({ch['number']})" for ch in channels]
    available_numbers = [i for i in range(1, max_channels + 1) if i not in used_numbers]
    channel_options.extend([f"New Number: {num}" for num in available_numbers[:10]])
    selected = dialog.select("Select Channel Number (Existing channels shown)", channel_options)
    if selected == -1:
        virtu_log("User canceled number selection, prompting for manual input", virtu_logINFO)
        suggested_number = suggest_channel_number()
        channel_number = dialog.input(f"Enter Channel Number (Suggested: {suggested_number})", str(suggested_number))
        try:
            channel_number = int(channel_number)
            if channel_number < 1 or channel_number > max_channels:
                raise ValueError
        except ValueError:
            xbmcgui.Dialog().ok("Error", f"Channel number must be between 1 and {max_channels}!")
            virtu_log(f"Channel creation aborted: invalid channel number {channel_number}", virtu_logINFO)
            return
    else:
        if selected < len(channels):
            xbmcgui.Dialog().ok("Error", f"Channel number {channel_options[selected].split('(')[-1][:-1]} is already used!")
            virtu_log("Channel creation aborted: selected existing number", virtu_logINFO)
            return
        channel_number = int(channel_options[selected].split(': ')[1])
        if channel_number in used_numbers:
            xbmcgui.Dialog().ok("Error", f"Channel number {channel_number} is already used!")
            virtu_log(f"Channel creation aborted: number {channel_number} already used", virtu_logINFO)
            return
    limit_options = ["Time Based (Duration)", "Item Based (Count)"]
    selected_limit = dialog.select("Select Playlist Limit Type", limit_options)
    if selected_limit == -1:
        selected_limit = 0  # Default to time
    limit_type = 'time' if selected_limit == 0 else 'items'
    channel = {
        'name': channel_name,
        'number': channel_number,
        'playlists': [],
        'is_new': True,
        'limit_type': limit_type
    }
    if LOG_VERBOSE:
        virtu_log(f"Created new channel object: {json.dumps(channel, indent=2)}", virtu_logDEBUG)
    virtu_log("Prompting for base playlist or folder", virtu_logINFO)
    source_type = dialog.select("Select Source Type", ["Smart Playlist", "Folder"])
    if source_type == -1:
        virtu_log("Channel creation aborted: no source selected", virtu_logINFO)
        return
    if source_type == 0:  # Smart Playlist
        playlist = select_playlist()
        if not playlist:
            virtu_log("Channel creation aborted: no base playlist selected", virtu_logINFO)
            return
        progress_dialog = xbmcgui.DialogProgressBG()
        progress_dialog.create("VirtuaTV", f"Validating playlist {os.path.basename(playlist['path'])}...")
        items, rule_order, is_random = get_playlist_items_with_durations(playlist['path'], playlist['type'], source='playlist')
        if not items:
            xbmcgui.Dialog().ok("Error", f"No valid items found in playlist {playlist['path']}. Ensure it contains valid {playlist['type']} and is indexed in the Kodi database.")
            virtu_log(f"No valid items found in base playlist {playlist['path']}", virtu_logINFO)
            progress_dialog.close()
            return
        total_duration = sum(item['duration'] for item in items)
        total_hours = total_duration / 3600
        progress_dialog.update(100, "VirtuaTV", f"Found {len(items)} items, total duration {total_hours:.2f} hours")
        xbmc.sleep(1000)
        progress_dialog.close()
        xbmcgui.Dialog().ok("Playlist Info", f"Found {len(items)} items with total duration {total_hours:.2f} hours!")
        virtu_log(f"Base playlist validated with {len(items)} items", virtu_logINFO)
        shuffle_shows = False
        fixed_show_order = None
        if playlist['type'] == 'episodes':
            randomization_options = ["No randomization", "Shuffle TV shows each cycle", "Set fixed random TV show order"]
            selected_random = dialog.select("TV Show Order Randomization", randomization_options)
            if selected_random == -1:
                selected_random = 0  # Default to no
            if selected_random == 1:
                shuffle_shows = True
            elif selected_random == 2:
                unique_shows = sorted(set(item.get('showtitle', '') for item in items if item.get('showtitle')))
                if not unique_shows:
                    dialog.ok("Error", "No TV shows found in the playlist.")
                    virtu_log(f"No TV shows found in playlist {playlist['path']}", virtu_logERROR)
                    return
                current_order = unique_shows[:]
                while True:
                    order_text = "\n".join(f"{i+1}. {show}" for i, show in enumerate(current_order))
                    dialog.textviewer("Current TV Show Order", order_text)
                    options = ["Randomize", "Save", "Cancel"]
                    sel = dialog.select("Options", options)
                    if sel == 0:  # Randomize
                        random.shuffle(current_order)
                    elif sel == 1:  # Save
                        fixed_show_order = current_order
                        break
                    elif sel == -1 or sel == 2:  # Cancel
                        break
        channel['playlists'].append({
            'path': playlist['path'],
            'type': 'base',
            'playlist_type': playlist['type'],
            'last_index': {},
            'random_order': {},
            'shuffle_shows': shuffle_shows,
            'fixed_show_order': fixed_show_order,
            'source': 'playlist'
        })
    else:  # Folder
        folder = select_folder()
        if not folder:
            virtu_log("Channel creation aborted: no folder selected", virtu_logINFO)
            return
        durations_file = xbmcvfs.translatePath("special://profile/addon_data/plugin.video.virtuatv/durations.json")
        durations = {}
        last_scan = 0
        if xbmcvfs.exists(durations_file):
            try:
                with xbmcvfs.File(durations_file, 'r') as f:
                    data = json.loads(f.read())
                    durations = data.get('durations', {})
                    last_scan = data.get('last_scan', 0)
            except Exception as e:
                virtu_log(f"Error loading durations from {durations_file}: {str(e)}", virtu_logERROR)
        video_extensions = ('.mp4', '.mkv', '.avi', '.m4v', '.ts', '.mov')
        video_files = []
        try:
            dirs, files = xbmcvfs.listdir(folder)
            files.sort()
            for file in files:
                if file.lower().endswith(video_extensions):
                    video_files.append(os.path.join(folder, file))
            dirs.sort()
            for dir in dirs:
                subfolder = os.path.join(folder, dir)
                sub_files = get_video_files_from_folder(subfolder)
                sub_files.sort()
                video_files.extend(sub_files)
        except Exception as e:
            xbmcgui.Dialog().ok("Error", f"Failed to list files in folder {folder}: {str(e)}")
            virtu_log(f"Error listing files in folder {folder}: {str(e)}", virtu_logERROR)
            return
        if not video_files:
            xbmcgui.Dialog().ok("Error", f"No video files found in folder {folder}.")
            virtu_log(f"No video files found in folder {folder}", virtu_logERROR)
            return
        missing_durations = [f for f in video_files if f not in durations or durations[f] <= 0]
        if not missing_durations:
            total_duration = sum(durations[f] for f in video_files if f in durations)
            total_hours = total_duration / 3600
            xbmcgui.Dialog().ok("Folder Info", f"Found {len(video_files)} items with total duration {total_hours:.2f} hours (using cached durations).")
            virtu_log(f"Folder {folder} has {len(video_files)} items with cached durations, total duration {total_hours:.2f} hours", virtu_logINFO)
        else:
            scan_now = dialog.yesno("Scan Folder", f"Durations missing for {len(missing_durations)}/{len(video_files)} files in folder. Scan now to retrieve durations? (Required for channel creation.)")
            if not scan_now:
                xbmcgui.Dialog().ok("Error", f"Cannot create channel: {len(missing_durations)} files lack valid durations. Please scan durations or select a different folder.")
                virtu_log(f"Channel creation aborted: {len(missing_durations)} files in {folder} lack durations", virtu_logINFO)
                return
            durations = scan_folder_durations(folder, force_scan=True)
            if not durations:
                xbmcgui.Dialog().ok("Error", f"No valid video files with durations found in folder {folder}.")
                virtu_log(f"No valid durations found for folder {folder}", virtu_logERROR)
                return
            total_duration = sum(durations.values())
            total_hours = total_duration / 3600
            xbmcgui.Dialog().ok("Folder Info", f"Scanned {len(durations)} items with total duration {total_hours:.2f} hours!")
            virtu_log(f"Scanned {len(durations)} items for folder {folder}, total duration {total_hours:.2f} hours", virtu_logINFO)
        items, rule_order, is_random = get_playlist_items_with_durations(folder, 'episodes', source='folder')
        if not items:
            xbmcgui.Dialog().ok("Error", f"No valid items found in folder {folder}. Ensure it contains valid video files with durations.")
            virtu_log(f"No valid items found in folder {folder}", virtu_logINFO)
            return
        shuffle_shows = False
        fixed_show_order = None
        randomization_options = ["No randomization", "Shuffle items each cycle", "Set fixed random item order"]
        selected_random = dialog.select("Item Order Randomization", randomization_options)
        if selected_random == -1:
            selected_random = 0  # Default to no
        if selected_random == 1:
            shuffle_shows = True
        elif selected_random == 2:
            unique_items = sorted(set(item['title'] for item in items))
            if not unique_items:
                dialog.ok("Error", "No items found in the folder.")
                virtu_log(f"No items found in folder {folder}", virtu_logERROR)
                return  # Changed from continue to return to avoid syntax error
            current_order = unique_items[:]
            while True:
                order_text = "\n".join(f"{i+1}. {item}" for i, item in enumerate(current_order))
                dialog.textviewer("Current Item Order", order_text)
                options = ["Randomize", "Save", "Cancel"]
                sel = dialog.select("Options", options)
                if sel == 0:  # Randomize
                    random.shuffle(current_order)
                elif sel == 1:  # Save
                    fixed_show_order = current_order
                    break
                elif sel == -1 or sel == 2:  # Cancel
                    break
        channel['playlists'].append({
            'path': folder,
            'type': 'base',
            'playlist_type': 'episodes',
            'last_index': {},
            'random_order': {},
            'shuffle_shows': shuffle_shows,
            'fixed_show_order': fixed_show_order,
            'source': 'folder'
        })
    while True:
        if LOG_VERBOSE:
            virtu_log("Showing add playlist dialog", virtu_logDEBUG)
        if not dialog.yesno("Add Playlist/Folder", "Would you like to add another playlist or folder to this channel?"):
            if LOG_INFO:
                virtu_log("User declined to add another playlist", virtu_logINFO)
            break
        source_type = dialog.select("Select Source Type", ["Smart Playlist", "Folder"])
        if source_type == -1:
            virtu_log("No additional source selected, continuing", virtu_logINFO)
            continue
        if source_type == 0:  # Smart Playlist
            playlist = select_playlist()
            if not playlist:
                virtu_log("No additional playlist selected, continuing", virtu_logINFO)
                continue
            progress_dialog = xbmcgui.DialogProgressBG()
            progress_dialog.create("VirtuaTV", f"Validating playlist {os.path.basename(playlist['path'])}...")
            items, rule_order, is_random = get_playlist_items_with_durations(playlist['path'], playlist['type'], source='playlist')
            if not items:
                xbmcgui.Dialog().ok("Error", f"No valid items found in playlist {playlist['path']}. Ensure it contains valid {playlist['type']} and is indexed in the Kodi database.")
                virtu_log(f"No valid items found in additional playlist {playlist['path']}", virtu_logINFO)
                progress_dialog.close()
                continue
            total_duration = sum(item['duration'] for item in items)
            total_hours = total_duration / 3600
            progress_dialog.update(100, "VirtuaTV", f"Found {len(items)} items, total duration {total_hours:.2f} hours")
            xbmc.sleep(1000)
            progress_dialog.close()
            xbmcgui.Dialog().ok("Playlist Info", f"Found {len(items)} items with total duration {total_hours:.2f} hours!")
            virtu_log(f"Additional playlist validated with {len(items)} items", virtu_logINFO)
            interleave = get_interleave_values(dialog)
            if interleave is None:
                virtu_log("Additional playlist addition skipped: interleave input canceled", virtu_logINFO)
                continue
            shuffle_shows = False
            fixed_show_order = None
            if playlist['type'] == 'episodes':
                randomization_options = ["No randomization", "Shuffle TV shows each cycle", "Set fixed random TV show order"]
                selected_random = dialog.select("TV Show Order Randomization", randomization_options)
                if selected_random == -1:
                    selected_random = 0  # Default to no
                if selected_random == 1:
                    shuffle_shows = True
                elif selected_random == 2:
                    unique_shows = sorted(set(item.get('showtitle', '') for item in items if item.get('showtitle')))
                    if not unique_shows:
                        dialog.ok("Error", "No TV shows found in the playlist.")
                        virtu_log(f"No TV shows found in playlist {playlist['path']}", virtu_logERROR)
                        continue
                    current_order = unique_shows[:]
                    while True:
                        order_text = "\n".join(f"{i+1}. {show}" for i, show in enumerate(current_order))
                        dialog.textviewer("Current TV Show Order", order_text)
                        options = ["Randomize", "Save", "Cancel"]
                        sel = dialog.select("Options", options)
                        if sel == 0:  # Randomize
                            random.shuffle(current_order)
                        elif sel == 1:  # Save
                            fixed_show_order = current_order
                            break
                        elif sel == -1 or sel == 2:  # Cancel
                            break
            channel['playlists'].append({
                'path': playlist['path'],
                'type': 'additional',
                'playlist_type': playlist['type'],
                'interleave': interleave,
                'last_index': {},
                'random_order': {},
                'shuffle_shows': shuffle_shows,
                'fixed_show_order': fixed_show_order,
                'source': 'playlist'
            })
        else:  # Folder
            folder = select_folder()
            if not folder:
                virtu_log("No additional folder selected, continuing", virtu_logINFO)
                continue
            durations_file = xbmcvfs.translatePath("special://profile/addon_data/plugin.video.virtuatv/durations.json")
            durations = {}
            last_scan = 0
            if xbmcvfs.exists(durations_file):
                try:
                    with xbmcvfs.File(durations_file, 'r') as f:
                        data = json.loads(f.read())
                        durations = data.get('durations', {})
                        last_scan = data.get('last_scan', 0)
                except Exception as e:
                    virtu_log(f"Error loading durations from {durations_file}: {str(e)}", virtu_logERROR)
            video_extensions = ('.mp4', '.mkv', '.avi', '.m4v', '.ts', '.mov')
            video_files = []
            try:
                dirs, files = xbmcvfs.listdir(folder)
                files.sort()
                for file in files:
                    if file.lower().endswith(video_extensions):
                        video_files.append(os.path.join(folder, file))
                dirs.sort()
                for dir in dirs:
                    subfolder = os.path.join(folder, dir)
                    sub_files = get_video_files_from_folder(subfolder)
                    sub_files.sort()
                    video_files.extend(sub_files)
            except Exception as e:
                xbmcgui.Dialog().ok("Error", f"Failed to list files in folder {folder}: {str(e)}")
                virtu_log(f"Error listing files in folder {folder}: {str(e)}", virtu_logERROR)
                continue
            if not video_files:
                xbmcgui.Dialog().ok("Error", f"No video files found in folder {folder}.")
                virtu_log(f"No video files found in folder {folder}", virtu_logERROR)
                continue
            missing_durations = [f for f in video_files if f not in durations or durations[f] <= 0]
            if not missing_durations:
                total_duration = sum(durations[f] for f in video_files if f in durations)
                total_hours = total_duration / 3600
                xbmcgui.Dialog().ok("Folder Info", f"Found {len(video_files)} items with total duration {total_hours:.2f} hours (using cached durations).")
                virtu_log(f"Folder {folder} has {len(video_files)} items with cached durations, total duration {total_hours:.2f} hours", virtu_logINFO)
            else:
                scan_now = dialog.yesno("Scan Folder", f"Durations missing for {len(missing_durations)}/{len(video_files)} files in folder. Scan now to retrieve durations? (Required for channel creation.)")
                if not scan_now:
                    xbmcgui.Dialog().ok("Error", f"Cannot add folder: {len(missing_durations)} files lack valid durations. Please scan durations or select a different folder.")
                    virtu_log(f"Folder addition aborted: {len(missing_durations)} files in {folder} lack durations", virtu_logINFO)
                    continue
                durations = scan_folder_durations(folder, force_scan=True)
                if not durations:
                    xbmcgui.Dialog().ok("Error", f"No valid video files with durations found in folder {folder}.")
                    virtu_log(f"No valid durations found for folder {folder}", virtu_logERROR)
                    continue
                total_duration = sum(durations.values())
                total_hours = total_duration / 3600
                xbmcgui.Dialog().ok("Folder Info", f"Scanned {len(durations)} items with total duration {total_hours:.2f} hours!")
                virtu_log(f"Scanned {len(durations)} items for folder {folder}, total duration {total_hours:.2f} hours", virtu_logINFO)
            items, rule_order, is_random = get_playlist_items_with_durations(folder, 'episodes', source='folder')
            if not items:
                xbmcgui.Dialog().ok("Error", f"No valid items found in folder {folder}. Ensure it contains valid video files with durations.")
                virtu_log(f"No valid items found in folder {folder}", virtu_logINFO)
                continue
            interleave = get_interleave_values(dialog)
            if interleave is None:
                virtu_log("Additional folder addition skipped: interleave input canceled", virtu_logINFO)
                continue
            shuffle_shows = False
            fixed_show_order = None
            randomization_options = ["No randomization", "Shuffle items each cycle", "Set fixed random item order"]
            selected_random = dialog.select("Item Order Randomization", randomization_options)
            if selected_random == -1:
                selected_random = 0  # Default to no
            if selected_random == 1:
                shuffle_shows = True
            elif selected_random == 2:
                unique_items = sorted(set(item['title'] for item in items))
                if not unique_items:
                    dialog.ok("Error", "No items found in the folder.")
                    virtu_log(f"No items found in folder {folder}", virtu_logERROR)
                    continue
                current_order = unique_items[:]
                while True:
                    order_text = "\n".join(f"{i+1}. {item}" for i, item in enumerate(current_order))
                    dialog.textviewer("Current Item Order", order_text)
                    options = ["Randomize", "Save", "Cancel"]
                    sel = dialog.select("Options", options)
                    if sel == 0:  # Randomize
                        random.shuffle(current_order)
                    elif sel == 1:  # Save
                        fixed_show_order = current_order
                        break
                    elif sel == -1 or sel == 2:  # Cancel
                        break
            channel['playlists'].append({
                'path': folder,
                'type': 'additional',
                'playlist_type': 'episodes',
                'interleave': interleave,
                'last_index': {},
                'random_order': {},
                'shuffle_shows': shuffle_shows,
                'fixed_show_order': fixed_show_order,
                'source': 'folder'
            })
    if channel['playlists']:
        channels = load_channels()
        if LOG_VERBOSE:
            virtu_log(f"Reloaded {len(channels)} channels before appending new channel", virtu_logDEBUG)
        channels.append(channel)
        if LOG_INFO:
            virtu_log(f"Appended new channel, total channels: {len(channels)}", virtu_logINFO)
        dialog.notification("VirtuaTV", f"Finalizing channel {channel_name} creation...", xbmcgui.NOTIFICATION_INFO, 2000)
        if not save_channels(channels):
            xbmcgui.Dialog().ok("Error", f"Failed to finalize channel {channel_name}!")
            virtu_log("Failed to save channels during finalization", virtu_logERROR)
            return
        if not generate_channel_files(channel_number):
            xbmcgui.Dialog().ok("Warning", f"Channel {channel_name} created, but file generation failed!")
            virtu_log("Channel created but file generation failed", virtu_logWARNING)
            return
        if not sync_files():
            xbmcgui.Dialog().ok("Warning", f"Channel {channel_name} created, but failed to sync files!")
            virtu_log("Channel created but file sync failed", virtu_logWARNING)
        storage_path = debug_settings()  # Get validated storage path
        safe_channel_name = ''.join(c for c in channel_name if c.isalnum() or c in (' ', '_', '-')).replace(' ', '_')
        m3u_path = os.path.join(storage_path, f'VirtuaTV_Channel_{channel_number}_{safe_channel_name}.m3u')
        xbmcgui.Dialog().ok("Success", f"Channel {channel_name} (Number {channel_number}) created successfully with {len(channel['playlists'])} sources at {m3u_path}!")
        virtu_log(f"Channel {channel_name} created with number {channel_number} and {len(channel['playlists'])} sources", virtu_logINFO)
    else:
        xbmcgui.Dialog().ok("Error", f"No sources added to channel {channel_name}!")
        virtu_log("Channel creation aborted: no sources added", virtu_logINFO)

def delete_channel():
    dialog = xbmcgui.Dialog()
    channels = load_channels()
    if not channels:
        xbmcgui.Dialog().ok("Error", "No channels exist to delete!")
        virtu_log("VirtuaTV: Delete channel aborted: no channels exist", virtu_logINFO)
        return
    channel_names = [f"{ch['name']} ({ch['number']})" for ch in channels]
    channel_nums = [ch['number'] for ch in channels]
    selected = dialog.select("Select Channel to Delete", channel_names)
    if selected == -1:
        virtu_log("VirtuaTV: Delete channel aborted: no channel selected", virtu_logINFO)
        return
    channel_number = channel_nums[selected]
    channel_name = channels[selected]['name']
    if not dialog.yesno("Confirm Delete", f"Are you sure you want to delete channel {channel_names[selected]}?"):
        virtu_log("VirtuaTV: Delete channel aborted: user canceled", virtu_logINFO)
        return
    storage_path = debug_settings()  # Get validated storage path
    safe_channel_name = ''.join(c for c in channel_name if c.isalnum() or c in (' ', '_', '-')).replace(' ', '_')
    m3u_filename = os.path.join(storage_path, f'VirtuaTV_Channel_{channel_number}_{safe_channel_name}.m3u')
    channels.pop(selected)
    if not save_channels(channels):
        xbmcgui.Dialog().ok("Error", f"Failed to delete channel {channel_name} ({channel_number})!")
        virtu_log("VirtuaTV: Failed to save channels after deletion", virtu_logERROR)
        return
    if not sync_files():
        xbmcgui.Dialog().ok("Warning", f"Channel {channel_name} deleted, but failed to sync files!")
        virtu_log("VirtuaTV: Channel deleted but file sync failed", virtu_logWARNING)
    try:
        if xbmcvfs.exists(m3u_filename):
            xbmcvfs.delete(m3u_filename)
            virtu_log(f"VirtuaTV: Deleted M3U file {m3u_filename} for channel {channel_name} ({channel_number})", virtu_logINFO)
        xbmcgui.Dialog().ok("Success", f"Channel {channel_name} ({channel_number}) deleted!")
        virtu_log(f"VirtuaTV: Channel {channel_number} deleted successfully", virtu_logINFO)
    except Exception as e:
        xbmcgui.Dialog().ok("Warning", f"Channel {channel_name} ({channel_number}) deleted, but failed to delete files: {str(e)}")
        virtu_log(f"VirtuaTV: Error deleting files for {channel_number}: {str(e)}", virtu_logERROR)

def delete_all_channels():
    dialog = xbmcgui.Dialog()
    if not dialog.yesno("Confirm Delete All", "Are you sure you want to delete all channels?"):
        virtu_log("VirtuaTV: Delete all channels aborted: user canceled", virtu_logINFO)
        return
    storage_path = debug_settings()  # Get validated storage path
    channels = load_channels()
    for channel in channels:
        channel_name = channel['name']
        channel_number = channel['number']
        safe_channel_name = ''.join(c for c in channel_name if c.isalnum() or c in (' ', '_', '-')).replace(' ', '_')
        m3u_filename = os.path.join(storage_path, f'VirtuaTV_Channel_{channel_number}_{safe_channel_name}.m3u')
        try:
            if xbmcvfs.exists(m3u_filename):
                xbmcvfs.delete(m3u_filename)
                virtu_log(f"VirtuaTV: Deleted M3U file {m3u_filename} for channel {channel_name} ({channel_number})", virtu_logINFO)
        except Exception as e:
            virtu_log(f"VirtuaTV: Error deleting files for channel {channel_number}: {str(e)}", virtu_logERROR)
    channels = []
    if save_channels(channels):
        if sync_files():
            dialog.ok("Success", "All channels deleted!")
            virtu_log("VirtuaTV: All channels deleted successfully", virtu_logINFO)
        else:
            dialog.ok("Warning", "All channels deleted, but failed to sync files!")
            virtu_log("VirtuaTV: All channels deleted but file sync failed", virtu_logWARNING)
    else:
        dialog.ok("Error", "Failed to delete all channels!")
        virtu_log("VirtuaTV: Failed to save empty channels.json after deletion", virtu_logERROR)

def edit_channel():
    dialog = xbmcgui.Dialog()
    channels = load_channels()
    if not channels:
        dialog.ok("Info", "No channels available to edit.")
        virtu_log("No channels to edit", virtu_logINFO)
        return
    channel_names = [f"{ch['name']} ({ch['number']})" for ch in channels]
    selected_channel = dialog.select("Select Channel to Edit", channel_names)
    if selected_channel == -1:
        virtu_log("Edit channel aborted: no channel selected", virtu_logINFO)
        return
    channel = channels[selected_channel]
    actions = ["Add Playlist/Folder to Channel", "Delete Playlist/Folder from Channel"]
    selected_action = dialog.select("Select Action", actions)
    if selected_action == -1:
        virtu_log("Edit channel aborted: no action selected", virtu_logINFO)
        return
    if selected_action == 0: # Add Playlist/Folder
        source_type = dialog.select("Select Source Type", ["Smart Playlist", "Folder"])
        if source_type == -1:
            virtu_log("Add source aborted: no type selected", virtu_logINFO)
            return
        if source_type == 0: # Smart Playlist
            playlist = select_playlist()
            if not playlist:
                virtu_log("Add playlist aborted: no playlist selected", virtu_logINFO)
                return
            progress_dialog = xbmcgui.DialogProgressBG()
            progress_dialog.create("VirtuaTV", f"Validating playlist {os.path.basename(playlist['path'])}...")
            virtu_log(f"Validating playlist {playlist['path']} for channel {channel['name']} ({channel['number']})", virtu_logDEBUG)
            items, rule_order, is_random = get_playlist_items_with_durations(playlist['path'], playlist['type'], source='playlist')
            if not items:
                dialog.ok("Error", f"No valid items found in playlist {playlist['path']}. Ensure it contains valid {playlist['type']} and is indexed in the Kodi database. Check missing_durations.log for details.")
                virtu_log(f"No valid items found in playlist {playlist['path']}", virtu_logERROR)
                progress_dialog.close()
                return
            total_duration = sum(item['duration'] for item in items)
            total_hours = total_duration / 3600
            unique_shows = sorted(set(item.get('showtitle', '') for item in items if item.get('showtitle')))
            virtu_log(f"Playlist {playlist['path']} validated: {len(items)} items, total duration {total_hours:.2f} hours, shows: {unique_shows}, rule_order: {rule_order}, is_random: {is_random}", virtu_logINFO)
            progress_dialog.update(100, "VirtuaTV", f"Found {len(items)} items, total duration {total_hours:.2f} hours")
            xbmc.sleep(1000)
            progress_dialog.close()
            dialog.ok("Playlist Info", f"Found {len(items)} items with total duration {total_hours:.2f} hours, shows: {', '.join(unique_shows) if unique_shows else 'None'}")
            interleave = get_interleave_values(dialog)
            if interleave is None:
                virtu_log("Add playlist aborted: interleave input canceled", virtu_logINFO)
                progress_dialog.close()
                return
            shuffle_shows = False
            fixed_show_order = None
            if playlist['type'] == 'episodes':
                randomization_options = ["No randomization", "Shuffle TV shows each cycle", "Set fixed random TV show order"]
                selected_random = dialog.select("TV Show Order Randomization", randomization_options)
                if selected_random == -1:
                    selected_random = 0 # Default to no
                if selected_random == 1:
                    shuffle_shows = True
                elif selected_random == 2:
                    if not unique_shows:
                        dialog.ok("Error", "No TV shows found in the playlist.")
                        virtu_log(f"No TV shows found in playlist {playlist['path']}", virtu_logERROR)
                        return
                    current_order = unique_shows[:]
                    while True:
                        order_text = "\n".join(f"{i+1}. {show}" for i, show in enumerate(current_order))
                        dialog.textviewer("Current TV Show Order", order_text)
                        options = ["Randomize", "Save", "Cancel"]
                        sel = dialog.select("Options", options)
                        if sel == 0: # Randomize
                            random.shuffle(current_order)
                        elif sel == 1: # Save
                            fixed_show_order = current_order
                            break
                        elif sel == -1 or sel == 2: # Cancel
                            break
            new_playlist = {
                'path': playlist['path'],
                'type': 'additional',
                'playlist_type': playlist['type'],
                'interleave': interleave,
                'last_index': {show: -1 for show in unique_shows},
                'random_order': {},
                'shuffle_shows': shuffle_shows,
                'fixed_show_order': fixed_show_order,
                'source': 'playlist'
            }
            channel['playlists'].append(new_playlist)
            channel['is_new'] = True # Flag for full regeneration
            virtu_log(f"Added playlist {playlist['path']} to channel {channel['name']} ({channel['number']}): {json.dumps(new_playlist, indent=2)}", virtu_logINFO)
        else: # Folder
            folder = select_folder()
            if not folder:
                virtu_log("Add folder aborted: no folder selected", virtu_logINFO)
                return
            virtu_log(f"Validating folder {folder} for channel {channel['name']} ({channel['number']})", virtu_logDEBUG)
            durations_file = xbmcvfs.translatePath("special://profile/addon_data/plugin.video.virtuatv/durations.json")
            durations = {}
            last_scan = 0
            if xbmcvfs.exists(durations_file):
                try:
                    with xbmcvfs.File(durations_file, 'r') as f:
                        data = json.loads(f.read())
                        durations = data.get('durations', {})
                        last_scan = data.get('last_scan', 0)
                except Exception as e:
                    virtu_log(f"Error loading durations from {durations_file}: {str(e)}", virtu_logERROR)
            video_extensions = ('.mp4', '.mkv', '.avi', '.m4v', '.ts', '.mov')
            video_files = []
            try:
                dirs, files = xbmcvfs.listdir(folder)
                files.sort()
                for file in files:
                    if file.lower().endswith(video_extensions):
                        video_files.append(os.path.join(folder, file))
                dirs.sort()
                for dir in dirs:
                    subfolder = os.path.join(folder, dir)
                    sub_files = get_video_files_from_folder(subfolder)
                    sub_files.sort()
                    video_files.extend(sub_files)
            except Exception as e:
                xbmcgui.Dialog().ok("Error", f"Failed to list files in folder {folder}: {str(e)}")
                virtu_log(f"Error listing files in folder {folder}: {str(e)}", virtu_logERROR)
                return
            if not video_files:
                xbmcgui.Dialog().ok("Error", f"No video files found in folder {folder}.")
                virtu_log(f"No video files found in folder {folder}", virtu_logERROR)
                return
            missing_durations = [f for f in video_files if f not in durations or durations[f] <= 0]
            if not missing_durations:
                total_duration = sum(durations[f] for f in video_files if f in durations)
                total_hours = total_duration / 3600
                xbmcgui.Dialog().ok("Folder Info", f"Found {len(video_files)} items with total duration {total_hours:.2f} hours (using cached durations).")
                virtu_log(f"Folder {folder} has {len(video_files)} items with cached durations, total duration {total_hours:.2f} hours", virtu_logINFO)
            else:
                scan_now = dialog.yesno("Scan Folder", f"Durations missing for {len(missing_durations)}/{len(video_files)} files in folder. Scan now to retrieve durations? (Required for channel creation.)")
                if not scan_now:
                    xbmcgui.Dialog().ok("Error", f"Cannot add folder: {len(missing_durations)} files lack valid durations. Please scan durations or select a different folder.")
                    virtu_log(f"Folder addition aborted: {len(missing_durations)} files in {folder} lack durations", virtu_logINFO)
                    return
                durations = scan_folder_durations(folder, force_scan=True)
                if not durations:
                    xbmcgui.Dialog().ok("Error", f"No valid video files with durations found in folder {folder}.")
                    virtu_log(f"No valid durations found for folder {folder}", virtu_logERROR)
                    return
                total_duration = sum(durations.values())
                total_hours = total_duration / 3600
                xbmcgui.Dialog().ok("Folder Info", f"Scanned {len(durations)} items with total duration {total_hours:.2f} hours!")
                virtu_log(f"Scanned {len(durations)} items for folder {folder}, total duration {total_hours:.2f} hours", virtu_logINFO)
            items, rule_order, is_random = get_playlist_items_with_durations(folder, 'episodes', source='folder')
            if not items:
                xbmcgui.Dialog().ok("Error", f"No valid items found in folder {folder}. Ensure it contains valid video files with durations.")
                virtu_log(f"No valid items found in folder {folder}", virtu_logINFO)
                return
            interleave = get_interleave_values(dialog)
            if interleave is None:
                virtu_log("Add folder aborted: interleave input canceled", virtu_logINFO)
                return
            shuffle_shows = False
            fixed_show_order = None
            randomization_options = ["No randomization", "Shuffle items each cycle", "Set fixed random item order"]
            selected_random = dialog.select("Item Order Randomization", randomization_options)
            if selected_random == -1:
                selected_random = 0 # Default to no
            if selected_random == 1:
                shuffle_shows = True
            elif selected_random == 2:
                unique_items = sorted(set(item['title'] for item in items))
                if not unique_items:
                    dialog.ok("Error", "No items found in the folder.")
                    virtu_log(f"No items found in folder {folder}", virtu_logERROR)
                    return
                current_order = unique_items[:]
                while True:
                    order_text = "\n".join(f"{i+1}. {item}" for i, item in enumerate(current_order))
                    dialog.textviewer("Current Item Order", order_text)
                    options = ["Randomize", "Save", "Cancel"]
                    sel = dialog.select("Options", options)
                    if sel == 0: # Randomize
                        random.shuffle(current_order)
                    elif sel == 1: # Save
                        fixed_show_order = current_order
                        break
                    elif sel == -1 or sel == 2: # Cancel
                        break
            new_playlist = {
                'path': folder,
                'type': 'additional',
                'playlist_type': 'episodes',
                'interleave': interleave,
                'last_index': {},
                'random_order': {},
                'shuffle_shows': shuffle_shows,
                'fixed_show_order': fixed_show_order,
                'source': 'folder'
            }
            channel['playlists'].append(new_playlist)
            channel['is_new'] = True # Flag for full regeneration
            virtu_log(f"Added folder {folder} to channel {channel['name']} ({channel['number']}): {json.dumps(new_playlist, indent=2)}", virtu_logINFO)
        if save_channels(channels):
            if sync_files():
                dialog.ok("Success", f"Source added to channel {channel['name']}. Regenerate channels to apply changes.")
                virtu_log(f"Source added to channel {channel['number']}, saved channels.json", virtu_logINFO)
            else:
                dialog.ok("Warning", f"Source added to channel {channel['name']}, but failed to sync files!")
                virtu_log(f"Source added to channel {channel['number']}, but file sync failed", virtu_logWARNING)
        else:
            dialog.ok("Error", f"Failed to add source to channel {channel['name']}!")
            virtu_log(f"Failed to save channels after adding source to channel {channel['number']}", virtu_logERROR)
    elif selected_action == 1: # Delete Playlist/Folder
        if not channel['playlists']:
            dialog.ok("Info", "No sources in this channel to delete.")
            virtu_log(f"No sources to delete in channel {channel['number']}", virtu_logINFO)
            return
        playlist_names = [f"{os.path.basename(pl['path'])} ({'base' if pl['type'] == 'base' else 'additional'}, {'playlist' if pl['source'] == 'playlist' else 'folder'})" for pl in channel['playlists']]
        selected_playlist = dialog.select("Select Source to Delete", playlist_names)
        if selected_playlist == -1:
            virtu_log("Delete source aborted: no source selected", virtu_logINFO)
            return
        deleted_path = channel['playlists'][selected_playlist]['path']
        del channel['playlists'][selected_playlist]
        channel['is_new'] = True # Flag for full regeneration
        if save_channels(channels):
            if sync_files():
                dialog.ok("Success", f"Source {deleted_path} deleted from channel {channel['name']}. Regenerate channels to apply changes.")
                virtu_log(f"Source {deleted_path} deleted from channel {channel['number']}", virtu_logINFO)
            else:
                dialog.ok("Warning", f"Source {deleted_path} deleted from channel {channel['name']}, but failed to sync files!")
                virtu_log(f"Source {deleted_path} deleted from channel {channel['number']}, but file sync failed", virtu_logWARNING)
        else:
            dialog.ok("Error", f"Failed to delete source from channel {channel['name']}!")
            virtu_log(f"Failed to save channels after deleting source from channel {channel['number']}", virtu_logERROR)
            
def clear_shared_folder():
    """
    Clears the 'shared_folder' setting and moves VirtuaTV-related files to addon_data folder.
    """
    global ADDON
    dialog = xbmcgui.Dialog()
    with SHARED_FOLDER_LOCK:
        # Stop playback to release file locks
        if xbmc.Player().isPlaying():
            xbmc.executebuiltin('PlayerControl(Stop)')
            xbmc.sleep(1000) # Wait for files to be released
        # Check if a shared folder is currently set
        current_value = ADDON.getSetting('shared_folder')
        if current_value == '':
            virtu_log("Clear shared folder aborted: setting already clear.", virtu_logINFO)
            dialog.notification("VirtuaTV", "The shared folder is already clear.", xbmcgui.NOTIFICATION_INFO, 3000)
            return
        # Confirm with the user before proceeding
        if not dialog.yesno("Confirm Clear", "Clear the shared folder setting and move VirtuaTV files to addon_data folder?"):
            virtu_log("Clear shared folder aborted: user canceled", virtu_logINFO)
            return
        # Get the current shared folder path
        shared_folder_norm = xbmcvfs.translatePath(current_value).replace('\\', '/')
        # Check for VirtuaTV files
        files_to_process = []
        if xbmcvfs.exists(shared_folder_norm):
            try:
                dirs, files = xbmcvfs.listdir(shared_folder_norm)
                files_to_process = [f for f in files if (f.lower().startswith("virtuatv") or f.lower() in ["channels.json", "virtuatv_settings.lock"]) and f.lower().endswith((".m3u", ".xml", ".xmltv", ".json", ".lock"))]
            except Exception as e:
                virtu_log(f"Error accessing shared folder {shared_folder_norm}: {str(e)}", virtu_logERROR)
                dialog.notification("VirtuaTV", f"Error accessing shared folder: {str(e)}", xbmcgui.NOTIFICATION_ERROR, 3000)
                return
        # Ensure addon_data folder exists
        if not xbmcvfs.exists(SETTINGS_DIR):
            if not xbmcvfs.mkdirs(SETTINGS_DIR):
                virtu_log(f"Failed to create addon_data folder: {SETTINGS_DIR}", virtu_logERROR)
                dialog.notification("VirtuaTV", f"Failed to create addon_data folder: {SETTINGS_DIR}", xbmcgui.NOTIFICATION_ERROR, 3000)
                return
        # Move files
        max_retries = 3
        moved_files = 0
        for file_name in files_to_process:
            src_path = os.path.join(shared_folder_norm, file_name)
            dest_path = os.path.join(SETTINGS_DIR, file_name)
            for attempt in range(max_retries):
                try:
                    if xbmcvfs.copy(src_path, dest_path):
                        if xbmcvfs.delete(src_path):
                            virtu_log(f"Moved file: {src_path} to {dest_path}", virtu_logINFO)
                            moved_files += 1
                            break
                        else:
                            virtu_log(f"Failed to delete source file after copy: {src_path}", virtu_logERROR)
                            dialog.notification("VirtuaTV", f"Failed to delete source file: {file_name}", xbmcgui.NOTIFICATION_ERROR, 3000)
                    else:
                        virtu_log(f"Failed to copy file: {src_path} to {dest_path} (attempt {attempt + 1})", virtu_logWARNING)
                        if attempt < max_retries - 1:
                            xbmc.sleep(1000)
                except Exception as e:
                    virtu_log(f"Error moving file {src_path} (attempt {attempt + 1}): {str(e)}", virtu_logERROR)
                    if attempt < max_retries - 1:
                        xbmc.sleep(1000)
            else:
                virtu_log(f"Failed to move file after {max_retries} attempts: {src_path}", virtu_logERROR)
                dialog.notification("VirtuaTV", f"Failed to move file: {file_name}", xbmcgui.NOTIFICATION_ERROR, 3000)
        if files_to_process:
            if moved_files > 0:
                virtu_log(f"Moved {moved_files} files from shared folder to addon_data folder: {SETTINGS_DIR}", virtu_logINFO)
                dialog.notification("VirtuaTV", f"Moved {moved_files} files to addon_data folder.", xbmcgui.NOTIFICATION_INFO, 3000)
            else:
                virtu_log(f"No files successfully moved from shared folder: {shared_folder_norm}", virtu_logWARNING)
                dialog.notification("VirtuaTV", "No files successfully moved from shared folder.", xbmcgui.NOTIFICATION_WARNING, 3000)
        else:
            virtu_log(f"No VirtuaTV files found in shared folder: {shared_folder_norm}", virtu_logINFO)
            dialog.notification("VirtuaTV", "No VirtuaTV files found in shared folder.", xbmcgui.NOTIFICATION_INFO, 3000)
        # Clear the shared_folder setting by removing the element from settings.xml
        settings_xml = os.path.join(SETTINGS_DIR, 'settings.xml')
        xml_updated = False
        max_xml_retries = 3
        for attempt in range(max_xml_retries):
            try:
                if xbmcvfs.exists(settings_xml):
                    with xbmcvfs.File(settings_xml, 'r') as f:
                        xml_content = f.read()
                    virtu_log(f"settings.xml before update: {xml_content}", virtu_logDEBUG)
                    tree = ET.parse(settings_xml)
                    root = tree.getroot()
                    for setting in root.findall('setting'):
                        if setting.get('id') == 'shared_folder':
                            root.remove(setting)
                            break
                    tree.write(settings_xml, encoding='utf-8', xml_declaration=True)
                    with xbmcvfs.File(settings_xml, 'r') as f:
                        xml_content_after = f.read()
                    virtu_log(f"settings.xml after update: {xml_content_after}", virtu_logDEBUG)
                    virtu_log("Successfully removed shared_folder from settings.xml", virtu_logINFO)
                xml_updated = True
                break
            except Exception as e:
                virtu_log(f"Error updating settings.xml (attempt {attempt + 1}): {str(e)}", virtu_logERROR)
                if attempt < max_xml_retries - 1:
                    xbmc.sleep(2000)
        if not xml_updated:
            virtu_log("Failed to update settings.xml after retries", virtu_logERROR)
            dialog.notification("VirtuaTV", "Failed to update settings.xml to clear shared folder", xbmcgui.NOTIFICATION_ERROR, 5000)
            return
        # Poll to confirm in-memory setting update
        xbmc.sleep(2000)
        poll_max = 10
        poll_count = 0
        while poll_count < poll_max:
            ADDON = xbmcaddon.Addon(ADDON_ID)
            settings_monitor.onSettingsChanged()
            current_setting = ADDON.getSetting('shared_folder')
            virtu_log(f"Poll {poll_count}: shared_folder = {current_setting}", virtu_logINFO)
            if current_setting == '':
                break
            poll_count += 1
            xbmc.sleep(1000)
        if ADDON.getSetting('shared_folder') != '':
            virtu_log(f"Warning: In-memory setting not updated after poll, but XML is cleared. Restart required.", virtu_logWARNING)
        global shared_folder, shared_folder_notified
        shared_folder = ''
        shared_folder_notified = False
        virtu_log(f"Shared folder setting cleared, global shared_folder: {shared_folder}, using addon_data folder", virtu_logINFO)
        dialog.notification("VirtuaTV", "Shared folder cleared. Using addon_data folder. Please restart Kodi for settings UI to update.", xbmcgui.NOTIFICATION_INFO, 5000)
        # Trigger sync to ensure files are recreated in addon_data
        sync_files()
    # Return to main menu
    main_menu()
    
def rescan_durations():
    """Manually rescan durations for all folder-based playlists or a selected folder."""
    dialog = xbmcgui.Dialog()
    channels = load_channels()
    folder_playlists = []
    for channel in channels:
        for playlist in channel.get('playlists', []):
            if playlist.get('source') == 'folder':
                folder_playlists.append({
                    'folder': playlist['path'],
                    'channel_name': channel['name'],
                    'channel_number': channel['number']
                })
    if not folder_playlists:
        dialog.ok("Info", "No folder-based playlists found in channels. Nothing to rescan.")
        virtu_log("No folder-based playlists found for rescan", virtu_logINFO)
        return
    # Ask if rescan all or select one
    options = ["Rescan all folder playlists", "Select a single folder"]
    selected = dialog.select("Rescan Durations", options)
    if selected == -1:
        virtu_log("Rescan aborted: no option selected", virtu_logINFO)
        return
    if selected == 0: # Rescan all
        scanned_folders = 0
        total_scanned_files = 0
        for folder_info in folder_playlists:
            folder = folder_info['folder']
            durations = scan_folder_durations(folder, force_scan=True)
            if durations:
                scanned_files = len([d for d in durations if durations[d] > 0])
                total_scanned_files += scanned_files
                scanned_folders += 1
                virtu_log(f"Rescanned {scanned_files} files in {folder} for channel {folder_info['channel_name']} ({folder_info['channel_number']})", virtu_logINFO)
            else:
                virtu_log(f"No durations found when rescanning {folder}", virtu_logWARNING)
        dialog.ok("Rescan Complete", f"Rescanned {scanned_folders} folders with {total_scanned_files} files total.")
    else: # Select a single folder
        folder = select_folder()
        if not folder:
            virtu_log("Rescan aborted: no folder selected", virtu_logINFO)
            return
        durations = scan_folder_durations(folder, force_scan=True)
        if durations:
            scanned_files = len([d for d in durations if durations[d] > 0])
            dialog.ok("Rescan Complete", f"Rescanned {scanned_files} files in {folder}.")
            virtu_log(f"Rescanned {scanned_files} files in {folder}", virtu_logINFO)
        else:
            dialog.ok("Rescan Failed", f"No valid durations found in {folder}.")
            virtu_log(f"No durations found when rescanning {folder}", virtu_logERROR)

def backup_addon():
    dialog = xbmcgui.Dialog()
    backup_path = dialog.browse(3, "Select Backup Folder", 'files')
    if not backup_path:
        virtu_log("VirtuaTV: Backup aborted: no folder selected", virtu_logINFO)
        return
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    backup_dir = os.path.join(backup_path, f'virtuatv_backup_{timestamp}')
    if not xbmcvfs.mkdirs(backup_dir):
        dialog.ok("Error", "Failed to create backup directory!")
        virtu_log(f"VirtuaTV: Failed to create backup directory {backup_dir}", virtu_logERROR)
        return
    settings_xml = os.path.join(SETTINGS_DIR, 'settings.xml')
    backup_channels = os.path.join(backup_dir, 'channels.json')
    backup_settings = os.path.join(backup_dir, 'settings.xml')
    success = True
    if xbmcvfs.exists(CHANNELS_FILE):
        if not xbmcvfs.copy(CHANNELS_FILE, backup_channels):
            success = False
            virtu_log(f"VirtuaTV: Failed to copy channels.json to {backup_channels}", virtu_logERROR)
    if xbmcvfs.exists(settings_xml):
        if not xbmcvfs.copy(settings_xml, backup_settings):
            success = False
            virtu_log(f"VirtuaTV: Failed to copy settings.xml to {backup_settings}", virtu_logERROR)
    if success:
        dialog.ok("Success", f"Backup created at {backup_dir}")
        virtu_log(f"VirtuaTV: Backup created at {backup_dir}", virtu_logINFO)
    else:
        dialog.ok("Error", "Failed to create complete backup!")

def restore_addon():
    dialog = xbmcgui.Dialog()
    restore_dir = dialog.browse(3, "Select Backup Folder", 'files')
    if not restore_dir:
        virtu_log("VirtuaTV: Restore aborted: no folder selected", virtu_logINFO)
        return
    restore_channels = os.path.join(restore_dir, 'channels.json')
    restore_settings = os.path.join(restore_dir, 'settings.xml')
    if not xbmcvfs.exists(restore_channels) or not xbmcvfs.exists(restore_settings):
        dialog.ok("Error", "Backup files not found in selected folder!")
        virtu_log("VirtuaTV: Restore aborted: missing backup files", virtu_logERROR)
        return
    if not dialog.yesno("Confirm Restore", "This will overwrite current channels and settings. Continue?"):
        virtu_log("VirtuaTV: Restore aborted: user canceled", virtu_logINFO)
        return
    settings_xml = os.path.join(SETTINGS_DIR, 'settings.xml')
    success = True
    if not xbmcvfs.copy(restore_channels, CHANNELS_FILE):
        success = False
        virtu_log(f"VirtuaTV: Failed to restore channels.json from {restore_channels}", virtu_logERROR)
    if not xbmcvfs.copy(restore_settings, settings_xml):
        success = False
        virtu_log(f"VirtuaTV: Failed to restore settings.xml from {restore_settings}", virtu_logERROR)
    if success:
        dialog.ok("Success", "Addon restored! Please restart Kodi for settings to take effect.")
        virtu_log("VirtuaTV: Addon restored successfully", virtu_logINFO)
    else:
        dialog.ok("Error", "Failed to restore addon!")

def compact_last_index(json_str):
    lines = json_str.splitlines()
    output = []
    i = 0
    in_last_index = False
    in_list = False
    list_buffer = []
    key_line = None
    has_comma = False
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if '"last_index": {' in stripped:
            in_last_index = True
        if in_last_index and '}' in stripped:
            in_last_index = False
        if in_list:
            if stripped.startswith(']'):
                compact_list = '[' + ','.join(list_buffer) + ']'
                if stripped.endswith(','):
                    compact_list += ','
                    has_comma = True
                else:
                    has_comma = False
                output.append(key_line.replace('[', compact_list))
                in_list = False
                list_buffer = []
                key_line = None
                i += 1
                continue
            else:
                num = stripped.rstrip(',')
                list_buffer.append(num)
            i += 1
            continue
        else:
            if in_last_index and stripped.endswith('": ['):
                in_list = True
                key_line = line
                i += 1
                continue
            output.append(line)
        i += 1
    return '\n'.join(output)

# Replace your existing sync_files function

#import xml.dom.minidom  # Added for pretty-printing XMLTV
def sync_files():
    """Synchronize channels.json, combine M3U files, and generate XMLTV for IPTV Simple Client."""
    global shared_folder, shared_folder_notified
    storage_path = debug_settings()  # Get validated storage path
    files_changed = False
    significant_change = False
    try:
        shared_channels = os.path.join(storage_path, 'channels.json')
        local_channels = CHANNELS_FILE
        # Sync channels.json (bidirectional based on timestamp)
        if xbmcvfs.exists(local_channels):
            stat_local = xbmcvfs.Stat(local_channels)
            mtime_local = stat_local.st_mtime()
            mtime_shared = xbmcvfs.Stat(shared_channels).st_mtime() if xbmcvfs.exists(shared_channels) else 0
            with xbmcvfs.File(local_channels, 'r') as f:
                local_content = f.read()
            shared_content = ''
            if xbmcvfs.exists(shared_channels):
                with xbmcvfs.File(shared_channels, 'r') as f:
                    shared_content = f.read()
            if mtime_local > mtime_shared and local_content != shared_content:
                if xbmcvfs.copy(local_channels, shared_channels):
                    virtu_log("sync_files: Pushed channels.json to shared folder", virtu_logINFO)
                    files_changed = True
                    significant_change = True
                else:
                    virtu_log("sync_files: Failed to push channels.json", virtu_logERROR)
                    return False
            elif mtime_shared > mtime_local and local_content != shared_content:
                if xbmcvfs.copy(shared_channels, local_channels):
                    virtu_log("sync_files: Pulled channels.json from shared folder", virtu_logINFO)
                    files_changed = True
                    significant_change = True
                else:
                    virtu_log("sync_files: Failed to pull channels.json", virtu_logERROR)
                    return False
        # Combine all M3U files into VirtuaTV.m3u and generate VirtuaTV.xml
        combined_m3u = os.path.join(storage_path, 'VirtuaTV.m3u')
        combined_xmltv = os.path.join(storage_path, 'VirtuaTV.xml')
        m3u_content = ['#EXTM3U']
        xmltv_root = ET.Element('tv', generator_info_name='VirtuaTV')
        existing_m3u_content = ''
        if xbmcvfs.exists(combined_m3u):
            with xbmcvfs.File(combined_m3u, 'r') as f:
                existing_m3u_content = f.read()
        new_m3u_content = []
        # Base time for XMLTV program start times (current time)
        base_time = datetime.datetime.now(datetime.timezone.utc)
        channels = load_channels()
        for channel in channels:
            channel_name = channel['name']
            channel_number = channel['number']
            channel_id = channel.get('id', f"{channel_name.replace(' ', '_')}@VirtuaTV")
            safe_channel_name = ''.join(c for c in channel_name if c.isalnum() or c in (' ', '_', '-')).replace(' ', '_')
            # Add channel to XMLTV
            channel_elem = ET.SubElement(xmltv_root, 'channel', id=channel_id)
            ET.SubElement(channel_elem, 'display-name').text = f"{channel_number}. {channel_name}"
            m3u_file = os.path.join(storage_path, f'VirtuaTV_Channel_{channel_number}_{safe_channel_name}.m3u')
            if xbmcvfs.exists(m3u_file):
                with xbmcvfs.File(m3u_file, 'r') as f:
                    channel_m3u_content = f.read().splitlines()
                # Process M3U entries, adding IPTV Simple Client attributes
                i = 0
                current_time = base_time
                while i < len(channel_m3u_content):
                    line = channel_m3u_content[i]
                    if line.startswith('#EXTM3U'):
                        i += 1
                        continue
                    if line.startswith('#EXTINF:'):
                        if i + 1 < len(channel_m3u_content):
                            duration_match = re.search(r'#EXTINF:(\d+)', line)
                            if duration_match:
                                duration = int(duration_match.group(1))
                                info_parts = line.split(',', 1)[1].split('//', 2)
                                showtitle = info_parts[0] if len(info_parts) >= 1 else channel_name
                                title = info_parts[1] if len(info_parts) >= 2 else channel_name
                                description = info_parts[2] if len(info_parts) >= 3 else ''
                                # Add M3U entry with IPTV Simple Client attributes
                                new_extinf = f'#EXTINF:{duration} tvg-id="{channel_id}" tvg-name="{channel_number}. {channel_name}" tvg-chno="{channel_number}" group-title="VirtuaTV",{channel_name}'
                                new_m3u_content.append(new_extinf)
                                new_m3u_content.append(channel_m3u_content[i + 1])
                                # Add program to XMLTV without start and stop times
                                programme_elem = ET.SubElement(xmltv_root, 'programme', channel=channel_id)
                                ET.SubElement(programme_elem, 'title').text = title
                                ET.SubElement(programme_elem, 'desc').text = description
                                current_time += datetime.timedelta(seconds=duration)
                            else:
                                virtu_log(f"sync_files: Invalid #EXTINF format in {m3u_file}: {line}", virtu_logWARNING)
                            i += 2
                        else:
                            virtu_log(f"sync_files: Incomplete #EXTINF entry in {m3u_file}: {line}", virtu_logWARNING)
                            i += 1
                    else:
                        virtu_log(f"sync_files: Unexpected line in {m3u_file}: {line}", virtu_logWARNING)
                        i += 1
                virtu_log(f"sync_files: Included {m3u_file} in combined M3U and XMLTV with IPTV attributes", virtu_logDEBUG)
            else:
                virtu_log(f"sync_files: M3U file {m3u_file} not found, skipping", virtu_logWARNING)
        # Write VirtuaTV.m3u
        combined_m3u_content = '\n'.join(m3u_content + new_m3u_content) + '\n'
        if combined_m3u_content != existing_m3u_content:
            temp_m3u = os.path.join(xbmcvfs.translatePath("special://temp"), f"virtuatv_combined_m3u_{os.getpid()}.m3u")
            with xbmcvfs.File(temp_m3u, 'w') as f:
                f.write(combined_m3u_content)
            if xbmcvfs.copy(temp_m3u, combined_m3u):
                xbmcvfs.delete(temp_m3u)
                virtu_log(f"sync_files: Generated combined M3U at {combined_m3u}", virtu_logINFO)
                files_changed = True
                significant_change = True
            else:
                xbmcvfs.delete(temp_m3u)
                virtu_log(f"sync_files: Failed to generate combined M3U at {combined_m3u}", virtu_logERROR)
                return False
        # Write VirtuaTV.xml with vertical formatting
        xmltv_content = ET.tostring(xmltv_root, encoding='unicode', method='xml')
        # Pretty-print XML for vertical formatting
        parsed_xml = xml.dom.minidom.parseString(xmltv_content)
        pretty_xml = parsed_xml.toprettyxml(indent="  ", encoding="UTF-8").decode("UTF-8")
        # Remove extra blank lines
        pretty_xml = "\n".join(line for line in pretty_xml.splitlines() if line.strip())
        existing_xmltv_content = ''
        if xbmcvfs.exists(combined_xmltv):
            with xbmcvfs.File(combined_xmltv, 'r') as f:
                existing_xmltv_content = f.read()
        if pretty_xml != existing_xmltv_content:
            temp_xmltv = os.path.join(xbmcvfs.translatePath("special://temp"), f"virtuatv_xmltv_{os.getpid()}.xml")
            with xbmcvfs.File(temp_xmltv, 'w') as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE tv SYSTEM "xmltv.dtd">\n' + pretty_xml)
            if xbmcvfs.copy(temp_xmltv, combined_xmltv):
                xbmcvfs.delete(temp_xmltv)
                virtu_log(f"sync_files: Generated XMLTV at {combined_xmltv}", virtu_logINFO)
                files_changed = True
                significant_change = True
            else:
                xbmcvfs.delete(temp_xmltv)
                virtu_log(f"sync_files: Failed to generate XMLTV at {combined_xmltv}", virtu_logERROR)
                return False
        # Clean up any local M3U files in addon data folder only if shared_folder is set and different from SETTINGS_DIR
        if shared_folder and storage_path != SETTINGS_DIR:
            for channel in channels:
                channel_name = channel['name']
                channel_number = channel['number']
                safe_channel_name = ''.join(c for c in channel_name if c.isalnum() or c in (' ', '_', '-')).replace(' ', '_')
                local_m3u = os.path.join(SETTINGS_DIR, f'VirtuaTV_Channel_{channel_number}_{safe_channel_name}.m3u')
                if xbmcvfs.exists(local_m3u):
                    xbmcvfs.delete(local_m3u)
                    virtu_log(f"sync_files: Removed local M3U file {local_m3u} as shared folder is in use", virtu_logINFO)
                    files_changed = True
                    significant_change = True
        return files_changed
    except Exception as e:
        virtu_log(f"sync_files: Error during synchronization: {str(e)}", virtu_logERROR)
        xbmcgui.Dialog().notification("VirtuaTV", f"Sync error: {str(e)}", xbmcgui.NOTIFICATION_ERROR, 1000, sound=False)
        return False

def regenerate_channels():
    dialog = xbmcgui.Dialog()
    channels = load_channels()
    if not channels:
        dialog.ok("Info", "No channels available to regenerate.")
        virtu_log("No channels available to regenerate", virtu_logINFO)
        return
    dialog.notification("VirtuaTV", "Starting channel regeneration...", xbmcgui.NOTIFICATION_INFO, 1500)
    virtu_log("Starting channel regeneration", virtu_logINFO)
    time.sleep(0.1)
    total_channels = len(channels)
    successful = 0
    for idx, channel in enumerate(channels):
        channel_name = channel.get('name', 'Unknown')
        channel_number = channel.get('number', -1)
        dialog.notification("VirtuaTV", f"Regenerating channel {channel_name} ({channel_number})...", xbmcgui.NOTIFICATION_INFO, 1500)
        virtu_log(f"Regenerating channel {channel_name} ({channel_number})", virtu_logINFO)
        time.sleep(0.1)
        if generate_channel_files(channel_number):
            successful += 1
        else:
            virtu_log(f"Failed to regenerate channel {channel_name} ({channel_number})", virtu_logERROR)
            dialog.notification("VirtuaTV", f"Failed to regenerate channel {channel_name} ({channel_number})", xbmcgui.NOTIFICATION_ERROR, 3000)
            time.sleep(0.1)
    if sync_files():
        dialog.notification("VirtuaTV", f"Regeneration complete: {successful}/{total_channels} channels successful", xbmcgui.NOTIFICATION_INFO, 3000)
        virtu_log(f"Regeneration complete: {successful}/{total_channels} channels successful", virtu_logINFO)
    else:
        dialog.notification("VirtuaTV", f"Regeneration complete: {successful}/{total_channels} channels successful, but failed to sync files!", xbmcgui.NOTIFICATION_WARNING, 3000)
        virtu_log(f"Regeneration complete: {successful}/{total_channels} channels successful, but file sync failed", virtu_logWARNING)
    time.sleep(0.1)

def auto_regen_loop():
    """
    Periodically checks if channels need replenishment based on auto_regen_interval and auto_regen_threshold.
    Exits immediately if auto_regen is disabled or service_mode is Disabled.
    For Addon Service ('1'), only runs when the addon's UI is active.
    """
    global shared_folder_notified
    # Check if auto_regen and service_mode allow operation
    if not ADDON.getSettingBool('auto_regen') or ADDON.getSetting('service_mode') == '2':
        virtu_log("auto_regen_loop: Auto-regeneration or service disabled, exiting", virtu_logDEBUG)
        return
    monitor = xbmc.Monitor()
    current_pid = str(os.getpid())
    virtu_log(f"Starting auto_regen_loop with PID {current_pid}", virtu_logINFO)
    # Initialize instance ID
    init_instance_id()
    # Check for conflicting settings
    conflict = check_settings_lock()
    if conflict and conflict.get('auto_regen', False):
        virtu_log(f"auto_regen_loop: Another client {conflict['client_id']} is regenerating channels", virtu_logWARNING)
        ADDON.setSettingBool('auto_regen', False)
        return
    # Ensure single instance by checking and cleaning stale lock
    global instance_lock_file
    if xbmcvfs.exists(instance_lock_file):
        try:
            with xbmcvfs.File(instance_lock_file, 'r') as f:
                pid = f.read().strip()
            if pid and pid != current_pid:
                try:
                    if os.name == 'nt':
                        result = subprocess.run(['tasklist', '/FI', f'PID eq {pid}', '/FO', 'CSV'],
                                               capture_output=True, text=True, shell=False)
                        is_running = any(line.strip().startswith(f'"kodi.exe","{pid}"') for line in result.stdout.splitlines())
                    else:
                        is_running = os.path.exists(f'/proc/{pid}')
                    if not is_running:
                        xbmcvfs.delete(instance_lock_file)
                        virtu_log(f"Removed stale instance lock file with PID {pid}", virtu_logINFO)
                    else:
                        virtu_log(f"Instance lock exists with active PID {pid}, skipping auto_regen_loop", virtu_logWARNING)
                        return
                except Exception as e:
                    virtu_log(f"Error checking PID {pid} for instance lock: {str(e)}", virtu_logERROR)
                    xbmcvfs.delete(instance_lock_file)
                    virtu_log("Removed potentially stale instance lock due to PID check error", virtu_logWARNING)
            else:
                virtu_log("No valid PID in instance lock file or PID matches current process, proceeding", virtu_logDEBUG)
                xbmcvfs.delete(instance_lock_file)
                virtu_log("Removed empty or invalid instance lock file", virtu_logINFO)
        except Exception as e:
            virtu_log(f"Error accessing instance lock file: {str(e)}", virtu_logERROR)
            xbmcvfs.delete(instance_lock_file)
            virtu_log("Removed instance lock file due to access error", virtu_logWARNING)
    try:
        with xbmcvfs.File(instance_lock_file, 'w') as f:
            f.write(current_pid)
        virtu_log(f"Acquired instance lock with PID {current_pid}", virtu_logINFO)
    except Exception as e:
        virtu_log(f"Failed to acquire instance lock: {str(e)}", virtu_logERROR)
        return
    update_settings_lock()
    if monitor.waitForAbort(2):
        virtu_log("auto_regen_loop: Abort requested during initial delay", virtu_logINFO)
        xbmcvfs.delete(instance_lock_file)
        virtu_log("Released instance lock", virtu_logINFO)
        return
    while not monitor.abortRequested():
        if not ADDON.getSettingBool('auto_regen') or ADDON.getSetting('service_mode') == '2':
            virtu_log("auto_regen_loop: Auto-regeneration or service disabled, exiting", virtu_logDEBUG)
            break
        # Check if addon is open for Addon Service mode
        if ADDON.getSetting('service_mode') == '1' and not xbmc.getCondVisibility('Window.IsActive(plugin.video.virtuatv)'):
            virtu_log("auto_regen_loop: Addon not open in Addon Service mode, pausing", virtu_logDEBUG)
            monitor.waitForAbort(10)  # Wait and check again
            continue
        auto_regen_interval = int(ADDON.getSetting('auto_regen_interval') or 60) * 60  # Convert minutes to seconds
        virtu_log(f"auto_regen_loop: Checking channels for replenishment, interval={auto_regen_interval} seconds", virtu_logDEBUG)
        channels = load_channels()
        if not channels:
            virtu_log("auto_regen_loop: No channels to replenish", virtu_logDEBUG)
        else:
            threshold_sec = int(ADDON.getSetting('auto_regen_threshold') or 12) * 3600
            successful = 0
            total_channels = len(channels)
            for channel in channels:
                channel_name = channel.get('name', 'Unknown')
                channel_number = channel.get('number', -1)
                try:
                    if 'last_gen_time' in channel and 'total_gen_duration' in channel:
                        last_gen = datetime.datetime.fromisoformat(channel['last_gen_time'])
                        time_passed = (datetime.datetime.now(datetime.timezone.utc) - last_gen).total_seconds()
                        time_left = channel['total_gen_duration'] - time_passed
                        virtu_log(f"auto_regen_loop: Channel {channel_name} (number {channel_number}): time_left={time_left}, threshold={threshold_sec}, is_new={channel.get('is_new', False)}", virtu_logDEBUG)
                        if time_left < threshold_sec or channel.get('is_new', False):
                            virtu_log(f"auto_regen_loop: Replenishing channel {channel_name} (time_left={time_left} < {threshold_sec} or is_new={channel.get('is_new', False)})", virtu_logINFO)
                            if update_channel_files(channel_number):
                                successful += 1
                                virtu_log(f"auto_regen_loop: Successfully replenished channel {channel_name} ({channel_number})", virtu_logINFO)
                            else:
                                virtu_log(f"auto_regen_loop: Failed to replenish channel {channel_name} ({channel_number})", virtu_logERROR)
                        else:
                            virtu_log(f"auto_regen_loop: Channel {channel_name} does not need replenishment (time_left={time_left} >= {threshold_sec})", virtu_logDEBUG)
                    else:
                        virtu_log(f"auto_regen_loop: Channel {channel_name} missing last_gen_time or total_gen_duration, forcing replenishment", virtu_logWARNING)
                        if update_channel_files(channel_number):
                            successful += 1
                            virtu_log(f"auto_regen_loop: Successfully replenished channel {channel_name} ({channel_number})", virtu_logINFO)
                        else:
                            virtu_log(f"auto_regen_loop: Failed to replenish channel {channel_name} ({channel_number})", virtu_logERROR)
                except Exception as e:
                    virtu_log(f"auto_regen_loop: Error processing channel {channel_name}: {str(e)}", virtu_logERROR)
            if successful > 0:
                if sync_files():
                    virtu_log(f"auto_regen_loop: Replenishment complete: {successful}/{total_channels} channels updated", virtu_logINFO)
                else:
                    virtu_log(f"auto_regen_loop: Replenishment complete: {successful}/{total_channels} channels updated, but file sync failed", virtu_logWARNING)
            else:
                virtu_log("auto_regen_loop: No channels needed replenishment", virtu_logDEBUG)
        virtu_log(f"auto_regen_loop: Waiting for {auto_regen_interval} seconds", virtu_logDEBUG)
        if monitor.waitForAbort(auto_regen_interval):
            virtu_log("auto_regen_loop: Abort requested, exiting", virtu_logINFO)
            break
    if xbmcvfs.exists(instance_lock_file):
        xbmcvfs.delete(instance_lock_file)
        virtu_log("Released instance lock", virtu_logINFO)
    update_settings_lock(auto_regen=False)

def scan_folder_durations(folder, force_scan=False):
    """Scan durations for video files in a folder and save to JSON."""
    ffprobe_path = ADDON.getSetting('ffprobe_path')
    if not ffprobe_path:
        xbmcgui.Dialog().ok("Error", "ffprobe path not set. Please select ffprobe executable in settings.")
        virtu_log("ffprobe path not set", virtu_logERROR)
        return {}
    if not xbmcvfs.exists(ffprobe_path) or not os.access(ffprobe_path, os.X_OK):
        xbmcgui.Dialog().ok("Error", "ffprobe executable not found or not executable. Please select a valid ffprobe binary in settings.")
        virtu_log(f"ffprobe executable invalid: {ffprobe_path}", virtu_logERROR)
        return {}
    durations_file = xbmcvfs.translatePath("special://profile/addon_data/plugin.video.virtuatv/durations.json")
    durations = {}
    last_scan = 0
    if xbmcvfs.exists(durations_file):
        with xbmcvfs.File(durations_file, 'r') as f:
            try:
                data = json.loads(f.read())
                durations = data.get('durations', {})
                last_scan = data.get('last_scan', 0)
            except Exception as e:
                virtu_log(f"Error loading durations from {durations_file}: {str(e)}", virtu_logERROR)
    rescan_days = int(ADDON.getSetting('rescan_days') or 7)
    days_since_scan = (datetime.datetime.now().timestamp() - last_scan) / (24 * 3600)
    if not force_scan and days_since_scan < rescan_days:
        virtu_log(f"Folder {folder} durations fresh (last scan {days_since_scan:.1f} days ago), skipping scan", virtu_logINFO)
        return durations
    progress = xbmcgui.DialogProgressBG()
    progress.create("Scanning Durations", f"Scanning {os.path.basename(folder)}...")
    video_extensions = ('.mp4', '.mkv', '.avi', '.m4v', '.ts', '.mov')
    video_files = []
    try:
        dirs, files = xbmcvfs.listdir(folder)
        files.sort() # Ensure consistent order
        for file in files:
            if file.lower().endswith(video_extensions):
                video_files.append(os.path.join(folder, file))
        dirs.sort() # Ensure consistent order
        for dir in dirs:
            subfolder = os.path.join(folder, dir)
            sub_files = get_video_files_from_folder(subfolder)
            video_files.extend(sub_files)
    except Exception as e:
        virtu_log(f"Error listing files in folder {folder}: {str(e)}", virtu_logERROR)
        progress.close()
        xbmcgui.Dialog().ok("Error", f"Failed to list files in folder {folder}: {str(e)}")
        return durations
    total_files = len(video_files)
    if total_files == 0:
        progress.close()
        xbmcgui.Dialog().ok("Scan Complete", f"No video files found in {folder}.")
        virtu_log(f"No video files found in {folder}", virtu_logWARNING)
        return durations
    scanned_count = 0
    for idx, file in enumerate(video_files):
        progress.update(int((idx / total_files) * 100), f"Scanning {os.path.basename(file)}...")
        if file in durations and durations[file] > 0 and not force_scan:
            continue
        try:
            cmd = [ffprobe_path, '-v', 'error', '-show_entries', 'format=duration', '-of', 'json', file]
            result = subprocess.run(cmd, capture_output=True, text=True)
            data = json.loads(result.stdout)
            duration = float(data.get('format', {}).get('duration', 0))
            if duration > 0:
                durations[file] = duration
                scanned_count += 1
        except Exception as e:
            virtu_log(f"Error scanning duration for {file}: {str(e)}", virtu_logERROR)
    data = {'durations': durations, 'last_scan': datetime.datetime.now().timestamp()}
    try:
        with xbmcvfs.File(durations_file, 'w') as f:
            f.write(json.dumps(data, indent=2))
        virtu_log(f"Saved durations for {scanned_count} files to {durations_file}", virtu_logINFO)
    except Exception as e:
        virtu_log(f"Error saving durations to {durations_file}: {str(e)}", virtu_logERROR)
        xbmcgui.Dialog().ok("Error", f"Failed to save durations: {str(e)}")
    progress.close()
    xbmcgui.Dialog().ok("Scan Complete", f"Scanned durations for {scanned_count} files in {folder}.")
    virtu_log(f"Summary: Scanned durations for {scanned_count} files in {folder}", virtu_logINFO)
    return durations

def select_folder():
    folder = xbmcgui.Dialog().browse(0, "Select Folder", 'files')
    if folder:
        return folder
    return None

def get_video_files_from_folder(folder):
    """Recursively collect video files from a folder and its subfolders."""
    video_extensions = ('.mp4', '.mkv', '.avi', '.m4v', '.ts', '.mov')
    video_files = []
    try:
        dirs, files = xbmcvfs.listdir(folder)
        files.sort() # Ensure consistent order
        for file in files:
            if file.lower().endswith(video_extensions):
                video_files.append(os.path.join(folder, file))
        dirs.sort() # Ensure consistent order
        for dir in dirs:
            subfolder = os.path.join(folder, dir)
            sub_files = get_video_files_from_folder(subfolder)
            video_files.extend(sub_files)
    except Exception as e:
        virtu_log(f"Error listing files in folder {folder}: {str(e)}", virtu_logERROR)
    return video_files

def select_ffprobe_path():
    """Select and validate the ffprobe executable path."""
    dialog = xbmcgui.Dialog()
    ffprobe_path = dialog.browse(1, "Select ffprobe Executable", 'files')
    if not ffprobe_path:
        dialog.ok("Error", "No file selected for ffprobe.")
        virtu_log("No ffprobe path selected", virtu_logWARNING)
        return
    # Convert to system path and check if file exists and is executable
    ffprobe_path = xbmcvfs.translatePath(ffprobe_path)
    if xbmcvfs.exists(ffprobe_path) and os.access(ffprobe_path, os.X_OK):
        ADDON.setSetting('ffprobe_path', ffprobe_path)
        ADDON.setSetting('ffprobe_select_path', ffprobe_path)
        dialog.notification("Success", f"ffprobe path set to: {ffprobe_path}", xbmcgui.NOTIFICATION_INFO, 5000)
        virtu_log(f"ffprobe path set to: {ffprobe_path}", virtu_logINFO)
    else:
        dialog.ok("Error", "Invalid or non-executable file selected for ffprobe. Ensure it is a valid ffprobe binary.")
        virtu_log(f"Invalid ffprobe path selected: {ffprobe_path}", virtu_logERROR)
        
class VirtuaTVChannel:
    def __init__(self):
        self.Playlist = VirtuaTVPlaylist()
        self.name = ''
        self.number = 0
        self.m3u_path = ''
        self.playlist_position = 0
        self.show_time_offset = 0
        self.last_access_time = 0
        self.is_valid = False
    def set_playlist(self, filename):
        return self.Playlist.load(filename)
    def set_show_position(self, show):
        show = int(show)
        self.playlist_position = self.fix_playlist_index(show)
    def set_show_time(self, thetime):
        self.show_time_offset = thetime // 1
    def set_access_time(self, thetime):
        self.last_access_time = thetime // 1
    def get_current_duration(self):
        return self.get_item_duration(self.playlist_position)
    def get_item_duration(self, index):
        return self.Playlist.get_duration(self.fix_playlist_index(index))
    def get_current_title(self):
        return self.get_item_title(self.playlist_position)
    def get_item_title(self, index):
        return self.Playlist.get_title(self.fix_playlist_index(index))
    def get_current_episode_title(self):
        return self.get_item_episode_title(self.playlist_position)
    def get_item_episode_title(self, index):
        return self.Playlist.get_episode_title(self.fix_playlist_index(index))
    def get_current_description(self):
        return self.get_item_description(self.playlist_position)
    def get_item_description(self, index):
        return self.Playlist.get_description(self.fix_playlist_index(index))
    def get_current_filename(self):
        return self.get_item_filename(self.playlist_position)
    def get_item_filename(self, index):
        return self.Playlist.get_filename(self.fix_playlist_index(index))
    def fix_playlist_index(self, index):
        if self.Playlist.size() == 0:
            return index
        while index >= self.Playlist.size():
            index -= self.Playlist.size()
        while index < 0:
            index += self.Playlist.size()
        return index

class VirtuaTVPlaylist:
    def __init__(self):
        self.item_list = []
    def get_duration(self, index):
        if 0 <= index < len(self.item_list):
            return self.item_list[index]['duration']
        return 0
    def get_filename(self, index):
        if 0 <= index < len(self.item_list):
            return self.item_list[index]['filename']
        return ''
    def get_title(self, index):
        if 0 <= index < len(self.item_list):
            return self.item_list[index]['title']
        return ''
    def get_episode_title(self, index):
        if 0 <= index < len(self.item_list):
            return self.item_list[index]['episode_title']
        return ''
    def get_description(self, index):
        if 0 <= index < len(self.item_list):
            return self.item_list[index]['description']
        return ''
    def size(self):
        return len(self.item_list)
    def load(self, filename):
        virtu_log(f"Loading playlist: {filename}", virtu_logINFO)
        self.item_list = []
        try:
            if not xbmcvfs.exists(filename):
                virtu_log(f"Playlist file does not exist: {filename}", virtu_logERROR)
                return False
            file = xbmcvfs.File(filename, 'r')
            lines = file.read().splitlines()
            file.close()
            i = 0
            while i < len(lines):
                if lines[i].startswith('#EXTM3U'):
                    i += 1
                    continue
                if lines[i].startswith('#EXTINF:'):
                    item = {}
                    extinf = lines[i][8:]
                    index = extinf.find(',')
                    if index > 0:
                        item['duration'] = int(extinf[:index])
                        title_data = extinf[index + 1:].split('//')
                        item['title'] = title_data[0]
                        item['episode_title'] = title_data[1] if len(title_data) > 1 else ''
                        item['description'] = title_data[2] if len(title_data) > 2 else ''
                    i += 1
                    if i < len(lines):
                        item['filename'] = lines[i]
                        self.item_list.append(item)
                i += 1
            return len(self.item_list) > 0
        except Exception as e:
            virtu_log(f"Error loading playlist {filename}: {str(e)}", virtu_logERROR)
            return False

def parse_m3u_for_epg(m3u_path):
    virtu_log(f"Parsing M3U for EPG: {m3u_path}", virtu_logINFO)
    programs = []
    try:
        playlist = VirtuaTVPlaylist()
        if playlist.load(m3u_path):
            for index, item in enumerate(playlist.item_list):
                programs.append({
                    'duration': item['duration'],
                    'title': item['title'],
                    'episode_title': item['episode_title'],
                    'description': item['description'],
                    'filename': item['filename']
                })
        return programs
    except Exception as e:
        virtu_log(f"Error parsing M3U {m3u_path}: {str(e)}", virtu_logERROR)
        return []

def show_epg(channels=None, current_channel=0):
    virtu_log("Showing EPG", virtu_logINFO)
    try:
        if channels is None:
            if not xbmcvfs.exists(CHANNELS_FILE):
                xbmcgui.Dialog().ok("VirtuaTV", "No channels found. Create channels or open settings?")
                return
            with xbmcvfs.File(CHANNELS_FILE, 'r') as f:
                channels_data = json.load(f)
            storage_path = debug_settings()
            channels = []
            for ch in channels_data:
                channel = VirtuaTVChannel()
                channel.name = ch['name']
                channel.number = ch['number']
                channel.m3u_path = os.path.join(storage_path, f"VirtuaTV_Channel_{ch['number']}_{''.join(c for c in ch['name'] if c.isalnum() or c in (' ', '_', '-')).replace(' ', '_')}.m3u")
                if channel.set_playlist(channel.m3u_path):
                    channel.is_valid = True
                    channels.append(channel)
        if not channels:
            if xbmcgui.Dialog().yesno("VirtuaTV", "No valid channels found. Create channels or open settings?", yeslabel="Create", nolabel="Settings"):
                xbmc.executebuiltin('RunPlugin(plugin://plugin.video.virtuatv/?action=create_channel)')
            else:
                xbmc.executebuiltin('Addon.OpenSettings(plugin.video.virtuatv)')
            return
        channels.sort(key=lambda x: x.number)
        epg = VirtuaTVEPG("script-virtuatv-epg.xml", ADDON.getAddonInfo('path'), "default")
        epg.channels = channels
        epg.current_channel = current_channel if current_channel in [ch.number for ch in channels] else channels[0].number
        epg_data = {}
        for channel in channels:
            channel_id = f"{channel.name.replace(' ', '_')}@VirtuaTV"
            programs = parse_m3u_for_epg(channel.m3u_path)
            epg_data[channel_id] = {
                'display_name': f"{channel.number}. {channel.name}",
                'programs': [
                    {
                        'title': prog['title'],
                        'episode_title': prog['episode_title'],
                        'desc': prog['description']
                    } for prog in programs
                ]
            }
        epg.epg_data = epg_data
        epg.doModal()
        virtu_log("show_epg: EPG closed", virtu_logINFO)
        del epg
    except Exception as e:
        virtu_log(f"Error showing EPG: {str(e)}", virtu_logERROR)
        xbmcgui.Dialog().ok("VirtuaTV", "Error displaying EPG.")

def play_channel(channel_number, channels):
    virtu_log(f"Playing channel {channel_number}", virtu_logINFO)
    try:
        channel = next((ch for ch in channels if ch.number == channel_number), None)
        if not channel or not channel.is_valid:
            virtu_log(f"Invalid channel {channel_number}", virtu_logERROR)
            xbmcgui.Dialog().notification("VirtuaTV", f"Invalid channel {channel_number}", xbmcgui.NOTIFICATION_ERROR, 3000)
            return
        playlist = xbmc.PlayList(xbmc.PLAYLIST_VIDEO)
        playlist.clear()
        if xbmcvfs.exists(channel.m3u_path):
            virtu_log(f"play_channel: Loading playlist for channel {channel_number}", virtu_logINFO)
            playlist.load(channel.m3u_path)
            start_pos = channel.playlist_position
            xbmc.executebuiltin('Dialog.Close(videoosd)')  # Close OSD before playback
            xbmc.Player().play(playlist, startpos=start_pos)
            monitor = xbmc.Monitor()
            timeout = 5  # Wait up to 5 seconds
            while not xbmc.Player().isPlayingVideo() and timeout > 0:
                if monitor.waitForAbort(0.1):
                    virtu_log("play_channel: Playback aborted during start wait", virtu_logWARNING)
                    return
                timeout -= 0.1
            if not xbmc.Player().isPlayingVideo():
                virtu_log(f"play_channel: Failed to start playback for channel {channel_number}", virtu_logERROR)
                xbmcgui.Dialog().notification("VirtuaTV", f"Failed to start playback for channel {channel_number}", xbmcgui.NOTIFICATION_ERROR, 3000)
                return
            with xbmcvfs.File(LAST_CHANNEL_JSON, 'w') as f:
                json.dump({'number': channel.number, 'name': channel.name}, f)
            virtu_log(f"Saved last channel: {channel.number} - {channel.name}, starting at position {start_pos}", virtu_logINFO)
            # Reinitialize overlay
            xbmc.executebuiltin('Dialog.Close(videoosd)')  # Ensure no OSD before overlay
            overlay = VirtuaTVOverlay("script-virtuatv-overlay.xml", ADDON.getAddonInfo('path'), "default", channels=channels, current_channel=channel_number)
            overlay.show()
            xbmcgui.Window(10000).setProperty('VirtuaTVOverlayActive', 'true')
            virtu_log(f"play_channel: Overlay shown for channel {channel_number}", virtu_logINFO)
            while not overlay.is_closing and not monitor.abortRequested():
                xbmc.sleep(100)
            overlay.close()
            xbmcgui.Window(10000).clearProperty('VirtuaTVOverlayActive')
            xbmcgui.Window(10000).clearProperty('VirtuaTVEPGActive')
            virtu_log(f"play_channel: Overlay closed for channel {channel_number}", virtu_logINFO)
            xbmc.executebuiltin('Dialog.Close(videoosd)')
            xbmcplugin.endOfDirectory(ADDON_HANDLE, succeeded=False)
            virtu_log("play_channel: Plugin terminated after overlay close", virtu_logINFO)
        else:
            virtu_log(f"M3U file missing for channel {channel_number}", virtu_logERROR)
            xbmcgui.Dialog().notification("VirtuaTV", f"M3U file missing for channel {channel_number}", xbmcgui.NOTIFICATION_ERROR, 3000)
    except Exception as e:
        virtu_log(f"Error playing channel {channel_number}: {str(e)}", virtu_logERROR)
        xbmcgui.Dialog().notification("VirtuaTV", f"Error playing channel {channel_number}: {str(e)}", xbmcgui.NOTIFICATION_ERROR, 3000)

class VirtuaTVEPG(xbmcgui.WindowXMLDialog):
    def __init__(self, *args, **kwargs):
        xbmcgui.WindowXMLDialog.__init__(self, *args, **kwargs)
        self.channels = []
        self.current_channel = 0
        self.focus_row = 0
        self.focus_index = 0
        self.channel_buttons = [[] for _ in range(6)]  # 6 rows for channels
        self.action_semaphore = threading.BoundedSemaphore()
        self.text_color = "FFFFFFFF"
        self.focused_color = "FF7d7d7d"
        self.text_font = "font10"
        self.media_path = os.path.join(args[1], 'resources', 'skins', xbmc.getSkinDir(), 'media') + '/'
        if not os.path.exists(self.media_path):
            self.media_path = os.path.join(args[1], 'resources', 'skins', 'default', 'media') + '/'
        self.programs_per_row = 7
        self.current_time_bar = None

    def onInit(self):
        virtu_log("VirtuaTVEPG onInit", virtu_logINFO)
        try:
            virtu_log("VirtuaTVEPG: Initializing controls", virtu_logINFO)
            timex, timey = self.getControl(120).getPosition()
            timew = self.getControl(120).getWidth()
            timeh = self.getControl(120).getHeight()
            virtu_log(f"VirtuaTVEPG: Time bar position: x={timex}, y={timey}, w={timew}, h={timeh}", virtu_logINFO)
            if self.current_time_bar:
                try:
                    self.removeControl(self.current_time_bar)
                except:
                    pass
            self.current_time_bar = xbmcgui.ControlImage(timex, timey, timew, timeh, self.media_path + 'pstvTimeBar.png')
            self.addControl(self.current_time_bar)
            self.set_time_labels()
            if self.set_channel_buttons(self.current_channel):
                self.focus_index = 0
                self.focus_row = 2
                self.setFocus(self.channel_buttons[self.focus_row][self.focus_index])
                self.set_show_info()
                virtu_log("VirtuaTVEPG: Initialization complete, 6 channels displayed", virtu_logINFO)
            else:
                raise Exception("Failed to set channel buttons")
        except Exception as e:
            virtu_log(f"Error in VirtuaTVEPG onInit: {str(e)}", virtu_logERROR)
            self.close()

    def set_time_labels(self):
        virtu_log("set_time_labels", virtu_logINFO)
        try:
            now = datetime.datetime.now()
            delta = datetime.timedelta(minutes=30)
            self.getControl(104).setLabel(now.strftime('%A, %d %B %Y').lstrip("0").replace(" 0", " "))
            for i in range(3):
                self.getControl(101 + i).setLabel(now.strftime("%I:%M %p").lstrip("0").replace(" 0", " "))
                now += delta
        except Exception as e:
            virtu_log(f"Error in set_time_labels: {str(e)}", virtu_logERROR)

    def set_channel_buttons(self, curchannel):
        virtu_log(f"set_channel_buttons {curchannel}", virtu_logINFO)
        try:
            self.center_channel = self.fix_channel(curchannel)
            curchannel = self.fix_channel(curchannel - 2, False)
            basex, basey = self.getControl(111).getPosition()
            basew = self.getControl(111).getWidth()
            baseh = self.getControl(111).getHeight()
            timex, timey = self.getControl(120).getPosition()
            timew = self.getControl(120).getWidth()
            virtu_log(f"set_channel_buttons: Base position: x={basex}, y={basey}, w={basew}, h={baseh}", virtu_logINFO)
            virtu_log("set_channel_buttons: Clearing existing buttons", virtu_logINFO)
            for row in self.channel_buttons:
                for button in row:
                    try:
                        self.removeControl(button)
                    except:
                        pass
            self.channel_buttons = [[] for _ in range(6)]
            for i in range(6):
                self.set_buttons(curchannel, i)
                curchannel = self.fix_channel(curchannel + 1)
            curchannel = self.fix_channel(self.center_channel - 2, False)
            for i in range(6):
                channel_id = curchannel
                self.getControl(301 + i).setLabel(self.channels[curchannel - 1].name)
                self.getControl(311 + i).setLabel(str(curchannel))
                self.getControl(321 + i).setImage(os.path.join(MEDIA_PATH, f"{self.channels[curchannel - 1].name}.png"))
                if not xbmcvfs.exists(os.path.join(MEDIA_PATH, f"{self.channels[curchannel - 1].name}.png")):
                    self.getControl(321 + i).setImage(os.path.join(MEDIA_PATH, "Default.png"))
                virtu_log(f"set_channel_buttons: Set channel {channel_id} at row {i}, top={400 + i * 40}", virtu_logINFO)
                curchannel = self.fix_channel(curchannel + 1)
            self.current_time_bar.setPosition(basex, timey)
            virtu_log("set_channel_buttons: Adding controls", virtu_logINFO)
            self.addControls([button for row in self.channel_buttons for button in row])
            virtu_log("set_channel_buttons: Controls added successfully", virtu_logINFO)
            return True
        except Exception as e:
            virtu_log(f"Error in set_channel_buttons: {str(e)}", virtu_logERROR)
            return False

    def set_buttons(self, curchannel, row):
        virtu_log(f"set_buttons {curchannel}, {row}", virtu_logINFO)
        try:
            curchannel = self.fix_channel(curchannel)
            basex, basey = self.getControl(111 + row).getPosition()
            baseh = self.getControl(111 + row).getHeight()
            basew = self.getControl(111 + row).getWidth()
            virtu_log(f"set_buttons: Row {row}, position: x={basex}, y={basey}, w={basew}, h={baseh}, programs={self.programs_per_row}", virtu_logINFO)
            programs = parse_m3u_for_epg(self.channels[curchannel - 1].m3u_path)
            if not programs:
                self.channel_buttons[row].append(xbmcgui.ControlButton(
                    basex, basey, basew, baseh, self.channels[curchannel - 1].name,
                    focusTexture=os.path.join(self.media_path, 'pstvButtonFocus.png'),
                    noFocusTexture=os.path.join(self.media_path, 'pstvButtonNoFocus.png'),
                    alignment=4, font=self.text_font, textColor=self.text_color, focusedColor=self.focused_color
                ))
                virtu_log(f"set_buttons: No programs for channel {curchannel}, added channel name button", virtu_logINFO)
                return True
            playlist_pos = self.channels[curchannel - 1].playlist_position
            button_width = basew // self.programs_per_row
            for i in range(self.programs_per_row):
                prog_index = (playlist_pos + i) % len(programs) if programs else 0
                if prog_index >= len(programs):
                    break
                mylabel = programs[prog_index]['title']
                xpos = basex + (i * button_width)
                self.channel_buttons[row].append(xbmcgui.ControlButton(
                    xpos, basey, button_width, baseh, mylabel,
                    focusTexture=os.path.join(self.media_path, 'pstvButtonFocus.png'),
                    noFocusTexture=os.path.join(self.media_path, 'pstvButtonNoFocus.png'),
                    alignment=4, font=self.text_font, textColor=self.text_color, focusedColor=self.focused_color
                ))
                virtu_log(f"set_buttons: Added program button at x={xpos}, y={basey}, index={prog_index}, width={button_width}", virtu_logINFO)
            return True
        except Exception as e:
            virtu_log(f"Error in set_buttons: {str(e)}", virtu_logERROR)
            return False

    def determine_playlist_pos_at_time(self, index, channel):
        virtu_log(f"determine_playlist_pos_at_time {index}, {channel}", virtu_logINFO)
        try:
            channel = self.fix_channel(channel)
            return self.channels[channel - 1].fix_playlist_index(index)
        except Exception as e:
            virtu_log(f"Error in determine_playlist_pos_at_time: {str(e)}", virtu_logERROR)
            return -1

    def set_show_info(self):
        virtu_log("set_show_info", virtu_logINFO)
        try:
            chnoffset = self.focus_row - 2
            newchan = self.center_channel
            while chnoffset != 0:
                newchan = self.fix_channel(newchan + (1 if chnoffset > 0 else -1), chnoffset > 0)
                chnoffset += -1 if chnoffset > 0 else 1
            plpos = self.determine_playlist_pos_at_time(self.channels[newchan - 1].playlist_position + self.focus_index, newchan)
            if plpos == -1:
                virtu_log("Unable to find playlist position", virtu_logERROR)
                return
            self.getControl(500).setLabel(self.channels[newchan - 1].get_item_title(plpos))
            self.getControl(501).setLabel(self.channels[newchan - 1].get_item_episode_title(plpos))
            self.getControl(502).setText(self.channels[newchan - 1].get_item_description(plpos))
            self.getControl(503).setImage(os.path.join(MEDIA_PATH, f"{self.channels[newchan - 1].name}.png"))
            if not xbmcvfs.exists(os.path.join(MEDIA_PATH, f"{self.channels[newchan - 1].name}.png")):
                self.getControl(503).setImage(os.path.join(MEDIA_PATH, "Default.png"))
            virtu_log(f"set_show_info: Updated info for channel {newchan}, position {plpos}", virtu_logINFO)
        except Exception as e:
            virtu_log(f"Error in set_show_info: {str(e)}", virtu_logERROR)

    def select_show(self):
        virtu_log("select_show", virtu_logINFO)
        try:
            chnoffset = self.focus_row - 2
            newchan = self.center_channel
            while chnoffset != 0:
                newchan = self.fix_channel(newchan + (1 if chnoffset > 0 else -1), chnoffset > 0)
                chnoffset += -1 if chnoffset > 0 else 1
            plpos = self.determine_playlist_pos_at_time(self.channels[newchan - 1].playlist_position + self.focus_index, newchan)
            if plpos == -1:
                virtu_log("Unable to find playlist position", virtu_logERROR)
                return
            virtu_log(f"select_show: Selecting channel {newchan}, program position {plpos}", virtu_logINFO)
            self.channels[newchan - 1].set_show_position(plpos)
            self.close()  # Close EPG before playing
            xbmcgui.Window(10000).clearProperty('VirtuaTVEPGActive')
            virtu_log("select_show: EPG closed before playing channel", virtu_logINFO)
            play_channel(newchan, self.channels)
        except Exception as e:
            virtu_log(f"Error in select_show: {str(e)}", virtu_logERROR)
            self.close()

    def fix_channel(self, channel, increasing=True):
        while channel < 1 or channel > len(self.channels):
            if channel < 1:
                channel = len(self.channels) + channel
            if channel > len(self.channels):
                channel -= len(self.channels)
        if not self.channels[channel - 1].is_valid:
            return self.fix_channel(channel + (1 if increasing else -1), increasing)
        return channel

    def onAction(self, act):
        action = act.getId()
        virtu_log(f"onAction {action}", virtu_logINFO)
        if self.action_semaphore.acquire(False):
            try:
                if action in ACTION_PREVIOUS_MENU:
                    virtu_log("onAction: Back key pressed, closing EPG", virtu_logINFO)
                    self.close()
                elif action == ACTION_MOVE_DOWN:
                    if self.focus_row == len(self.channel_buttons) - 1:
                        self.set_channel_buttons(self.fix_channel(self.center_channel + 1))
                        self.focus_row = len(self.channel_buttons) - 2
                    else:
                        self.focus_row += 1
                    self.set_proper_button(self.focus_row)
                elif action == ACTION_MOVE_UP:
                    if self.focus_row == 0:
                        self.set_channel_buttons(self.fix_channel(self.center_channel - 1, False))
                        self.focus_row = 1
                    else:
                        self.focus_row -= 1
                    self.set_proper_button(self.focus_row)
                elif action == ACTION_MOVE_LEFT:
                    if self.focus_index > 0:
                        self.focus_index -= 1
                        self.setFocus(self.channel_buttons[self.focus_row][self.focus_index])
                        self.set_show_info()
                elif action == ACTION_MOVE_RIGHT:
                    if self.focus_index < len(self.channel_buttons[self.focus_row]) - 1:
                        self.focus_index += 1
                        self.setFocus(self.channel_buttons[self.focus_row][self.focus_index])
                        self.set_show_info()
                elif action == ACTION_SELECT_ITEM:
                    self.select_show()
            except Exception as e:
                virtu_log(f"Error in onAction: {str(e)}", virtu_logERROR)
                self.close()
            finally:
                self.action_semaphore.release()

    def set_proper_button(self, newrow):
        self.focus_row = newrow
        if self.focus_index >= len(self.channel_buttons[newrow]):
            self.focus_index = max(0, len(self.channel_buttons[newrow]) - 1)
        self.setFocus(self.channel_buttons[newrow][self.focus_index])
        self.set_show_info()

    def onDeinit(self):
        virtu_log("VirtuaTVEPG onDeinit: Cleaning up EPG", virtu_logINFO)
        xbmcgui.Window(10000).clearProperty('VirtuaTVEPGActive')

class VirtuaTVOverlay(xbmcgui.WindowXMLDialog):
    def __init__(self, *args, **kwargs):
        xbmcgui.WindowXMLDialog.__init__(self, *args, **kwargs)
        self.channels = kwargs.get('channels', [])
        self.current_channel = kwargs.get('current_channel', 0)
        self.is_closing = False

    def onInit(self):
        virtu_log("VirtuaTVOverlay onInit", virtu_logINFO)
        try:
            self.getControl(3000).setImage(os.path.join(MEDIA_PATH, f"{self.channels[self.current_channel - 1].name}.png"))
            if not xbmcvfs.exists(os.path.join(MEDIA_PATH, f"{self.channels[self.current_channel - 1].name}.png")):
                self.getControl(3000).setImage(os.path.join(MEDIA_PATH, "Default.png"))
            xbmcgui.Window(10000).setProperty('VirtuaTVOverlayActive', 'true')
            virtu_log("VirtuaTVOverlay: Overlay active", virtu_logINFO)
        except Exception as e:
            virtu_log(f"Error in VirtuaTVOverlay onInit: {str(e)}", virtu_logERROR)

    def onAction(self, act):
        action = act.getId()
        virtu_log(f"VirtuaTVOverlay onAction {action}", virtu_logINFO)
        try:
            if action == ACTION_SELECT_ITEM:
                virtu_log("VirtuaTVOverlay: Enter key pressed, attempting to show EPG", virtu_logINFO)
                if xbmc.Player().isPlayingVideo():
                    xbmc.executebuiltin('Dialog.Close(videoosd)')  # Close video OSD only
                    xbmc.sleep(100)
                    xbmcgui.Window(10000).setProperty('VirtuaTVEPGActive', 'true')
                    show_epg(channels=self.channels, current_channel=self.current_channel)
                    xbmcgui.Window(10000).clearProperty('VirtuaTVEPGActive')
                    virtu_log("VirtuaTVOverlay: EPG closed", virtu_logINFO)
                    xbmc.executebuiltin('Dialog.Close(videoosd)')  # Ensure no OSD
                    xbmc.sleep(50)
            elif action in ACTION_PREVIOUS_MENU:
                virtu_log("VirtuaTVOverlay: Back key pressed, attempting to show close dialog", virtu_logINFO)
                if xbmc.Player().isPlayingVideo():
                    xbmc.executebuiltin('Dialog.Close(videoosd)')  # Close video OSD only
                    xbmc.sleep(100)
                    virtu_log("VirtuaTVOverlay: Showing yes/no dialog", virtu_logINFO)
                    if xbmcgui.Dialog().yesno("VirtuaTV", "Close VirtuaTV?", yeslabel="Yes", nolabel="No"):
                        virtu_log("VirtuaTVOverlay: User confirmed close, stopping playback", virtu_logINFO)
                        self.is_closing = True
                        xbmc.executebuiltin('PlayerControl(Stop)')
                        xbmc.sleep(200)
                        xbmcgui.Dialog().notification("VirtuaTV", "Settings saved, exiting to Kodi", xbmcgui.NOTIFICATION_INFO, 3000, sound=False)
                        self.close()
                        xbmcgui.Window(10000).clearProperty('VirtuaTVOverlayActive')
                        xbmcgui.Window(10000).clearProperty('VirtuaTVEPGActive')
                        xbmc.executebuiltin('ActivateWindow(10000)')
                        xbmcplugin.endOfDirectory(ADDON_HANDLE, succeeded=False)
                        virtu_log("VirtuaTVOverlay: Addon exited to Kodi main menu", virtu_logINFO)
                    else:
                        virtu_log("VirtuaTVOverlay: User canceled close dialog", virtu_logINFO)
                        xbmc.executebuiltin('Dialog.Close(videoosd)')  # Ensure no OSD
                        xbmc.sleep(50)
                else:
                    virtu_log("VirtuaTVOverlay: No video playing, closing overlay", virtu_logINFO)
                    self.close()
                    xbmcgui.Window(10000).clearProperty('VirtuaTVOverlayActive')
                    xbmcgui.Window(10000).clearProperty('VirtuaTVEPGActive')
                    xbmc.executebuiltin('ActivateWindow(10000)')
            elif action == ACTION_CONTEXT_MENU:
                virtu_log("VirtuaTVOverlay: Context menu key pressed, opening Kodi context menu", virtu_logINFO)
                xbmc.executebuiltin('ActivateWindow(10140)')
        except Exception as e:
            virtu_log(f"Error in VirtuaTVOverlay onAction: {str(e)}", virtu_logERROR)
            self.close()

    def onDeinit(self):
        virtu_log("VirtuaTVOverlay onDeinit: Cleaning up overlay", virtu_logINFO)
        xbmcgui.Window(10000).clearProperty('VirtuaTVOverlayActive')

class PlaybackMonitor(xbmc.Player):
    def __init__(self):
        super(PlaybackMonitor, self).__init__()
        self.current_channel = None
        self.last_position = -1
        self.last_time = -1
        virtu_log("PlaybackMonitor: Initialized", virtu_logINFO)

    def onPlayBackStarted(self):
        try:
            if self.isPlayingVideo():
                current_file = self.getPlayingFile()
                channels = load_channels()
                storage_path = xbmcvfs.translatePath(shared_folder).replace('\\', '/') if shared_folder and xbmcvfs.exists(xbmcvfs.translatePath(shared_folder)) else SETTINGS_DIR
                for channel in channels:
                    channel_name = channel.get('name', 'Unknown')
                    channel_number = channel.get('number', -1)
                    safe_channel_name = ''.join(c for c in channel_name if c.isalnum() or c in (' ', '_', '-')).replace(' ', '_')
                    m3u_path = os.path.join(storage_path, f'VirtuaTV_Channel_{channel_number}_{safe_channel_name}.m3u')
                    m3u_smb_path = m3u_path if storage_path.startswith('smb://') else xbmcvfs.translatePath(m3u_path)
                    if current_file == m3u_path or current_file == m3u_smb_path or m3u_path in current_file or m3u_smb_path in current_file:
                        self.current_channel = channel
                        virtu_log(f"PlaybackMonitor: Detected playback of channel {channel_name} ({channel_number})", virtu_logINFO)
                        break
                if not self.current_channel:
                    virtu_log(f"PlaybackMonitor: No channel matched for file {current_file}", virtu_logDEBUG)
                self.last_position = -1
                self.last_time = -1
        except Exception as e:
            virtu_log(f"PlaybackMonitor: Error in onPlayBackStarted: {str(e)}", virtu_logERROR)

    def onPlayBackPaused(self):
        if self.current_channel and self.isPlayingVideo():
            channel_number = self.current_channel.get('number', -1)
            channel_name = self.current_channel.get('name', 'Unknown')
            try:
                playlist = xbmc.PlayList(xbmc.PLAYLIST_VIDEO)
                self.last_position = playlist.getposition()
                self.last_time = self.getTime()
                virtu_log(f"PlaybackMonitor: Pause detected for channel {channel_name} ({channel_number}) at position {self.last_position}, time {self.last_time}", virtu_logINFO)
                # Placeholder for future pause/resume
            except Exception as e:
                virtu_log(f"PlaybackMonitor: Error in onPlayBackPaused for channel {channel_name}: {str(e)}", virtu_logERROR)
        else:
            virtu_log("PlaybackMonitor: Pause detected but no valid channel or video playing", virtu_logDEBUG)

    def onPlayBackResumed(self):
        if self.current_channel:
            channel_name = self.current_channel.get('name', 'Unknown')
            channel_number = self.current_channel.get('number', -1)
            virtu_log(f"PlaybackMonitor: Resume detected for channel {channel_name} ({channel_number})", virtu_logINFO)
            self.last_position = -1
            self.last_time = -1

    def onPlayBackStopped(self):
        if self.current_channel and self.last_position >= 0:
            channel_name = self.current_channel.get('name', 'Unknown')
            channel_number = self.current_channel.get('number', -1)
            virtu_log(f"PlaybackMonitor: Stop detected for channel {channel_name} ({channel_number}) at position {self.last_position}", virtu_logINFO)
            channels = load_channels()
            for ch in channels:
                if ch['number'] == channel_number:
                    channel_obj = next((c for c in self.channels if c.number == channel_number), None)
                    if channel_obj:
                        channel_obj.set_show_position(self.last_position)
                        virtu_log(f"PlaybackMonitor: Updated playlist_position to {self.last_position} for channel {channel_name}", virtu_logINFO)
            self.current_channel = None
            self.last_position = -1
            self.last_time = -1

    def onPlayBackEnded(self):
        if self.current_channel and self.last_position >= 0:
            channel_name = self.current_channel.get('name', 'Unknown')
            channel_number = self.current_channel.get('number', -1)
            virtu_log(f"PlaybackMonitor: End detected for channel {channel_name} ({channel_number}) at position {self.last_position}", virtu_logINFO)
            channels = load_channels()
            for ch in channels:
                if ch['number'] == channel_number:
                    channel_obj = next((c for c in self.channels if c.number == channel_number), None)
                    if channel_obj:
                        channel_obj.set_show_position(self.last_position + 1)
                        virtu_log(f"PlaybackMonitor: Updated playlist_position to {self.last_position + 1} for channel {channel_name}", virtu_logINFO)
            self.current_channel = None
            self.last_position = -1
            self.last_time = -1

def open_settings():
    virtu_log("VirtuaTV: Opening settings", virtu_logINFO)
    ADDON.openSettings()

# Main execution block
if __name__ == '__main__':
    params = {}
    if len(sys.argv) > 2:
        paramstr = sys.argv[2].lstrip('?')
        if paramstr:
            params = dict(part.split('=', 1) for part in paramstr.split('&') if '=' in part)
    action = params.get('action', '')
    virtu_log(f"Launching with action={action}, argv[0]={sys.argv[0]}", virtu_logINFO)
    settings_monitor = SettingsMonitor()
    playback_monitor = PlaybackMonitor()

    # Handle plugin actions if explicitly launched
    if sys.argv[0].startswith('plugin://plugin.video.virtuatv'):
        try:
            if xbmcgui.Window(10000).getProperty('VirtuaTVOverlayActive') == 'true' or xbmcgui.Window(10000).getProperty('VirtuaTVEPGActive') == 'true':
                virtu_log("Main execution: Overlay or EPG active, skipping actions", virtu_logINFO)
                xbmcplugin.endOfDirectory(ADDON_HANDLE, succeeded=False)
                sys.exit(0)
            elif action == 'create_channel':
                create_channel()
            elif action == 'delete_channel':
                delete_channel()
            elif action == 'open_settings':
                open_settings()
            elif action == 'regenerate_channels':
                regenerate_channels()
            elif action == 'delete_all_channels':
                delete_all_channels()
            elif action == 'backup_addon':
                backup_addon()
            elif action == 'restore_addon':
                restore_addon()
            elif action == 'edit_channel':
                edit_channel()
            elif action == 'select_ffprobe_path':
                select_ffprobe_path()
            elif action == 'rescan_durations':
                rescan_durations()
            elif action == 'do_nothing':
                do_nothing()
            elif action == 'clear_shared_folder':
                clear_shared_folder()
            else:
                main_menu()
        except Exception as e:
            virtu_log(f"Main execution error: {str(e)}", virtu_logERROR)
        virtu_log("Main execution: Plugin terminating", virtu_logINFO)
        xbmcplugin.endOfDirectory(ADDON_HANDLE, succeeded=False)
        sys.exit(0)

    # Start auto_regen_loop for Background Service mode on Kodi startup
    if (ADDON.getSettingBool('auto_regen') and
        ADDON.getSetting('service_mode') == '0' and
        not any(thread.name == 'auto_regen_loop' for thread in threading.enumerate())):
        virtu_log("Starting auto_regen_loop thread: auto_regen enabled and service_mode set to Background Service", virtu_logINFO)
        thread = threading.Thread(target=auto_regen_loop, name='auto_regen_loop')
        thread.daemon = True
        thread.start()
    else:
        virtu_log("auto_regen_loop not started: auto_regen disabled, service_mode not Background Service, or thread already running", virtu_logDEBUG)

    # Keep settings_monitor and playback_monitor alive
    while not settings_monitor.waitForAbort(10):
        pass