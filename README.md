# VirtuaTV
# VirtuaTV – Readme for Kodi + PseudoTV Classic Users

**VirtuaTV** turns your existing Kodi video library into real-looking 24/7 TV channels that work perfectly with the classic **script.pseudotv** (fnord12 fork).  
No more playlist resets, no more “jumping back to the beginning” when you pause for days – it just works like real TV.

## What VirtuaTV actually does (in plain English)

- Takes your smart playlists (.xsp files) of TV shows or movies  
- Builds a never-ending schedule (M3U + XMLTV) that always has many hours ahead of “now”  
- Automatically adds new episodes/movies in the background so the channel never ends  
- **Never breaks resume** in PseudoTV Classic – you can pause for a week and come back exactly where you left off  
- Works with local files, SMB, NFS, UPnP – anything Kodi can see  

## Key Features

| Feature | What it means for you |
|-------|-----------------------|
| **Safe Append Mode** | 100 % safe for PseudoTV Classic – no skipped or repeated episodes |
| **Time-based refill** | Keeps ~24–48 hours of programming ahead of “now” at all times |
| **Automatic rebuild when file gets too big** | M3U files never grow forever (you choose the limit, e.g. 25 MB) |
| **Full cycle completion** | Prevents the same show playing twice in a row at the append point |
| **Optional combined M3U/XMLTV** | One big file for people who prefer IPTV Simple Client or external players |
| **PseudoTV-compatible M3U format** | Shows proper SxxExx titles and plots inside PseudoTV |

## How to install

1. Download the latest release from the **Releases** page  
2. In Kodi → Add-ons → Install from zip file → select the zip  
3. Done – VirtuaTV appears under Video Add-ons

## How to create your first channel (2 minutes)

1. Open VirtuaTV → **Create a New Channel**  
2. Give it a name and pick a number  
3. Choose **Base playlist** → pick any .xsp smart playlist (TV shows or movies)  
4. (Optional) Add more playlists with interleave settings for bumpers/commercials  
5. Click **OK** → channel is built instantly

# How to Use VirtuaTV with PseudoTV Classic (fnord12 fork)

This guide shows you the **bullet-proof way** to make VirtuaTV completely take over your PseudoTV Classic channels so they never run out, never repeat the same show for hours, and never break resume—even after pausing for days.

**BACK UP YOUR CURRENT PseudoTV SETTINGS FIRST!**  
If anything feels wrong, you can always restore.

### Why do this?
- PseudoTV Classic by itself eventually runs out of programs or repeats the same long show forever.
- VirtuaTV gives you true 24/7 channels that keep going forever with perfect episode rotation and perfect resume.

### Step-by-Step Setup

1. **Turn OFF every PseudoTV background updating option**  
   In PseudoTV Settings → Channel Settings → uncheck ALL of these:
   - Background Updating Enabled
   - Update Channels in Background
   - Force Channel Reset on Startup
   - Any “Auto-update” or “Refresh” options  
   → You want PseudoTV to **never touch** the M3U files again.

2. **Create your channels in PseudoTV exactly as you normally would**  
   - You can even use a tiny number (e.g., 10–20 programs) – it doesn’t matter.
   - Just make sure the channel uses the exact same .xsp smart playlists you will use in VirtuaTV.

3. **Create the SAME channels in VirtuaTV**
   - Open VirtuaTV → Create a New Channel
   - Use the **exact same channel numbers** and the **exact same .xsp playlists** you used in PseudoTV
   - Recommended VirtuaTV settings (one-time):
     - Enable Auto-Regeneration: ON
     - Safe Append Mode: ON
     - Refill when less than: 6–12 hours
     - Rebuild if M3U exceeds: 25–50 MB (prevents huge files)
     - Combined output: OFF (not needed)

4. **Replace the PseudoTV M3U/XMLTV files with VirtuaTV’s files**
   After VirtuaTV finishes generating:
   - Go to your VirtuaTV cache folder  
     (usually: `userdata/addon_data/plugin.video.virtuatv/cache/`)
   - Copy these files for each channel:
     - `channel_n.m3u`  → overwrite the one in your PseudoTV channels folder
     -  The channel_n.xmltv` are not used by PTV but will also be created.
   - If you use a shared network folder for PseudoTV channels, overwrite the files there too.
   - Change the default VirtuaTV output folder to whatever folder you have PTV writing to so that VirtuaTV's m3u files will overwrite current m3u files during append.
   - You must have PTV create the initial m3u files then copy your new m3u files overtop.

5. **Restart PseudoTV (or Kodi)**
   - Your channels now run 100 % on VirtuaTV’s never-ending schedules.
   - From now on VirtuaTV silently keeps them filled in the background.

### What You Get Forever

- Channels never run out
- No more “same show for 6 hours” problem
- Perfect resume after days of pausing
- New episodes appear automatically when you add them to your library
- Zero maintenance

Enjoy real 24/7 TV from your own collection – exactly like cable, but yours.

You’re done. Fire it up and forget it!

## Recommended settings (set once and forget)

| Setting | Recommended value | Why |
|-------|-------------------|-----|
| **Enable Auto-Regeneration** | ON | Turns on the background refill |
| **Check Interval** | 60 minutes (or 30 if you want it snappier) | How often it checks if more content is needed |
| **Safe Append Mode (PseudoTV Classic)** | ON | This is the magic that makes resume perfect |
| **Safe Append: Refill when less than** | 6–12 hours | Safety buffer – lower = more frequent small appends |
| **Rebuild channel if M3U exceeds** | 25–50 MB (0 = never) | Keeps files from growing forever |
| **Generate Combined M3U/XMLTV** | Your choice (usually OFF for PseudoTV Classic) | Only needed for external players |

That’s literally it. Once these are set, your channels run forever with zero maintenance.

## Frequently Asked Questions

**Q: Will my channels ever run out of episodes?**  
A: No – every show cycles forever (last_index rolls over automatically).

**Q: Do I need to do anything when I add new episodes to my library?**  
A: Nothing. The next time VirtuaTV refreshes the playlist it will see the new files and include them automatically.

**Q: Can I use this with PseudoTV Live or IPTV Simple Client?**  
A: Yes, but turn **Safe Append Mode OFF** if you use PseudoTV Live – Live reads the file differently.

**Q: My M3U file is getting huge!**  
A: Set “Rebuild channel if M3U exceeds” to 25–50 MB. When it hits that size it will do a clean rebuild and start fresh.



Enjoy real 24/7 TV channels from your own library – no more babysitting playlists!

— Happy watching! 🚀
