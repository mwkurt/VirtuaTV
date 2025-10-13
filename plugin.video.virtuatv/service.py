#!/usr/bin/python
# coding: utf-8
import xbmc
import xbmcaddon
ADDON = xbmcaddon.Addon('plugin.video.virtuatv')
ADDON_ID = ADDON.getAddonInfo('id')
ADDON_NAME = ADDON.getAddonInfo('name')
ICON = ADDON.getAddonInfo('icon')

def autostart():
    if ADDON.getSettingBool('notify'):
        xbmc.executebuiltin("Notification(%s, Auto-regeneration started, 4000, %s)" % (ADDON_NAME, ICON))
    xbmc.sleep(2000)  # Fixed 2-second delay
    xbmc.executebuiltin("RunScript(%s)" % ADDON_ID)
    xbmc.log("VirtuaTV Service: Started", xbmc.LOGINFO)

if ADDON.getSettingBool('auto_regen') and ADDON.getSetting('service_mode') == '0':
    autostart()