# *************************************************************************
# Title:    MRBus/MRBee to MQTT Raw Bridge
# Authors:  Nathan D. Holmes <maverick@drgw.net>
#           Michael D. Petersen <railfan@drgw.net>
# File:     mrbus-mqtt.py
# License:  GNU General Public License v3
#
# LICENSE:
#   Copyright (C) 2020 Nathan Holmes & Michael Petersen
#    
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 3 of the License, or
#   any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
# DESCRIPTION:
#
#*************************************************************************

import sys
import time
import traceback
import socket
import argparse
import configparser
import re
import struct
import mrbus
import datetime
import paho.mqtt.client as mqtt
import json
import logging
import daemonize
import sys, os, time
import signal

try:
   import serial.tools.list_ports
except ImportError:
   raise ImportError('serial.tools.list_ports is missing - you probably need to use pip to install serial and pySerial')

def findXbeePort():
   """This looks for the first USB serial port with an FTDI bridge chip.  In the RasPi embedded esu-bridge, this will always be the XBee."""
   ports = list(serial.tools.list_ports.grep("ttyUSB"))
   for p in ports:
      if "FTDI" == p.manufacturer:
         return p.device
   return None

def mqtt_onConnect(client, userdata, flags, rc):
   logger = userdata['logger']
   
   if rc == 0:
      # Successful Connection
      logger.info("Successful MQTT Connection")
      client.connected_flag = True
   elif rc == 1:
      logger.error("ERROR: MQTT Incorrect Protocol Version")
      client.connected_flag = False
   elif rc == 2:
      logger.error("ERROR: MQTT Invalid Client ID")
      client.connected_flag = False
   elif rc == 3:
      logger.error("ERROR: MQTT Broker Unavailable")
      client.connected_flag = False
   elif rc == 4:
      logger.error("ERROR: MQTT Bad Username/Password")
      client.connected_flag = False
   elif rc == 5:
      logger.error("ERROR: MQTT Not Authorized")
      client.connected_flag = False
   else:
      logger.error("ERROR: MQTT Other Failure %d" % (rc))
      client.connected_flag = False

def mqtt_onDisconnect(client, userdata, rc):
   logger = userdata['logger']
   logger.warning("MQTT disconnected - reason: [%s]" % (str(rc)))
   client.connected_flag = False

def mqtt_onMessage(client, userdata, message):
  try:
    message = message.payload.decode()
    decodedValues = json.loads(message)
    if 'type' in decodedValues and decodedValues['type'] == 'pkt' and 'src' in decodedValues and 'dst' in decodedValues and 'cmd' in decodedValues and 'data' not in decodedValues:
      data = []
      for d in decodedValues['data']:
        data.append(int(d))

      pkt = mrbus.packet(int(decodedValues['src']), int(decodedValues['dst']), int(decodedValues['cmd']), data)
      userdata['txQueue'].put(pkt)      
  except:
    pass


def getMillis():
   return time.time() * 1000.0

class globalConfiguration:
   sensors = None
   configOpts = None

   def __init__(self):
      self.sensors = { }
      self.configOpts = { }
      self.logger = logging.getLogger('main')

   def parserGetWithDefault(self, parser, section, key, defaultValue):
      try:
         value = parser.get(section, key)
         if value is None:
            value = defaultValue
      except:
         value = defaultValue

      return value

   def parserGetIntWithDefault(self, parser, section, key, defaultValue):
      try:
         value = parser.getint(section, key)
         if value is None:
            value = defaultValue
      except:
         value = defaultValue

      return value

   def logLevelToVal(self, logLevel, default):
      logLevelValues = { 'error':logging.ERROR, 'warning':logging.WARNING, 'info':logging.INFO, 'debug':logging.DEBUG }      
      logLevel = logLevel.lower()
      if logLevel in logLevelValues:
         return logLevelValues[logLevel]
      return default


   def setupLogger(self, logFile, consoleLogLevel, fileLogLevel):
      self.logger = logging.getLogger('main')
      self.logger.setLevel(logging.DEBUG)
      fh = logging.FileHandler(self.configOpts['logFile'])
      fh.setLevel(self.logLevelToVal(fileLogLevel, logging.DEBUG))
      
      ch = logging.StreamHandler()
      ch.setLevel(self.logLevelToVal(consoleLogLevel, logging.DEBUG))
      
      logFileFormatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
      fh.setFormatter(logFileFormatter)
      ch.setFormatter(logFileFormatter)
      
      self.logger.addHandler(fh)
      self.logger.addHandler(ch)

   def loadConfiguration(self, confFilename, isDaemon=False, logFile=None, workingDir='.'):
      # Reinitialize
      self. __init__()
      
      if not isDaemon:
         print("Reading configuration file [%s]" % (confFilename))
      parser = configparser.SafeConfigParser()
      parser.read(confFilename)
      if not isDaemon:
         print("Configuration file successfully read")

      if isDaemon:
         logFileDefault = os.path.abspath('/tmp/mrbus-mqtt.log')
      else:
         logFileDefault = os.path.abspath('%s/mrbus-mqtt.log' % (workingDir))

      if logFile is not None:
         self.configOpts['logFile'] = os.path.abspath(logFile)
      else:
         self.configOpts['logFile'] = self.parserGetWithDefault(parser, "global", "logFile", logFileDefault)

      # This is where logging gets set up
      self.configOpts['consoleLogLevel'] = self.parserGetWithDefault(parser, "global", "consoleLogLevel", "error").lower()
      self.configOpts['fileLogLevel'] = self.parserGetWithDefault(parser, "global", "fileLogLevel", "debug").lower()
      self.setupLogger(self.configOpts['logFile'], self.configOpts['consoleLogLevel'], self.configOpts['fileLogLevel'])
      
      
      self.logger.info("---------------------------------------------------------------------------")
      self.logger.info("Logging startup at %s", datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat())

      # Get global options
      baseAddrStr = self.parserGetWithDefault(parser, "global", "baseAddress", "0xFE")
      self.configOpts['baseAddress'] = int(baseAddrStr, 16)
      self.configOpts['sourceName'] = self.parserGetWithDefault(parser, "global", "sourceName", "mrbus-mqtt")
      self.configOpts['locale'] = self.parserGetWithDefault(parser, "global", "locale", "house")
      self.configOpts['mqttBroker'] = self.parserGetWithDefault(parser, "global", "mqttServer", "localhost")
      self.configOpts['mqttPort'] = self.parserGetIntWithDefault(parser, "global", "mqttPort", 1883)
      self.configOpts['mqttUsername'] = self.parserGetWithDefault(parser, "global", "mqttUsername", None)
      self.configOpts['mqttPassword'] = self.parserGetWithDefault(parser, "global", "mqttPassword", None)
      self.configOpts['mqttReconnectInterval'] = self.parserGetIntWithDefault(parser, "global", "mqttReconnectInterval", 10)
      self.configOpts['mrbusPort'] = self.parserGetWithDefault(parser, "global", "mrbusPort", None)
      self.configOpts['mrbusInterfaceType'] = self.parserGetWithDefault(parser, "global", "mrbusInterfaceType", "mrbee")


class SignalHandler():
    def __init__(self):
       self.terminate = False
       self.reparse = False

    def signalHandlerTerminate(self, signal, frame):
       self.terminate = True
       
       
import queue

def main(mainParms):
   # Unpack incoming parameters
   # mainParms = {'startupDirectory': pwd, 'configFile': configFile, 'serialPort': args.serial, 'isDaemon':isDaemon, 'logFile':args.logfile }
   
   serialPort = mainParms['serialPort']

   mrbTxQueue = queue.Queue()
   
   mrbee = None
   gConf = globalConfiguration();
   gConf.loadConfiguration(mainParms['configFile'], logFile=mainParms['logFile'], workingDir=mainParms['startupDirectory'], isDaemon=mainParms['isDaemon'])

   # Get logger
   logger = gConf.logger

   signalHandler = SignalHandler();

   mqtt.Client.connected_flag = False
   mqttClient = mqtt.Client(userdata={'logger':logger, 'txQueue':mrbTxQueue})
   mqttClient.on_connect=mqtt_onConnect
   mqttClient.on_disconnect=mqtt_onDisconnect
   mqttClient.on_message=mqtt_onMessage
   mqttClient.subscribe("%s/send" % (gConf.configOpts['locale']))
   
   if gConf.configOpts['mqttUsername'] is not None and gConf.configOpts['mqttPassword'] is not None:
      mqttClient.username_pw_set(username=gConf.configOpts['mqttUsername'], password=gConf.configOpts['mqttPassword'])

   # Initialization

   lastPacket = getMillis() - 1000.0
   lastMQTTConnectAttempt = None

   logger.info("Starting run phase")
   
   signal.signal(signal.SIGINT, signalHandler.signalHandlerTerminate)
   signal.signal(signal.SIGTERM, signalHandler.signalHandlerTerminate)
   #signal.signal(signal.SIGKILL, signalHandler.signalHandler)
   
   # Main Run Loop - runs until something weird happens
   try:
      while True:
         if signalHandler.terminate:
            raise KeyboardInterrupt
         
         
         # Initialize MRBus / MRBee client if necessary
         if mrbee is None:
            try:
               # If it didn't come as an argument, get it from the configuration file
               if serialPort is None or len(serialPort) == 0:
                  serialPort = gConf.configOpts['mrbusPort']
               # If we didn't get the port from either of those, search for an FTDI bridge
               #   part using findXbeePort()
               if serialPort is None:
                  serialPort = findXbeePort()

               if serialPort is None:
                  logger.warning("No XBee/MRBus interface found, waiting and retrying...")
                  time.sleep(2)
                  continue

               if gConf.configOpts['mrbusInterfaceType'] == 'mrbee':
                  mrbusInterfaceType = 'mrbee'
               elif gConf.configOpts['mrbusInterfaceType'] == 'ci2':
                  mrbusInterfaceType = 'mrbus'
               else:
                  logger.error("Unknown interface type [%s]" % (gConf.configOpts['mrbusInterfaceType']))
                  mrbusInterfaceType = 'unknown'

               logger.info("Trying interface [%s] on serial port [%s]" % (mrbusInterfaceType, serialPort))
               mrbee = mrbus.mrbus(serialPort, gConf.configOpts['baseAddress'], logger=gConf.logger, busType=mrbusInterfaceType)
               mrbee.setXbeeLED('D9', True);
              
            except(KeyboardInterrupt):
               raise
            except Exception as e:
               if mrbee is not None:
                  mrbee.disconnect()
               mrbee = None
               logger.exception("Exception in starting MRBus interface")
               time.sleep(2)
               continue # Restart running loop - no point bringing this thing up if we have no mrbus interface
            
         # Reconnect MQTT client if necessary

         if mqttClient.connected_flag is False and (lastMQTTConnectAttempt is None or lastMQTTConnectAttempt + gConf.configOpts['mqttReconnectInterval'] < time.time()):
            # We don't have an MQTT client and need to try reconnecting
            try:
               lastMQTTConnectAttempt = time.time()
               mqttClient.loop_start()
               mqttClient.connect(gConf.configOpts['mqttBroker'], gConf.configOpts['mqttPort'], keepalive=60)
               while not mqttClient.connected_flag: 
                  time.sleep(2) # Wait for callback to fire
               mqttClient.loop_stop()
               if mqttClient.connected_flag is True:
                  mrbee.setXbeeLED('D8', True);
            except(KeyboardInterrupt):
               raise
            except:
               mqttClient.connected_flag = False
               mrbee.setXbeeLED('D8', False);

         try:
            # Start allowing MQTT callbacks
            mqttClient.loop_start()
            pkt = mrbee.getpkt()

            if getMillis() + 100.0 > lastPacket and mrbee.getXbeeLED('D7') is True:
               mrbee.setXbeeLED('D7', False);

            if not mrbTxQueue.empty():
               try:
                  pkt = mrbTxQueue.get_nowait()
                  mrbee.sendpkt(pkt.dest, pkt.data, pkt.src)
               except:
                  pass


            if pkt is None:
               time.sleep(0.01)
               continue
            
            lastPacket = getMillis()
            mrbee.setXbeeLED('D7', True);
            
            print(pkt)
            
            if mqttClient.connected_flag is False:
               logger.warning("Skipping packet processing - no MQTT broker connected")
               continue
            
            updateTime = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

            rawTopic = "%s/raw" % (gConf.configOpts['locale'])
            updateMessage = {
              'time':updateTime,
              'type': 'pkt',
              'src': pkt.src,
              'dst': pkt.dest,
              'cmd': pkt.cmd,
              'data': ["%u" % d for d in pkt.data]
            }

            message = json.dumps(updateMessage, sort_keys=True)
            if mqttClient.connected_flag is True:
               mqttClient.publish(topic=rawTopic, payload=message)

         except (KeyboardInterrupt):
            logger.warning("Caught KeyboardInterrupt, terminating")
            raise
         
         except Exception as e:
            logger.exception("Caught some sort of exception, restarting the whole thing")
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            del exc_info         

            try:
               mrbee.disconnect()
               mrbee = None
            except:
               pass

   # Highest level, outer interrupt handler.  
   except (KeyboardInterrupt):
      logger.info("User requested program termination, exiting...")
      try:
         if mrbee is not None:
            mrbee.disconnect()
      except:
         pass

      try:
         if mqttClient is not None and mqttClient.connected_flag is True:
            mqttClient.disconnect()
      except:
         pass
      logger.info("Terminated")
      logging.shutdown()

if __name__ == "__main__":
   ap = argparse.ArgumentParser()
   ap.add_argument("-s", "--serial", help="specify serial device for XBee radio", type=str, default=None)
   ap.add_argument("-d", "--daemon", help="Daemon control:  start / stop / restart", type=str, default=None)
   ap.add_argument("-p", "--pidfile", help="Daemon pid file", type=str, default='/tmp/mrbus-mqtt.pid')
   ap.add_argument("-l", "--logfile", help="Log file", type=str, default=None)
   ap.add_argument("-c", "--config", help="specify file with configuration", type=str, default='mrbus-mqtt.cfg')
   args = ap.parse_args()
   
   # Because we might become a daemon, we need to canonicalize our path to our configuration file
   pwd = os.getcwd()
   configFileName = os.path.basename(args.config)
   configDir = os.path.dirname(args.config)
   if configDir is None or len(configDir) == 0:
      configDir = pwd
   configFile = "%s/%s" % (configDir, configFileName)
   
   isDaemon = False
   
   if args.daemon is not None:
      pidfile=args.pidfile
      daemonize.startstop(action=args.daemon, stdout='/dev/null', pidfile=pidfile)
      isDaemon = True
      
   mainParms = {'startupDirectory': pwd, 'configFile': configFile, 'serialPort': args.serial, 'isDaemon':isDaemon, 'logFile':args.logfile }
   
   try:
      main(mainParms)
   except Exception as e:
      print(e)
      if args.daemon is not None:
         try:
            os.remove(pidfile)
         except IOError:
            pass
