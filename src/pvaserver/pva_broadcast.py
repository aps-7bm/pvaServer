import time
import random
import threading
import queue
import argparse
import os
import os.path
import ctypes.util
import numpy as np
import h5py as h5

from pathlib import Path

import pvaccess as pva

from pvaserver import __version__
from pvaserver import util
from pvaserver import log


class FrameGenerator:
    def __init__(self):
        self.frames = None
        self.nInputFrames = 0
        self.rows = 0
        self.cols = 0
        self.dtype = None
        self.compressorName = None

    def getFrameData(self, frameId):
        if frameId < self.nInputFrames and frameId >= 0:
            return self.frames[frameId]
        return None

    def getFrameInfo(self):
        if self.frames is not None and not self.nInputFrames:
            self.nInputFrames, self.rows, self.cols = self.frames.shape
            self.dtype = self.frames.dtype
        return (self.nInputFrames, self.rows, self.cols, self.dtype, self.compressorName)

    def getUncompressedFrameSize(self):
        return self.rows*self.cols*self.frames[0].itemsize

    def getCompressedFrameSize(self):
        if self.compressorName:
            return len(self.getFrameData(0))
        else:
            return self.getUncompressedFrameSize()

    def getCompressorName(self):
        return self.compressorName


class NumpyRandomGenerator(FrameGenerator):

    def __init__(self, nf, nx, ny, datatype, minimum, maximum):
        FrameGenerator.__init__(self)
        self.nf = nf
        self.nx = nx
        self.ny = ny
        self.datatype = datatype
        self.minimum = minimum
        self.maximum = maximum
        self.generateFrames()

    def generateFrames(self):
        print('Generating random frames')

        dt = np.dtype(self.datatype)
        if not self.datatype.startswith('float'):
            dtinfo = np.iinfo(dt)
            mn = dtinfo.min
            if self.minimum is not None:
                mn = int(max(dtinfo.min, self.minimum))
            mx = dtinfo.max
            if self.maximum is not None:
                mx = int(min(dtinfo.max, self.maximum))
            self.frames = np.random.randint(mn, mx, size=(self.nf, self.ny, self.nx), dtype=dt)
        else:
            # Use float32 for min/max, to prevent overflow errors
            dtinfo = np.finfo(np.float32)
            mn = dtinfo.min
            if self.minimum is not None:
                mn = float(max(dtinfo.min, self.minimum))
            mx = dtinfo.max
            if self.maximum is not None:
                mx = float(min(dtinfo.max, self.maximum))
            self.frames = np.random.uniform(mn, mx, size=(self.nf, self.ny, self.nx))
            if datatype == 'float32':
                self.frames = np.float32(self.frames)

        print(f'Generated frame shape: {self.frames[0].shape}')
        print(f'Range of generated values: [{mn},{mx}]')

class AdSimServer:

    # Uses frame cache of a given size. If the number of input
    # files is larger than the cache size, the server will be constantly 
    # regenerating frames.

    SHUTDOWN_DELAY = 1.0
    MIN_CACHE_SIZE = 1
    CACHE_TIMEOUT = 1.0
    DELAY_CORRECTION = 0.0001
    NOTIFICATION_DELAY = 0.1
    BYTES_IN_MEGABYTE = 1000000
    METADATA_TYPE_DICT = {
        'value' : pva.DOUBLE,
        'timeStamp' : pva.PvTimeStamp()
    }

    def __init__(self, args):
        self.lock = threading.Lock()
        self.deltaT = 0
        self.cacheTimeout = self.CACHE_TIMEOUT
        if args.frame_rate > 0:
            self.deltaT = 1.0/args.frame_rate
            self.cacheTimeout = max(self.CACHE_TIMEOUT, self.deltaT)
        self.runtime = args.runtime
        self.reportPeriod = args.report_period 
        self.frameGeneratorList = []
        self.frameCacheSize = max(args.cache_size, self.MIN_CACHE_SIZE)
        self.nFrames = args.n_frames

        self.frameGeneratorList = self.create_frame_generator_list(args)
        
        if not self.frameGeneratorList:
            log.error('Found: %d file with extension %s' % (len(input_files), args.file_format))
            log.error('Use option:--file-format to change file format')
            exit()

        self.nInputFrames = 0
        for fg in self.frameGeneratorList:
            nInputFrames, self.rows, self.cols, self.dtype, self.compressorName = fg.getFrameInfo()
            self.nInputFrames += nInputFrames
        if self.nFrames > 0:
            self.nInputFrames = min(self.nFrames, self.nInputFrames)

        fg = self.frameGeneratorList[0]
        self.frameRate = args.frame_rate
        self.uncompressedImageSize = util.IntWithUnits(fg.getUncompressedFrameSize(), 'B')
        self.compressedImageSize = util.IntWithUnits(fg.getCompressedFrameSize(), 'B')
        self.compressedDataRate = util.FloatWithUnits(self.compressedImageSize*self.frameRate/self.BYTES_IN_MEGABYTE, 'MBps')
        self.uncompressedDataRate = util.FloatWithUnits(self.uncompressedImageSize*self.frameRate/self.BYTES_IN_MEGABYTE, 'MBps')

        self.channelName = args.channel_name
        self.pvaServer = pva.PvaServer()
        self.pvaServer.addRecord(self.channelName, pva.NtNdArray(), None)


        # Use PvObjectQueue if cache size is too small for all input frames
        # Otherwise, simple dictionary is good enough
        self.usingQueue = False
        if self.nInputFrames > self.frameCacheSize:
            self.usingQueue = True
            self.frameCache = pva.PvObjectQueue(self.frameCacheSize)
        else:
            self.frameCache = {}

        print(f'Number of input frames: {self.nInputFrames} (size: {self.cols}x{self.rows}, {self.uncompressedImageSize}, type: {self.dtype}, compressor: {self.compressorName}, compressed size: {self.compressedImageSize})')
        print(f'Frame cache type: {type(self.frameCache)} (cache size: {self.frameCacheSize})')
        print(f'Expected data rate: {self.compressedDataRate} (uncompressed: {self.uncompressedDataRate})')

        self.currentFrameId = 0
        self.nPublishedFrames = 0
        self.startTime = 0
        self.lastPublishedTime = 0
        self.startDelay = args.start_delay
        self.isDone = False
        self.screen = None
        self.screenInitialized = False
        self.disableCurses = args.disable_curses


    def create_frame_generator_list(self, args):
        """Creates a list of frame generators based on input args.
        """
        if args.use_sim_data:
            nf = args.n_frames
            if nf <= 0:
                nf = self.frameCacheSize
            self.frameGeneratorList.append(NumpyRandomGenerator(nf, args.n_x_pixels, args.n_y_pixels, args.datatype, args.minimum, args.maximum))

        else: # loading data from file(s)
            log.error("Non-sim functionality not yet implemented.")


    def setupCurses(self):
        screen = None
        if not self.disableCurses:
            try:
                import curses
                screen = curses.initscr()
                self.curses = curses
            except ImportError as ex:
                pass
        return screen

        
    def addFrameToCache(self, frameId, ntnda):
        if not self.usingQueue:
            # Using dictionary
            self.frameCache[frameId] = ntnda
        else:
            # Using PvObjectQueue
            try:
                waitTime = self.startDelay + self.cacheTimeout
                self.frameCache.put(ntnda, waitTime)
            except pva.QueueFull:
                pass
            
    def getFrameFromCache(self):
        if not self.usingQueue:
            # Using dictionary
            cachedFrameId = self.currentFrameId % self.nInputFrames
            if cachedFrameId not in self.frameCache:
            # In case frames were not generated on time, just use first frame
                cachedFrameId = 0
            ntnda = self.frameCache[cachedFrameId]
        else:
            # Using PvObjectQueue
            ntnda = self.frameCache.get(self.cacheTimeout)
        return ntnda

    def frameProducer(self, extraFieldsPvObject=None):
        startTime = time.time()
        frameId = 0
        frameData = None
        while not self.isDone:
            for fg in self.frameGeneratorList:
                nInputFrames, ny, nx, dtype, compressorName = fg.getFrameInfo()
                for fgFrameId in range(0,nInputFrames):
                    if self.isDone or (self.nInputFrames > 0 and frameId >= self.nInputFrames):
                        break
                    frameData = fg.getFrameData(fgFrameId)
                    if frameData is None:
                        break
                    ntnda = util.AdImageUtility.generateNtNdArray2D(frameId, frameData, nx, ny, dtype, compressorName, extraFieldsPvObject)
                    self.addFrameToCache(frameId, ntnda)
                    frameId += 1
            if self.isDone or not self.usingQueue or frameData is None or (self.nInputFrames > 0 and frameId >= self.nInputFrames):
                # All frames are in cache or we cannot generate any more data
                break
        self.printReport(f'Frame producer is done after {frameId} generated frames')

    def prepareFrame(self, t=0):
        # Get cached frame
        frame = self.getFrameFromCache()
        if frame is not None:
            # Correct image id and timestamps
            self.currentFrameId += 1
            frame['uniqueId'] = self.currentFrameId
            if t <= 0:
                t = time.time()
            ts = pva.PvTimeStamp(t)
            frame['timeStamp'] = ts
            frame['dataTimeStamp'] = ts
        return frame

    def framePublisher(self):
        while True:
            if self.isDone:
                return

            # Prepare frame with a given timestamp
            # so that image times are as close as possible
            try:
                frame = self.prepareFrame(updateTime)
            except pva.QueueEmpty:
                self.printReport(f'Server exiting after emptying queue')
                self.isDone = True
                return
            except Exception:
                if self.isDone:
                    return
                raise

            # Publish frame
            self.pvaServer.update(self.channelName, frame)
            self.lastPublishedTime = time.time()
            self.nPublishedFrames += 1
            if self.usingQueue and self.nPublishedFrames >= self.nInputFrames:
                self.printReport(f'Server exiting after publishing {self.nPublishedFrames}')
                self.isDone = True
                return

            runtime = 0
            frameRate = 0
            if self.nPublishedFrames > 1:
                runtime = self.lastPublishedTime - self.startTime
                deltaT = runtime/(self.nPublishedFrames - 1)
                frameRate = 1.0/deltaT
            else:
                self.startTime = self.lastPublishedTime
            if self.reportPeriod > 0 and (self.nPublishedFrames % self.reportPeriod) == 0:
                report = 'Published frame id {:6d} @ {:.3f}s (frame rate: {:.4f}fps; runtime: {:.3f}s)'.format(self.currentFrameId, self.lastPublishedTime, frameRate, runtime)
                self.printReport(report)

            if runtime > self.runtime:
                self.printReport(f'Server exiting after reaching runtime of {runtime:.3f} seconds')
                return

            if self.deltaT > 0:
                nextPublishTime = self.startTime + self.nPublishedFrames*self.deltaT
                delay = nextPublishTime - time.time() - self.DELAY_CORRECTION
                if delay > 0:
                    threading.Timer(delay, self.framePublisher).start()
                    return

    def printReport(self, report):
        with self.lock:
            if not self.screenInitialized:
                self.screenInitialized = True
                self.screen = self.setupCurses()
            if self.screen:
                self.screen.erase()
                self.screen.addstr(f'{report}\n')
                self.screen.refresh()
            else:
                print(report)

    def start(self):

        threading.Thread(target=self.frameProducer, daemon=True).start()
        self.pvaServer.start()
        threading.Timer(self.startDelay, self.framePublisher).start()

    def stop(self):
        self.isDone = True
        self.pvaServer.stop()
        runtime = self.lastPublishedTime - self.startTime
        deltaT = 0
        frameRate = 0
        if self.nPublishedFrames > 1:
            deltaT = runtime/(self.nPublishedFrames - 1)
            frameRate = 1.0/deltaT
        dataRate = util.FloatWithUnits(self.uncompressedImageSize*frameRate/self.BYTES_IN_MEGABYTE, 'MBps')
        time.sleep(self.SHUTDOWN_DELAY)
        if self.screen:
            self.curses.endwin()
        print('\nServer runtime: {:.4f} seconds'.format(runtime))
        print('Published frames: {:6d} @ {:.4f} fps'.format(self.nPublishedFrames, frameRate))
        print(f'Data rate: {dataRate}')


class ReadBCSTomoData(object):
    def __init__(self):
        pass

        
    def read(self, socket):
        START_TAG = b"[start]"  
        END_TAG = b"[end]"

        data_obj = {}
        data_obj["start"] = self.sock_recv(socket)
        data_obj["image"] = self.sock_recv(socket)
        data_obj["info"] = self.sock_recv(socket)
        data_obj["h5_file"] = self.sock_recv(socket)
        data_obj["tif_file"] = self.sock_recv(socket)
        data_obj["params"] = self.sock_recv(socket)
        data_obj["end"] = self.sock_recv(socket)

        info = data_obj["info"]
        image_buffer_size = len(data_obj["image"])
        info_size = int.from_bytes(info[0:4], byteorder='big')  # unused
        # logger.debug(f"info_size: {info_size}   buffer_size: {image_buffer_size}")

        info = np.frombuffer(info[4:], dtype=">u4")
        data_obj["info"] = info

        image = data_obj["image"]
        # if len(image) > 100:
        #     logger.info(f"first char: {image[0:99]}") 
 

            
        # image_size = int.from_bytes(image[0:4], byteorder='big') # unused
        image = np.frombuffer(image[4:], dtype=">u2")
        # logger.info(image[0:24])
        image = image.reshape((info[0], info[1]))
        image = image.reshape((1, info[1], info[0]))

        data_obj["image"] = image

        if data_obj["start"].startswith(START_TAG) and data_obj["end"].startswith(END_TAG):
            return data_obj

        return None

    def is_final(self, data_obj):
        FINAL_TAG = b"-writedone"
        return data_obj["params"].startswith(FINAL_TAG)

    def is_delete(self, data_obj):
        FINAL_TAG = b"-delete"
        return data_obj["params"].startswith(FINAL_TAG)

    def is_garbage(self, data_obj):
        return data_obj["params"].startswith(b"meta data")

    # DEBUG = open("tmp.run", "wb")
    def sock_recv(self, socket):
        msg = socket.recv()
        #if not DEBUG is None:
        #    DEBUG.write(msg)
        #print(msg)
        return msg

### main from ZMQ ALS code

def main(zmq_pub_address: str = "tcp://192.168.1.84",
         zmq_pub_port: int = 5555,
         beamline: str = "bl832",
         notify_workflow = True,
         prefect_api_url=None,
         prefect_api_key=None,
         prefect_deployment="new_832_file_flow/new_file_832",
         log_level="INFO"):
    logger.setLevel(log_level.upper())
    logger.debug("DEBUG LOGGING SET")
    logger.info(f"zmq_pub_address: {zmq_pub_address}")
    logger.info(f"zmq_pub_port: {zmq_pub_port}")
    # set connection
    ctx = zmq.Context()
    socket = ctx.socket(zmq.SUB)
    logger.info(f"binding to: {zmq_pub_address}:{zmq_pub_port}")
    socket.connect(f"{zmq_pub_address}:{zmq_pub_port}")
    socket.setsockopt(zmq.SUBSCRIBE, b"")

    reader = ReadBCSTomoData()
    writer = DXWriter()

    data_obj = None
    while True:
        try:
            data_obj = reader.read(socket)
            if reader.is_final(data_obj):
                logger.info("Received -writedone from LabView")
                writer.finalize(data_obj)
                asyncio.run(
                    prefect_start_flow(prefect_api_url, prefect_deployment, writer.active_filename, api_key=prefect_api_key)
                )
                if writer.write_times and len(writer.write_times) > 0:
                    average_write_time = sum(writer.write_times) / len(writer.write_times)
                    logger.info(f"Average frame {average_write_time: .2f} kbps")
                writer.write_times = []
            elif reader.is_delete(data_obj):
                logger.debug("deletion tag called")
                # TODO delete file
                pass
            elif reader.is_garbage(data_obj):
                logger.info("!!! Ignoring message with garbage metadata tag from BCS. Probably after a restart.")
            else:
                writer.process(data_obj)
        except KeyboardInterrupt as e:
            logger.error("Ctrl-C Interruption detected, Quitting...")
            break
        except Exception as e:
            logger.exception("Frame object failed to parse:")
