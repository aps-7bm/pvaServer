import time
import random
import threading
import numpy as np

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


class PVABroadcaster:
    '''Broadcasts images sent to this class over PVA.
    '''

    SHUTDOWN_DELAY = 1.0

    def __init__(self, args):
        self.channelName = args.channel_name
        self.pvaServer = pva.PvaServer()
        self.pvaServer.addRecord(self.channelName, pva.NtNdArray(), None)
            

    def frameProducer(self, frame_data, nx, ny, dtype, compressorName, t=0):
        startTime = time.time()
        frameId = 0
        ntnda = util.AdImageUtility.generateNtNdArray2D(frameId, frame_data, nx, ny, dtype, compressorName, None)
        if t <= 0:
            t = time.time()
        ts = pva.PvTimeStamp(t)
        ntnda['timeStamp'] = ts
        ntnda['dataTimeStamp'] = ts
        self.pvaServer.update(self.channelName, ntnda)

    def start(self):
        self.pvaServer.start()


    def stop(self):
        self.isDone = True
        self.pvaServer.stop()
        time.sleep(self.SHUTDOWN_DELAY)
        print("Shutting down pvaBroadcast")


class ArgsHolder():
    pass


def test_code(rows = 256, columns = 256, dtype = 'uint8', num_frames = 100, time_delay = 0.1):
    '''Test that PVA broadcast works as we want it to.
    '''
    random_frames = NumpyRandomGenerator(num_frames, columns, rows, dtype, 0, 100)
    args = ArgsHolder()
    args.channel_name = "pvapy:image"
    pvab = PVABroadcaster(args)
    pvab.start()
    print(random_frames.frames.shape)
    for i in range(random_frames.frames.shape[0]):
        pvab.frameProducer(random_frames.frames[i,...], columns, rows, dtype, None)
        time.sleep(0.5)





