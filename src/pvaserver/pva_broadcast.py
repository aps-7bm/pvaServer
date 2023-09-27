import time
import logging
import numpy as np
import zmq
import pva_instance as pvai


logger = logging.getLogger("beamline")
logger.propagate = False
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

#Module variables to hold the PVABroadcast objects
data_broadcast = None
dark_broadcast = None
white_broadcast = None
theta_broadcast = None
broadcasters = [data_broadcast, dark_broadcast, white_broadcast, theta_broadcast]

#Variables regarding the ZeroMQ stream
zmq_address = "tcp://192.168.10.100"
zmq_port = 5555
beamline = "bl832"


class Args:
    '''Dummy class to hold channel name for instantiating PVABroadcast objects.
    '''
    pass


class TomoStreamPVASet:
    def __init__(self, 
                data_channel_name = "tomostreamdata:proj:image",
                white_channel_name = "tomostreamdata:white:image",
                dark_channel_name = "tomostreamdata:dark:image",
                theta_channel_name = "tomostreamdata:theta:image",
                ):
        self.set_up_pva_streams(data_channel_name,
                                dark_channel_name,
                                white_channel_name,
                                theta_channel_name,
                                )

        self.broadcasters = [self.data_broadcast, 
                            self.dark_broadcast, 
                            self.white_broadcast, 
                            self.theta_broadcast]
       
    def set_up_pva_streams(self,
                        data_channel_name = "tomostreamdata:proj:image",
                        dark_channel_name = "tomostreamdata:dark:image",
                        white_channel_name = "tomostreamdata:white:image",
                        theta_channel_name = "tomostreamdata:theta:image",
                        ):
        args = Args()
        args.channel_name = data_channel_name
        self.data_broadcast = pvai.PVABroadcaster(args)
        args.channel_name = dark_channel_name
        self.dark_broadcast = pvai.PVABroadcaster(args)
        args.channel_name = white_channel_name
        self.white_broadcast = pvai.PVABroadcaster(args)
        args.channel_name = theta_channel_name
        self.theta_broadcast = pvai.PVABroadcaster(args)

    def start_pva_streams(self):
        for i in self.broadcasters:
            i.start()

    def stop_pva_streams(self):
        for i in self.broadcasters:
            i.stop()

    def broadcast_image(self, frame_data, param_dict, frameID):
        '''Broadcast the frame to the right PVA stream.
        '''
        if '-image_key' not in param_dict.keys():
            print('Not a valid image frame.  Skipping.')
            for i in param_dict.keys():
                print(i, param_dict[i])
            return
        #Parse out the data we need
        columns, rows, dtype = self.parse_image_params(param_dict)

        #Projection frame
        if int(param_dict['-image_key']) == 0:
            self.data_broadcast.frameProducer(frameID, frame_data, columns, rows, dtype, None, t=0)
        elif int(param_dict['-image_key']) == 1:
            self.white_broadcast.frameProducer(frameID, frame_data, columns, rows, dtype, None, t=0)
        elif int(param_dict['-image_key']) == 2:
            self.dark_broadcast.frameProducer(frameID, frame_data, columns, rows, dtype, None, t=0)


    def parse_image_params(self, param_dict):
        '''Parse the rows, columns, and dtype from params dictionary.
        '''
        param_keys = param_dict.keys()
        if '-nrays' in param_keys:
            columns = param_dict['-nrays']
        elif '+nrays' in param_keys:
            columns = param_dict['+nrays']
        else:
            print("Can't find the number of columns in the image param dictionary")
            return 0, 0, 'uint16'
        if '-nslices' in param_keys:
            rows = param_dict['-nslices']
        elif '+nslices' in param_keys:
            rows = param_dict['+nslices']
        else:
            print("Can't find the number of columns in the image param dictionary")
            return 0, 0, 'uint16'
        #Find dtype.  Right now, this just gives uint16.
        #This should be changed to read what the camera is really doing.
        if '-dtype' in param_keys:
            if param_dict['-dtype'] == "":
                dtype = 'uint16'
        return columns, rows, dtype


class ReadBCSTomoData(object):
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
        
        info = np.frombuffer(data_obj["info"][4:], dtype=">u4")
        data_obj["info"] = info
        print(data_obj["info"])

        image = np.frombuffer(data_obj["image"][4:], dtype=">u2")
        image = image.reshape((1, info[1], info[0]))
        data_obj["image"] = image

        if data_obj["start"].startswith(START_TAG) and data_obj["end"].startswith(END_TAG):
            return data_obj
        else:
            logger.debug('Invalid frame: ignore')
            return None, None


    def is_final(self, data_obj):
        FINAL_TAG = b"-writedone"
        return data_obj["params"].startswith(FINAL_TAG)

    def is_delete(self, data_obj):
        FINAL_TAG = b"-delete"
        return data_obj["params"].startswith(FINAL_TAG)

    def is_garbage(self, data_obj):
        return data_obj["params"].startswith(b"meta data")

    def is_null_frame(self, data_obj):
        return (data_obj['image'].shape[1] + data_obj['image'].shape[2]) <= 3

    def sock_recv(self, socket):
        msg = socket.recv()
        return msg
    
    def params_to_dict(self, params):
        kv_pair_list = params.decode().split('\r\n')
        output_dict = {}
        for i in kv_pair_list:
            kv_split = i.split(' ')
            if len(kv_split) > 1:
                output_dict[kv_split[0]] = kv_split[1]
            else:
                output_dict[kv_split[0]] = ""
        return output_dict
    

### main from ZMQ ALS code
class ZMQ_Stream:
    def __init__(self,
                zmq_pub_address,
                zmq_pub_port,
                beamline,
                pva_set,
                ):
        logger.info(f"zmq_pub_address: {zmq_pub_address}")
        logger.info(f"zmq_pub_port: {zmq_pub_port}")
        
        # set connection
        ctx = zmq.Context()
        self.socket = ctx.socket(zmq.SUB)
        logger.info(f"binding to: {zmq_pub_address}:{zmq_pub_port}")
        self.socket.connect(f"{zmq_pub_address}:{zmq_pub_port}")
        self.socket.setsockopt(zmq.SUBSCRIBE, b"")
        
        self.reader = ReadBCSTomoData()
        self.pva_set = pva_set
    
    def zmq_monitor_loop(self, sleep_time = 0.01):
        '''Endless loop to monitor ZeroMQ stream.
        '''
        data_obj = None
        while True:
            time.sleep(sleep_time)
            try:
                data_obj = self.reader.read(self.socket)
                print("Got a data object")
                if self.reader.is_final(data_obj):
                    logger.info("Received -writedone from LabView")
                    continue
                elif self.reader.is_garbage(data_obj):
                    logger.info("!!! Ignoring message with garbage metadata tag from BCS. Probably after a restart.")
                    continue
                elif self.reader.is_null_frame(data_obj):
                    logger.info('Found null frame.  Probably the beginning or end of a scan.')
                    continue
                #We must have a valid frame.  Let's broadcast it.
                #Send the frame data, the parameters, and the frame number
                param_dict = self.reader.params_to_dict(data_obj['params'])
                self.pva_set.broadcast_image(data_obj['image'][0,...],
                                            param_dict,
                                            data_obj['info'][4])
                    
            except KeyboardInterrupt as e:
                logger.error("Ctrl-C Interruption detected, Quitting...")
                break
            except Exception as e:
                logger.exception("Frame object failed to parse:")


def main_loop():
    try:
        tomostream_pva_broadcasters = TomoStreamPVASet()
        tomostream_pva_broadcasters.start_pva_streams()
        zmq_stream = ZMQ_Stream(zmq_address, 
                                zmq_port,
                                beamline,
                                tomostream_pva_broadcasters,
                                )
        zmq_stream.zmq_monitor_loop(0.01)
    except KeyboardInterrupt as e:
        tomostream_pva_broadcasters.stop_pva_streams()
        zmq_stream.socket.close()

if __name__ == "__main__":
    main_loop()
