import asyncio
import datetime
from enum import IntEnum
import json
import logging
import os
from pathlib import Path
import time
import uuid

import h5py
from lxml import etree
import numpy as np
from prefect.client import OrionClient
from prefect.orion.schemas.core import Flow, FlowRun
from prefect.orion.schemas.states import Scheduled
import pytz
import requests
import typer
import tzlocal
import zmq


logger = logging.getLogger("beamline")
logger.propagate = False
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

local = pytz.timezone ("America/Los_Angeles")


def convert_2_string_list(value):
        try:
            return np.string_(value)
        except:
            return value

def convert_to_string(value):
    return np.string_(value)


def iso_time_2_stamp(iso_time: str, local_tz):
    return local_tz.localize(datetime.datetime.strptime(iso_time, "%m-%d-%Y %H:%M:%S.%f"), is_dst=None)


def bcs_params_2_dict(bcs_params):
    """ BCS sends image parameters in a unique encoding, as a list of 
    values that looks like: 
       ['-pgeometry PARALLEL', '-pfilegeom RADIOGRAPH', '+sdate 09-29-2022 09:48:19.974']
       
    Where - means something like and+ means something

    This method parses each item and builds returnst hem as a dictionary.
    """
    return_params = {}
    for param in bcs_params:
        param = param.strip()
        if len(param) == 0:
            continue

        # if param[0] == '-':
        #     continue

        split_index = param.find(" ")
        if split_index > 0:
            key = param[1:split_index]
            value = param[split_index+1:]
            return_params[key] = convert_2_string_list(value)
    return return_params

def bcs_date_2_dt(binary_iso):
    return local.localize(datetime.datetime.strptime(binary_iso.decode(), "%m-%d-%Y %H:%M:%S.%f"), is_dst=None)


local_tz = pytz.timezone("America/Los_Angeles")
# ROOT_MAP = { "source" : f"R:", "dest": "/home/alscompute/data" } 
ROOT_MAP = { "source" : f"R:", "dest": "/global/raw" } 

map_dict = {
                # top level
                "/measurement/sample/file_name" : ["dataset", "str", "file", 0, ""],
                "/measurement/instrument/instrument_name" : ["end_station", "str", "file", 0, ""],
                "/measurement/instrument/source/beamline" : ["end_station", "str", "file", 0, ""],
                "/measurement/instrument/source/source_name" : ["facility", "str", "file", 0, ""],
                # "/measurement/sample/experimenter/name" : ["owner", "str", "file", 0, ""], # removed, it's now coming from the xml proposal document
                #"/" : ["stage", "str", "file", 0],
                "/process/acquisition/start_date" : ["sdate", "timestamp", "file", 0, ""],
                # "/measurement/instrument/time_stamp" : ["stage_date", "timestamp", "group", 1, ""],
                #"/" : ["stage_flow", "str", "group", 1], #"/" : ["stage_version", "str", "group", 1],
                "/measurement/sample/uuid" : ["uuid", "str", "file", 0, ""],

                # data parent level
                "/measurement/instrument/source/current" : ["Beam_Current", "float", "group", 0, ""],
                "/measurement/instrument/camera_motor_stack/setup/camera_distance" : ["Camera_Z_Support", "float", "group", 1, ""],
                "/measurement/instrument/detector/dark_field_value" : ["Dark_Offset", "int", "group", 0, ""],
                "/measurement/instrument/attenuator/setup/filter_y" : ["Filter_Motor", "int", "group", 0, "mm"],
                "/measurement/instrument/slits/setup/hslits_A_Door" : ["Horiz_Slit_A_Door", "float", "group", 0, "mm"],
                "/measurement/instrument/slits/setup/hslits_A_Wall" : ["Horiz_Slit_A_Wall", "float", "group", 0, "mm"],
                "/measurement/instrument/slits/setup/hslits_center" : ["Horiz_Slit_Pos", "float", "group", 0, "mm"],
                "/measurement/instrument/slits/setup/hslits_size" : ["Horiz_Slit_Size", "float", "group", 0, "mm"],
                "/measurement/instrument/source/beam_intensity_incident" : ["Izero", "float", "group", 1, ""],
                "/measurement/instrument/slits/setup/vslits_Lead_Flag" : ["Lead_Flag", "float", "group", 0, ""],
                "/measurement/instrument/monochromator/energy" : ["senergy", "float", "group", 1, "eV"],
                #"/" : ["Reconstruction_Type"], #"/" : ["TC0"], #"/" : ["TC1"],
                "/measurement/instrument/monochromator/setup/temperature_tc2" : ["TC2", "float", "group", 0, ""],
                "/measurement/instrument/monochromator/setup/temperature_tc3" : ["TC3", "float", "group", 0, ""],
                "/measurement/instrument/monochromator/setup/Z2" : ["Z2", "float", "group", 0, ""],
                "/process/acquisition/rotation/range" : ["arange", "int", "group", 0, ""],
                #"/" : ["archdir"], #"/" : ["auto_eval_roi"],
                "/measurement/instrument/sample_motor_stack/setup/axis1pos" : ["axis1pos", "float", "group", 1, ""],
                "/measurement/instrument/sample_motor_stack/setup/axis2pos" : ["axis2pos", "float", "group", 1, ""],
                "/measurement/instrument/sample_motor_stack/setup/sample_x" : ["axis3pos", "float", "group", 1, ""],
                "/measurement/instrument/sample_motor_stack/setup/sample_y" : ["axis4pos", "float", "group", 1, ""],
                "/measurement/instrument/sample_motor_stack/setup/axis5pos" : ["axis5pos", "float", "group", 1, ""],
                "/measurement/instrument/camera_motor_stack/setup/camera_elevation" : ["axis6pos", "float", "group", 1, ""],

                #beta
                #bgeometry
                "/process/acquisition/rotation/blur_limit" : ["blur_limit", "int", "group", 0, ""],
                "/process/acquisition/flat_fields/bright_num_avg_of" : ["bright_num_avg_of", "int", "group", 0, ""],
                "/process/acquisition/flat_fields/flat_field_exposure" : ["brightexptime", "int", "group", 0, ""],
                "/measurement/instrument/detector/model" : ["camera_used", "str", "group", 0, ""],

                #cammode #cddepth #cdmaterial #cdtype #cdxsize #cdzsize #cooler_on

                "/measurement/instrument/detector/temperature" : ["cooler_target", "int", "group", 0, ""],
                "/process/acquisition/dark_fields/dark_num_avg_of" : ["dark_num_avg_of", "int", "group", 0, ""],

                "/process/acquisition/flat_fields/i0cycle" : ["i0cycle", "int", "group", 0, ""],
                "/process/acquisition/flat_fields/i0_move_x": ["i0hmove", "int", "group", 0, ""],
                "/process/acquisition/flat_fields/i0_move_y": ["i0vmove", "int", "group", 0, ""],

                "/measurement/instrument/detection_system/objective/camera_objective" : ["lens_name", "str", "group", 0, ""],
                "/process/acquisition/rotation/multiRev": ["multiRev", "int", "group", 0, ""],
                "/process/acquisition/rotation/num_angles": ["nangles", "int", "group", 0, ""],
                "/process/acquisition/rotation/nhalfCir": ["nhalfCir", "int", "group", 0, ""],

                "/measurement/instrument/detector/dimension_x": ["nrays", "int", "group", 0, ""],
                "/measurement/instrument/detector/dimension_y": ["nslices", "int", "group", 0, ""],
                "/process/acquisition/flat_fields/num_flat_fields": ["num_bright_field", "int", "group", 0, ""],
                "/process/acquisition/dark_fields/num_dark_fields": ["num_dark_fields", "int", "group", 0, ""],

                "/measurement/instrument/detector/exposure_time": ["obstime", "float", "group", 1, ""],
                "/measurement/instrument/detection_system/name": ["optics_type", "str", "group", 0, ""],

                "/measurement/instrument/detector/delay_time": ["postImageDelay", "int", "group", 0, ""],
                "/process/acquisition/name" : ["projection_mode", "str", "group", 0, ""],
                "/measurement/instrument/detector/pixel_size": ["pxsize", "float", "group", 0, ""],
                "/measurement/instrument/source/source_name": ["scanner", "str", "group", 0, ""],
                "/measurement/instrument/detection_system/scintillator/scintillator_type": ["scintillator_name", "str", "group", 0, ""],
                "/measurement/instrument/source/current": ["scurrent", "float", "group", 1, ""],

                "/process/acquisition/mosaic/tile_xmovedist": ["tile_xmovedist", "float", "group", 0, ""],
                "/process/acquisition/mosaic/tile_xnumimg": ["tile_xnumimg", "int", "group", 0, ""],
                "/process/acquisition/mosaic/tile_xorig": ["tile_xorig", "float", "group", 0, ""],
                "/process/acquisition/mosaic/tile_xoverlap": ["tile_xoverlap", "int", "group", 0, ""],

                "/process/acquisition/mosaic/tile_ymovedist": ["tile_ymovedist", "float", "group", 0, ""],
                "/process/acquisition/mosaic/tile_ynumimg": ["tile_ynumimg", "int", "group", 0, ""],
                "/process/acquisition/mosaic/tile_yorig": ["tile_yorig", "float", "group", 0, ""],
                "/process/acquisition/mosaic/tile_yoverlap": ["tile_yoverlap", "int", "group", 0, ""],

                "/measurement/instrument/camera_motor_stack/setup/tilt_motor": ["tilt", "float", "group", 0, ""],

                "/measurement/instrument/monochromator/setup/turret1": ["turret1", "float", "group", 1, ""],
                "/measurement/instrument/monochromator/setup/turret2": ["turret2", "float", "group", 1, ""],

                "/process/acquisition/flat_fields/usebrightexpose": ["usebrightexpose", "int", "group", 0, ""],
                "/measurement/instrument/detector/binning_x": ["xbin", "int", "group", 0, ""],
                "/measurement/instrument/detector/binning_y": ["ybin", "int", "group", 0, ""],
                
                # reconstruction parameters sent in by users
                "/process/tomo_rec/setup/algorithm/phase_filt": ["phase_filt", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/remove_outliers": ["remove_outliers", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/simple_ring_removal": ["simple_ring_removal", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/normalize_by_ROI": ["normalize_by_ROI", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/reconstruction_type": ["Reconstruction_Type", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/ring_removal_method": ["ring_removal_method", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/distance": ["distance", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/beta": ["beta", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/delta": ["delta", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/radius": ["radius", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/threshold": ["threshold", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/kernel_size": ["kernel_size", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/exclude_selected_projections": ["exclude_selected_projections", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/normalization_ROI_left": ["normalization_ROI_left", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/normalization_ROI_right": ["normalization_ROI_right", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/normalization_ROI_top": ["normalization_ROI_top", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/normalization_ROI_bottom": ["normalization_ROI_bottom", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/output_type": ["output_type", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/output_scaling_min_value": ["output_scaling_min_value", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/output_scaling_max_value": ["output_scaling_max_value", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/low_ring_value": ["low_ring_value", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/upp_ring_value": ["upp_ring_value", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/ring_threshold": ["ring_threshold", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/max_ring_size": ["max_ring_size", "str", "group", 0, ""],
                "/process/tomo_rec/setup/algorithm/max_arc_length":  ["max_arc_length", "str", "group", 0, ""],
}

def convert(entry, value, new_run=None, m=None):
    ret_value = None

    try:
        if entry[1] == "str": ret_value = np.string_(value)
    except:
         print("Could not parse value", value);
         ret_value = value

    try:
        if entry[1] == "float": ret_value = float(value)
    except:
        ret_value = 0.0

    try:
        if entry[1] == "int": ret_value = float(value)
    except:
        ret_value = 0

    if entry[1] == "timestamp": 
        local.localize(datetime.datetime.strptime(value, "%m-%d-%Y %H:%M:%S.%f"), is_dst=None).timestamp()

    try:
        if entry[1] == "timestamp": ret_value = datetime.fromisoformat(value).timestamp()
    except:
         print("timestamp did not convert")
         ret_value = 0


    if entry[1] == "str":
       if new_run is not None or m is not None:
           new_run[m] = ret_value
       return ret_value

    value = ret_value
    if new_run is not None or m is not None:
        value = new_run.create_dataset(m, (1,), float)
        value[0] = ret_value

    return value

def notify_new_file(filename, full_path):
    data = {
        "name": filename,
        "current_status": "started file creation",
        "full_path": full_path
    }
    try:
        requests.post("http://status_service:8000/entry_status/", json=data)
    except:
          logger.error("Error updating status service that file is new")


def notify_status_complete(filename):
    try:
        requests.patch(f"http://status_service:8000/entry_status/{filename}", json={"new_status": "file created"})
    except:
        logger.error("Error updating status service that file is complete")

MOD_BY = 0
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


class BCSFinishCodes(IntEnum):
    USER_ABORTED = 3
class DXWriter:
    def __init__(self, notify_workflow=True):
        self.active_filename = ""
        self.active_size_estimate = 0
        self.active_params = ""
        self.active_proposal = ""
        self.notify_workflow = notify_workflow
        self.write_times = []
        self.index = 0


    def parse_dataset_name(self, dataset_name):
        # I think this means we get a windows path and parse it for thinks like user name
        logger.debug(f"dataset name is {dataset_name}")
        components = dataset_name.split("\\")
        drive_name = components[0] if len(components) > 0 else "unknown"
        user_name = components[1] if len(components) > 1 else "unknownUser"
        sub_dir_name = components[2] if len(components) > 2 else user_name
        dataset_name = sub_dir_name # components[-1]
        num_groups = len(components)-3 if len(components) >= 4 else 1
        groups = []
        dataset_location = ""

        for tt in range(2, len(components)-1):
            groups.append(components[tt])
            dataset_location += components[tt]

            if tt < len(components)-2:
                dataset_location += "/"

    
        if len(groups) == 1:
            dataset_location = groups[0]
        if len(groups) == 2:
            dataset_location = groups[1]

        result = {}

        result["drive_name"] = drive_name
        result["user_name"] = user_name
        result["sub_dir_name"] = sub_dir_name
        result["dataset_name"] = dataset_name
        result["groups"] = groups
        result["dataset_location"] = dataset_location

        return result

    def update_workflow_and_close(self, action=0):
        logger.debug("creating done file")
        if len(self.active_filename) == 0:
            return

        basefile = os.path.basename(self.active_filename)

        if basefile.startswith("t_"):
            self.active_filename = ""
            self.active_size_estimate = 0
            self.index = 0
            return

        logger.debug(f"Done file called {action}")
        file_size_error = False
        try:
            file_size = os.path.getsize(self.active_filename)
            logger.debug(f"File size estimate for: {self.active_filename}  is {file_size} {self.active_size_estimate}")
            file_size_error = False

            if file_size < self.active_size_estimate:
                file_size_error = True
        except:
            file_size_error = True
            pass
    
        basefile = os.path.basename(self.active_filename)
        notify_status_complete(basefile)

        if not self.notify_workflow:
            return

        

    def close_active_file(self):

        self.active_filename = ""
        self.active_size_estimate = 0
        self.active_params = ""
        self.active_proposal = ""
        self.index = 0
    
    def finalize(self, data_obj):
        logger.info(f"finalizing file {self.active_filename}")
        try:
            image = data_obj["image"]
            image_size = int.from_bytes(image[0:4], byteorder='big')
            image = np.frombuffer(image[4:], dtype=np.dtype(np.uint16).newbyteorder('>'))
            self.update_workflow_and_close(image[0])
        except:
            self.update_workflow_and_close()

    def setup_dx_file(self, h5_file, h5_file_path, dataset_path_metadata, bcs_params):
        h5_dirname = os.path.dirname(h5_file)
        if not os.path.exists(h5_dirname):
            logger.debug(f"Creating folder: {h5_dirname}")
            os.makedirs(h5_dirname)

        # if for whatever reason last file was not emitted,
        # finalize it now
        self.update_workflow_and_close()

        self.active_filename = h5_file_path
        self.active_size_estimate = 0
        logger.info(f"Creating file, {h5_file}")

        new_run = h5py.File(h5_file, "w")
        #group = new_run.create_group(result["dataset_location"])
        new_run["implements"] = convert_2_string_list("exchange:measurement:processbrea")

        exchange_group = new_run.create_group("exchange")
        measurement_group = new_run.create_group("measurement")
        process_group = new_run.create_group("process")

        
        ### GATHER METADATA
        try:
            sdate = bcs_params["sdate"]
        except KeyError:
            sdate = None
            logger.info(f"couldn't get metadata, was the LabViuewsystem restarted recently? params: {bcs_params}")
        new_attrs = {}
        group_attrs = {}
        new_attrs["dataset"] = convert_2_string_list(dataset_path_metadata["dataset_name"])
        new_attrs["end_station"] = convert_2_string_list("bl832")
        new_attrs["facility"] = convert_2_string_list("als")
        new_attrs["owner"] = convert_2_string_list(dataset_path_metadata["user_name"])
        new_attrs["stage"] = convert_2_string_list("raw")
        # new_attrs["stage_date"] = convert_2_string_list(local_date)
        new_attrs["sdate"] = convert_2_string_list(sdate)
        new_attrs["stage_flow"] = convert_2_string_list("/raw")
        new_attrs["stage_version"] = convert_2_string_list("bl832-1.0.0")
        new_attrs["uuid"] = convert_2_string_list(str(uuid.uuid4()))
        # new_run.attrs["test"] = "test" 
        # add all parameters to group
        #test_groups = result["dataset_location"].split("/")
   
        # for param in bcs_params:
        #     logger.info(param)
        #     param = param.strip()
        #     logger.info(param)
        #     if len(param) == 0:
        #         continue

        #     split_index = param.find(" ")

        #     if split_index > 0:
        #         key = param[1:split_index]
        #         value = param[split_index+1:]
        #         logger.info(convert_2_string_list(value))
        #         group_attrs[key] = convert_2_string_list(value)

        
        new_run_dict = {}
        group_dict = {}

        for i in new_attrs:
            new_run_dict[i] = new_attrs[i].decode()
        for key in bcs_params.keys():
            group_dict[key] = bcs_params[key].decode()

        
        # logger.debug(f"group_dict {group_dict}")
        self.active_size_estimate = int(group_dict['nrays'])*int(group_dict['nslices'])*int(group_dict['nangles'])*2
        # json_file_attrs = json.dumps(new_run_dict)
        # json_group_attrs = json.dumps(group_dict)
        
        new_run["process/acquisition/plan_args"] = convert_2_string_list(self.active_params)
        new_run["measurement/sample/experiment/raw_text"] = convert_2_string_list(self.active_proposal)
        name = "Unknown"
        email = "Unknown"
        proposal = ""
        pi = ""
        exp_lead = ""
        beamline = ""
        abstract = ""
        title = ""

        try:
            prop = "".join([c.decode() for c in list(new_run["/measurement/sample/experiment/raw_text"])]) 
            root = etree.fromstring('<lee>' + prop + '</lee>')
            email = root[0][2][1].text
            first_name = root[0][3][1].text
            middle_name = root[0][4][1].text
            last_name = root[0][5][1].text
            # name = root[0][3][1].text  + ", "  + root[0][4][1].text + ", " + root[0][5][1].text
            name = first_name + ", "
            if middle_name:
                name += middle_name + ", "
            if last_name:
                name += last_name
            proposal = root[1][2][1].text
            pi = root[1][3][1].text
            exp_lead = root[1][4][1].text
            beamline = root[1][5][1].text
            abstract = root[1][6][1].text
            title = root[1][8][1].text
        except:
            logger.debug(f"Parsing Raw Text Failed {self.active_proposal}")

        new_run["measurement/sample/experiment/title"] = convert_2_string_list(title)
        new_run["measurement/sample/experimenter/email"] = convert_2_string_list(email)
        new_run["measurement/sample/experimenter/name"] = convert_2_string_list(name)

        new_run["measurement/sample/experiment/proposal"] = convert_2_string_list(proposal)
        new_run["measurement/sample/experiment/pi"] = convert_2_string_list(pi)

        new_run["measurement/sample/experiment/experiment_lead"] = convert_2_string_list(exp_lead)
        new_run["measurement/sample/experiment/beamline"] = convert_2_string_list(beamline)
        new_run["measurement/sample/experiment/abstract"] = convert_2_string_list(abstract)

        #EXCHANGE_GROUP
        exchange_group["name"] = convert_2_string_list("tomography")
        exchange_group["description"] = convert_2_string_list("raw tomography")

        num_nangles = int(group_dict['nangles']) - MOD_BY
        data = exchange_group.create_dataset("data", (num_nangles, int(group_dict['nslices']), int(group_dict['nrays'])), np.uint16)
        theta = exchange_group.create_dataset("theta", (num_nangles,), float)

        data_dark = exchange_group.create_dataset("data_dark", (int(group_dict['num_dark_fields']), int(group_dict['nslices']), int(group_dict['nrays'])), np.uint16)
        theta_dark = exchange_group.create_dataset("theta_dark", (int(group_dict['num_dark_fields']),), float)

        data_white = exchange_group.create_dataset("data_white", (int(group_dict['BFI_Count']), int(group_dict['nslices']), int(group_dict['nrays'])), np.uint16)
        theta_white = exchange_group.create_dataset("theta_white", (int(group_dict['BFI_Count']),), float)

        data.attrs["axes"] = convert_to_string("theta:y:x")
        theta.attrs["units"] = convert_to_string("deg")
        data_dark.attrs["axes"] = convert_to_string("theta_dark:y:x")
        theta_dark.attrs["units"] = convert_to_string("deg")
        data_white.attrs["axes"] = convert_to_string("theta_white:y:x")
        theta_white.attrs["units"] = convert_to_string("deg")

        num_nangles = int(group_dict['nangles']) - MOD_BY
        num_darks = int(group_dict['num_dark_fields'])
        num_bfi_count = int(group_dict['BFI_Count'])

        logger.debug(f"COUNTS {num_bfi_count} {num_darks} {num_nangles} slices, rays {int(group_dict['nslices'])}  {int(group_dict['nrays'])}")

        #image_type = new_run.create_dataset("measurement/instrument/image_type", (num_nangles+num_darks+num_bfi_count,), 'S20')
        image_dates = new_run.create_dataset("process/acquisition/image_date", (num_nangles,), float)
        dark_dates = new_run.create_dataset("process/acquisition/dark_date", (int(group_dict['num_dark_fields']),), float)
        bright_dates = new_run.create_dataset("process/acquisition/bak_date", (int(group_dict['BFI_Count']),), float)
        timestamps = new_run.create_dataset("measurement/instrument/time_stamp",  (num_nangles,), int)
        for e in new_run_dict:
            new_run["/defaults/file_attrs/" + e] = convert_2_string_list(new_run_dict[e])

        # acquisition_start_date = new_run.create_dataset("process/acquisition/start_date", (1,), float)
        # acquisition_start_date[0] = bcs_date_2_dt(new_attrs["sdate"]).timestamp()
        for m in map_dict:
            entry = map_dict[m]

            try:
                if entry[2] == "group" and entry[3] == 1:
                    dataset = new_run.create_dataset(m, (num_nangles+num_darks+num_bfi_count,), float)
                
                if entry[2] == "file" or (entry[2] == "group" and entry[3] == 0):
                    value = group_dict[entry[0]] if entry[2] == "group" else new_run_dict[entry[0]]
                    #new_run[m] = convert(entry, value, new_run, m)
                    convert(entry, value, new_run, m)
                    if len(entry[4]) > 0: new_run[m].attrs["units"] = convert_to_string(entry[4])
            except:
                logger.info(f"{m}, was not mapped  {entry[0]}")
        
        basefile = os.path.basename(self.active_filename)
        if not basefile.startswith("t_"):
            try:
                notify_new_file(basefile, self.active_filename)
            except Exception as e:
                logger.exception("Unable to update remote transfer")
        return new_run
    
    
    def process(self, data_obj):
        image = data_obj["image"]
        info = data_obj["info"]
        h5_file = data_obj["h5_file"]
        tif_file = data_obj["tif_file"]
        bcs_params = data_obj["params"]
        h5_file_orig = h5_file.decode()
        tif_file_orig = tif_file.decode()
        h5_file = h5_file_orig.replace('\\', '/').replace(ROOT_MAP["source"], ROOT_MAP["dest"])
        tif_file = tif_file_orig.replace('\\','/').replace(ROOT_MAP["source"], ROOT_MAP["dest"])
        bcs_params = [param.decode() for param in bcs_params.split(b'\r\n')]           
        #info_size = int.from_bytes(info[0:4], byteorder='big')  # unused
        #info = np.frombuffer(info[4:], dtype=np.dtype(np.uint32).newbyteorder('>'))
        #image_size = int.from_bytes(image[0:4], byteorder='big') # unused
        #image = np.frombuffer(image[4:], dtype=np.dtype(np.uint16).newbyteorder('>'))
        #image = image.reshape((info[0], info[1]))

        """ reshape image to proper shape """
        #image = image.reshape((1, info[1], info[0]))

        dataset_path_metadata = self.parse_dataset_name(tif_file_orig)
        logger.debug(f"dataset_path_metadata: {dataset_path_metadata}")
        # {'drive_name': 'R:',
        # 'user_name': 'DD-00639_STrovati', 
        # 'sub_dir_name': 't_20220929_090854_dylan', 
        # 'dataset_name': 't_20220929_090854_dylan', 
        # 'groups': ['t_20220929_090854_dylan'], 
        # 'dataset_location': 't_20220929_090854_dylan'}
        #OVERRIDE LOGIC
        h5_file = os.path.join(os.path.join(ROOT_MAP["dest"], dataset_path_metadata["user_name"]), os.path.basename(h5_file))

        h5_file_path = h5_file
        h5_file = Path(h5_file)
        tif_node_filename = os.path.basename(tif_file)


        """if path does not exist create one"""
        logger.debug(f"{h5_file}, {tif_node_filename}, {h5_file.exists()}")

        if tif_node_filename.find("placeHolder.tif") >= 0:
            if h5_file.exists():
                os.remove(h5_file_path)
            if tif_file.find("proposal") >= 0:
                # logger.debug(f"Proposal identified {bcs_params}")
                self.active_proposal = bcs_params
            else: 
                logger.info("Scan settings identified")
                self.active_params = bcs_params
            return

        keys = []
        if tif_node_filename.find("image_key.tif") >= 0:
            # when the image tag contains the special file name 'image_key.tif',
            # it means that this message is an Image Key, which will be placed
            # in the DX file in a special place.
            # ImageKey comes in as an bytes object, whose values are individual
            # ASCII characters, one per image (0=projection, 1=flat, 2=dark, 3=invalid) 
            # that indicates the type of image that we have. This will get written as
            # an array the DX file
            for character in bcs_params[0]: # not even sure why this comes in as a list
                # append int version of ascii character
                keys.append(15 & int(character))
        image_key = np.array(keys, dtype=np.uint8)
        dataset_attrs = bcs_params_2_dict(bcs_params)
        if not h5_file.exists():
            dx_file = self.setup_dx_file(h5_file, h5_file_path, dataset_path_metadata, dataset_attrs)
        else:
            dx_file = h5py.File(h5_file, "a")
            #group = new_run[result["dataset_location"]]
        exchange_group = dx_file["exchange"]

        """ Write image_key """
        if len(image_key) > 0:
            logger.debug(f"image_key found with length {len(image_key)}")
            exchange_group.create_dataset("image_key", data=image_key)
            return

        """ write attributes per image """

        dataset_attrs["dim1"] = 1
        dataset_attrs["dim2"] = info[1]
        dataset_attrs["dim3"] = info[0]
        dataset_attrs["errorFlag"] = 0

        for e in dataset_attrs:
            dx_file["/defaults/group_attrs/metadata/" + str(info[-1]) + "/" + e] = convert_2_string_list(dataset_attrs[e])

        data = exchange_group["data"]
        theta = exchange_group["theta"]

        data_dark = exchange_group["data_dark"]
        theta_dark = exchange_group["theta_dark"]

        data_white = exchange_group["data_white"]
        theta_white = exchange_group["theta_white"]

        # tif node looks like 
        #   background frame '20230206_145610_test_pipelinebak_0000_0000'
        #   dark frame
        #   data frame '20230206_145610_test_pipeline_x00y00_0000_0000.tif'

        tiff_node_tokenized = tif_node_filename.split("_")
   
        self.index = int(tiff_node_tokenized[-1].replace(".tif", ""))

        image_dates = dx_file["process/acquisition/image_date"]
        dark_dates = dx_file["process/acquisition/dark_date"]
        bright_dates = dx_file["process/acquisition/bak_date"]
        sdate_dt = bcs_date_2_dt(dataset_attrs["sdate"])

        is_background_frame = tif_node_filename.find("bak")   
        is_dark_frame = tif_node_filename.find("drk") 

        for e in dataset_attrs:
            if is_background_frame > 0:
                dx_file["/defaults/group_attrs/bak/" + str(tiff_node_tokenized[-2]) + "/" + e] = dataset_attrs[e]
            elif is_dark_frame > 0:
                dx_file["/defaults/group_attrs/drk/" + str(tiff_node_tokenized[-2]) + "/" + e] = dataset_attrs[e]
            else:
                dx_file["/defaults/group_attrs/prj/" + str(self.index)  + "/" + e] = dataset_attrs[e]

        # logger.info(f" {tif_node_filename}  index {self.index}   {tiff_node_tokenized[-2]}")
        if is_background_frame > 0:
           # the file had the string "bak" in it...what if the user adds that to the name???
           index = int(tiff_node_tokenized[-2])
           data_white[index] = image
           theta_white[index] = float(dataset_attrs["rot_angle"])
           bright_dates[index] = sdate_dt.timestamp()
           #new_run["measurement/instrument/image_type"][int(info[-1])] = convert_2_string_list("flats")
        elif is_dark_frame > 0:
           
           index = int(tiff_node_tokenized[-2])
           data_dark[index] = image
           theta_dark[index] = float(dataset_attrs["rot_angle"])
           dark_dates[index] = sdate_dt.timestamp()
          
           #new_run["measurement/instrument/image_type"][int(info[-1])] = convert_2_string_list("darks")
        else:
           
           index = self.index - MOD_BY
           start_time = time.perf_counter()
           data[index] = image
           theta[index] = float(dataset_attrs["rot_angle"])
           image_dates[index] = sdate_dt.timestamp()
           end_time = time.perf_counter()
           elapsed = end_time - start_time
           kbytes_per_second = (image.nbytes / elapsed ) / 1024
        #    logger.info(f"frame #{index} {image.nbytes} bytes in {elapsed: .3f} secoonds  {kbytes_per_second: .3f} kbytes_per_second")
           self.write_times.append(kbytes_per_second)
           #new_run["measurement/instrument/image_type"][int(info[-1])] = convert_2_string_list("projections")        
        dx_file["measurement/instrument/time_stamp"][self.index] = sdate_dt.timestamp()
        for m in map_dict:
            entry = map_dict[m]

            try:
                if (entry[2] == "group" and entry[3] == 1):
                    value = dataset_attrs[entry[0]]

                    # print(m, m in new_run, entry, value)

                    dx_file[m][int(info[-1])] = convert(entry, value)
                    if len(entry[4]) > 0: dx_file[m].attrs["units"] = convert_to_string(entry[4])
            except:
                 logger.error(f"{m}, was not written {entry[0]}, {entry}, {m}")

        dx_file.close()

        # change the mode of the file
        try:
            os.chmod(h5_file_path, 0o770)
        except:
            logger.error(f"Failed to chmod {h5_file_path}")



async def prefect_start_flow(prefect_api_url, deployment_name, file_path, api_key=None):
    client = OrionClient(prefect_api_url, api_key=api_key)
    deployment = await client.read_deployment_by_name(deployment_name)
    flow_run = await client.create_flow_run_from_deployment(
        deployment.id,
        name=os.path.basename(file_path),
        parameters={"file_path": file_path},
    )
    return flow_run

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
            
            # print(f"#### {data_obj}")


if __name__ == "__main__":
    typer.run(main)
