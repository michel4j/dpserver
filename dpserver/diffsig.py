import time
import base64
import uuid
import subprocess
from pathlib import Path
from multiprocessing import Queue

from typing import Any, Union

import numpy
from mxio import ImageFrame, DataSet
from mxio.formats import eiger, cbf


from dpserver import parser
from szrpc.log import get_module_logger

SAVE_DELAY = 0.1
RETRY_TIMEOUT = 15
D_MAX = 50

logger = get_module_logger('worker')

numpy.errstate(invalid='ignore', divide='ignore')


def short_uuid():
    """
    Generate a 22 character UUID4 representation
    """
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).strip(b'=')


DISTL_SPECS = {
    "root": {
        "sections": {
            "summary": {
                "fields": [
                    "Spot Total : <int:total_spots>",
                    "Remove Ice : <int:bragg_spots>",
                    "In-Resolution Total : <int:resolution_spots>",
                    "Good Bragg Candidates : <int:bragg_spots>",
                    "Ice Rings : <int:ice_rings>",
                    "Method 2 Resolution : <float:alt_resolution>",
                    "Method 1 Resolution : <float:resolution>",
                    "Maximum unit cell : <float:max_cell>",
                    "Saturation, Top <int:top_saturation> Peaks : <float:top_saturation>",
                    "Signals range from <float:signal_min> to <float:signal_max> with mean integrated signal <float:signal_avg>",
                ]
            }
        }
    }
}

DOZOR_DATA = """!
detector {detector}
library /cmcf_apps/xtal/dozor/xds-zcbf.so
nx {x_size}
ny {y_size}
pixel {pixel_size:0.4f}
exposure {exposure:0.4f}
spot_size 2
spot_level 3
detector_distance {distance:0.3f}
X-ray_wavelength {wavelength:0.4f}
fraction_polarization 0.990
pixel_min 3
pixel_max {count_cutoff}
orgx {x_center}
orgy {y_center}
oscillation_range {delta_angle:0.4f}
image_step 1
starting_angle {start_angle:0.4f}
first_image_number {index}
number_images 1
name_template_image {name_template}
end
"""

DOZOR_ENTRY = "<int:index> | <int:bragg_spots> <float:score> <float:resolution> <float:avg_signal>"

DOZOR_OUTPUT = {
    "root": {
        "sections": {
            "summary": {
                "fields": [DOZOR_ENTRY]
            }
        }
    }
}


def wait_for_file(filename: Union[str, Path], after: float = 1.0, timeout: float = RETRY_TIMEOUT) -> bool:
    """
    Wait until a file is not modified for `after` seconds
    :param filename: File name
    :param after: Duration after any modifications to return
    :param timeout: maximum time to wait
    :return: Boolean, True if the wait was successful
    """
    path = Path(filename)
    end_time = time.time() + timeout
    while time.time() < end_time:
        if path.exists() and time.time() - path.stat().st_mtime > after:
            logger.debug(f'File {path} is done writing ..')
            break
        time.sleep(0.01)
    else:
        if not path.exists():
            logger.warning(f'File {path} does not exist ..')
        elif time.time() - path.stat().st_mtime < after:
            logger.warning(f'File {path} is still being written to, after {timeout} seconds ..')
        return False
    return True


def calc_score(record):
    scale = 1e-6
    base = record.bragg_spots * record.avg_intensity * record.avg_snr
    penalty = 1.0 - 0.5 * record.num_ice_rings/6.0
    return base * penalty * scale


def frame_signal(frame: ImageFrame, index: int) -> dict:
    """
    Perform signal strength analysis on a file
    :param frame: mxio ImageFrame
    :param index: frame index
    :return: Dictionary of results
    """
    from mxspots import scorer
    from mxspots.models import SpotParams

    result = {
        'ice_rings': 0, 'resolution': 50, 'total_spots': 0, 'bragg_spots': 0, 'signal_avg': 0, 'signal_min': 0,
        'signal_max': 0, 'frame_number': index, 'score': 0.0
    }

    start_time = time.time()
    frame_score = scorer.score(frame, SpotParams(snr_threshold=5, ice_sensitivity=0.5, d_min=4.0))
    duration = time.time() - start_time
    score = calc_score(frame_score)
    result.update({
        'frame_number': index,
        'score': score,
        'duration': duration,
        'total_spots': frame_score.spot_count,
        'bragg_spots': frame_score.bragg_spots,
        'signal_avg': frame_score.avg_snr,
        'resolution': frame_score.d_min,
        'ice_rings': frame_score.num_ice_rings,
        'signal_min': 0,
        'signal_max': frame_score.avg_intensity,
    })

    return result


def file_signal(frame_path: str, index: int) -> dict:
    """
    Perform signal strength analysis on a file
    :param frame_path: full path to file
    :param index: frame index
    :return: Dictionary of results
    """

    frame = Path(frame_path)
    success = wait_for_file(frame_path)
    if success:
        dataset = DataSet.new_from_file(frame)
        results = frame_signal(dataset.frame, index)

        if frame_path.startswith('/dev/shm/'):
            frame.unlink(missing_ok=True)
        return results

    return {
        'ice_rings': 0, 'resolution': 50, 'total_spots': 0, 'bragg_spots': 0, 'signal_avg': 0, 'signal_min': 0,
        'signal_max': 0, 'frame_number': index, 'score': 0.0, 'duration': 0.0
    }


def stream_signal(frame_data: Any) -> dict:
    """
    Perform signal strength analysis on a in-memory data
    :param frame_data: Eiger stream data
    :return: dictionary of results
    """

    header, data = frame_data
    dataset = eiger.EigerStream()
    dataset.parse_header(header)
    dataset.parse_image(data)
    return frame_signal(dataset.frame, dataset.index)


def distl_signal(frame_path: str, index: int) -> dict:
    """
    Perform signal strength analysis on a file
    :param frame_path: full path to file
    :param index: frame index
    :return: Dictionary of results
    """
    frame = Path(frame_path)
    result = {
        'ice_rings': 0, 'resolution': 50, 'total_spots': 0, 'bragg_spots': 0, 'signal_avg': 0, 'signal_min': 0,
        'signal_max': 0, 'frame_number': index, 'score': 0.0, 'duration': 0.0
    }
    success = wait_for_file(frame_path)
    if success:
        start_time = time.time()
        args = ['distl.signal_strength', 'distl.res.outer=1.5', str(frame)]
        output = subprocess.check_output(args, stderr=subprocess.STDOUT)
        info = parser.parse_text(output.decode('utf-8'), DISTL_SPECS)['summary']
        info['frame_number'] = index
        info['score'] = info['bragg_spots']
        info['duration'] = (time.time() - start_time)*1000
        if frame_path.startswith('/dev/shm/'):
            frame.unlink(missing_ok=True)
        result.update(info)
    return result


def dozor_signal(frame_path: str, index: int) -> dict:
    """
    Perform signal strength analysis on a file
    :param frame_path: full path to file
    :param index: frame index
    :return: Dictionary of results
    """
    frame = Path(frame_path)
    result = {
        'ice_rings': 0, 'resolution': 50, 'total_spots': 0, 'bragg_spots': 0, 'signal_avg': 0, 'signal_min': 0,
        'signal_max': 0, 'frame_number': index, 'score': 0.0
    }
    success = wait_for_file(frame_path)
    if success:
        start_time = time.time()
        dat_file = Path('/dev/shm') / Path(frame_path).with_suffix('.dat').name
        dset = DataSet.new_from_file(frame)
        detector = dset.frame.detector.replace('Dectris', '').replace(' ', '').strip().lower()

        with open(dat_file, 'w') as handle:
            handle.write(DOZOR_DATA.format(
                detector=detector,
                x_size=dset.frame.size.x,
                y_size=dset.frame.size.y,
                pixel_size=dset.frame.pixel_size.x,
                exposure=dset.frame.exposure,
                distance=dset.frame.distance,
                wavelength=dset.frame.wavelength,
                count_cutoff=dset.frame.cutoff_value,
                x_center=dset.frame.center.x,
                y_center=dset.frame.center.y,
                delta_angle=dset.frame.delta_angle,
                start_angle=dset.frame.start_angle,
                index=dset.index,
                name_template=str(dset.directory / dset.glob)
            ))

        args = ['dozor', str(dat_file)]
        output = subprocess.check_output(args, stderr=subprocess.STDOUT)
        info = parser.parse_text(output.decode('utf-8'), DOZOR_OUTPUT)['summary']
        info['frame_number'] = dset.index
        info['duration'] = 1000*(time.time() - start_time)
        for path in [frame, dat_file]:
            if str(path).startswith('/dev/shm/'):
                path.unlink(missing_ok=True)
        result.update(info)
    return result


def stream_dozor_signal(frame_data: Any) -> dict:
    """
    Perform signal strength analysis on a in-memory data
    :param frame_data: Eiger stream data
    :return: dictionary of results
    """
    header, data = frame_data
    result = {
        'ice_rings': 0, 'resolution': 50, 'total_spots': 0, 'bragg_spots': 0, 'signal_avg': 0, 'signal_min': 0,
        'signal_max': 0, 'frame_number': 1, 'score': 0.0
    }
    dataset = eiger.EigerStream()
    dataset.parse_header(header)
    dataset.parse_image(data)

    frame_path = Path('/dev/shm') / f'{dataset.name}_{dataset.index:06d}.cbf'
    cbf.CBFDataSet.save_frame(frame_path, dataset.frame)
    info = dozor_signal(str(frame_path), dataset.index)
    result.update(info)
    return result


def signal_worker(tasks: Queue, results: Queue):
    """
    Signal strength worker. Reads data from the inbox queue and puts the results to the outbox
    :param tasks: Inbox queue to fetch tasks
    :param results: Outbox queue to place completed results
    """

    num_tasks = 0
    work_time = 0

    for task in iter(tasks.get, 'STOP'):
        if task == 'STOP':
            tasks.task_done()
            break

        num_tasks += 1
        t = time.time()
        kind, index, frame_data = task

        result = {
            'ice_rings': 0,
            'resolution': 50,
            'total_spots': 0,
            'bragg_spots': 0,
            'signal_avg': 0,
            'signal_min': 0,
            'signal_max': 0,
            'frame_number': index,
            'score': 0.0
        }

        try:
            if kind == 'stream':
                result = stream_signal(frame_data)
            elif kind == 'file':
                frame_path = frame_data
                result = file_signal(frame_path, index)
            logger.debug(f'Raster: processed frame #{index} in {result.get('duration', 0.0):0.2f} sec')
        except Exception as err:
            logger.error(err)

        results.put(result)
        work_time += time.time() - t
        tasks.task_done()
        time.sleep(0)

    ips = 0.0 if work_time == 0 else num_tasks / work_time
    logger.info(f'Worker completed {num_tasks}: {ips:0.1f} fps')
