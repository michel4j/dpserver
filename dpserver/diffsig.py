import time
import base64
import uuid
import subprocess
from pathlib import Path
from multiprocessing import Queue


from typing import Any

import numpy
from scipy.signal import find_peaks
from mxio import ImageFrame, read_image, DataSet
from mxio.formats import eiger

from scipy.ndimage.filters import maximum_filter, uniform_filter, gaussian_filter1d
from scipy.ndimage.interpolation import zoom
from scipy.ndimage.morphology import generate_binary_structure, binary_erosion
from skimage import filters

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


def window_stdev(X, window_size):
    c1 = uniform_filter(X, window_size, mode='reflect')
    c2 = uniform_filter(X * X, window_size, mode='reflect')
    return numpy.sqrt(numpy.abs(c2 - c1 * c1))


def detect_peaks(image):
    """
    Takes an image and detect the peaks using the local maximum filter.
    Returns a boolean mask of the peaks (i.e. 1 when
    the pixel's value is the neighborhood maximum, 0 otherwise)
    """

    # define an 8-connected neighborhood
    # apply the local maximum filter; all pixel of maximal value
    # in their neighborhood are set to 1
    # local_max is a mask that contains the peaks we are
    # looking for, but also the background.
    # In order to isolate the peaks we must remove the background from the mask.
    # we create the mask of the background
    # a little technicality: we must erode the background in order to
    # successfully subtract it from local_max, otherwise a line will
    # appear along the background border (artifact of the local maximum filter)
    # we obtain the final mask, containing only peaks,
    # by removing the background from the local_max mask (xor operation)

    neighborhood = generate_binary_structure(2, 2)
    local_max_image = maximum_filter(image, footprint=neighborhood)
    local_max = local_max_image == image
    background = (image == 0)
    eroded_background = binary_erosion(background, structure=neighborhood, border_value=1)
    detected_peaks = local_max ^ eroded_background
    peaks = numpy.argwhere(detected_peaks)
    counts = numpy.extract(detected_peaks, local_max_image)
    return peaks, counts


def remove_rings(spots, sigma=2, bins=500, min_width=None, min_height=None):
    radii = spots[:, 7]
    counts, edges = numpy.histogram(radii, bins=numpy.linspace(0, radii.max(), bins))
    radius = numpy.diff(edges) * 0.5 + edges[:-1]
    counts = gaussian_filter1d(counts.astype(float), sigma)
    peak_indices, details = find_peaks(counts, height=(min_height, None), width=(min_width, None), prominence=0.5)

    peaks = radius[peak_indices]

    mask = spots[:, 9] > 20  # ignore reflections with d-spacing higher than 20 A
    for l, r in zip(details['left_bases'], details['right_bases']):
        mask |= (radii >= radius[l]) & (radii <= radius[r])

    spots_flt = spots[~mask]
    return spots_flt[spots_flt[:, 3].argsort()][::-1], len(peaks)



def signal(frame: ImageFrame, scale: int = 1):
    # represent image as z-scores of the standard deviation
    # zero everything less than MIN_SIGMA
    # find peaks in the resulting image
    # calculate the resolution of found peaks
    # mask peaks that are within ice rings

    if scale != 1:
        image = zoom(frame.data, 1 / scale)
    else:
        image = frame.data

    cy, cx = numpy.array([frame.center.x, frame.center.y]) / scale

    std_img = window_stdev(image, 2)
    data = filters.gaussian(std_img, sigma=2)
    thresh = numpy.percentile(std_img, 98.)
    data[data < thresh] = 0.0
    peaks, counts = detect_peaks(data)
    num_spots = len(peaks)

    # initialize results
    num_rings = 0
    num_bragg = 0
    signal_avg = 0.
    signal_min = 0.
    signal_max = 0.
    resolution = 50.

    if num_spots:
        max_radius = scale * frame.pixel_size.x * min((frame.size.x / 2), (frame.size.y / 2))
        peak_radii = scale * frame.pixel_size.x * ((peaks - (cx, cy)) ** 2).sum(axis=1) ** 0.5
        peak_angle = 0.5 * numpy.arctan(peak_radii / frame.distance)
        peak_resol = (frame.wavelength / (2 * numpy.sin(peak_angle)))

        spots = numpy.array([
            (
                scale * pk[1], scale * pk[0],
                frame.start_angle + 0.5*frame.delta_angle,
                counts[i],
                0, 0, 0,
                peak_radii[i], peak_angle[i], peak_resol[i] # #7 = radius, #8 = angle, #9 = d-spacing
            )
            for i, pk in enumerate(peaks) if peak_radii[i] < max_radius and peak_resol[i] < D_MAX
        ])

        num_bragg = 0
        if len(spots):
            clean_spots, num_rings = remove_rings(spots, bins=2000, min_height=2)
            numpy.save('/tmp/spots.npy', clean_spots)
            num_bragg = len(clean_spots)

            if num_bragg:
                signal_avg = clean_spots[:50, 3].mean()
                signal_min = clean_spots[:50, 3].min()
                signal_max = clean_spots[0, 3]
                resolution = round(numpy.percentile(peak_resol, 1), 3)

    return {
        'ice_rings': num_rings,
        'resolution': resolution,
        'total_spots': num_spots,
        'bragg_spots': num_bragg,
        'signal_avg': signal_avg,
        'signal_min': signal_min,
        'signal_max': signal_max,
    }


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


def file_signal(frame_path: str, index: int) -> dict:
    """
    Perform signal strength analysis on a file
    :param frame_path: full path to file
    :param index: frame index
    :return: Dictionary of results
    """
    frame = Path(frame_path)
    end_time = time.time() + RETRY_TIMEOUT
    while not frame.exists() and time.time() < end_time:
        time.sleep(SAVE_DELAY)

    dataset = DataSet.new_from_file(frame)
    result = signal(dataset.frame, scale=4)
    result['frame_number'] = index
    if frame_path.startswith('/dev/shm/'):
        frame.unlink(missing_ok=True)

    return result


def distl_signal(frame_path: str, index: int) -> dict:
    """
    Perform signal strength analysis on a file
    :param frame_path: full path to file
    :param index: frame index
    :return: Dictionary of results
    """
    frame = Path(frame_path)
    end_time = time.time() + RETRY_TIMEOUT
    while not frame.exists() and time.time() < end_time:
        time.sleep(SAVE_DELAY)

    args = ['distl.signal_strength', 'distl.res.outer=1.5', str(frame)]
    output = subprocess.check_output(args, stderr=subprocess.STDOUT)
    result = parser.parse_text(output.decode('utf-8'), DISTL_SPECS)['summary']
    result['frame_number'] = index
    if frame_path.startswith('/dev/shm/'):
        frame.unlink(missing_ok=True)
    return result



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
    result = signal(dataset.frame, scale=4)
    result['frame_number'] = dataset.index
    return result


def signal_worker(tasks: Queue, results: Queue):
    """
    Signal strength worker. Reads data from the inbox queue and puts the results to the outbox
    :param tasks: Inbox queue to fetch tasks
    :param results: Outbox queue to place completed results
    """
    index = 0
    num_tasks = 0
    work_time = 0
    start_time = time.time()
    cleanup = []

    for task in iter(tasks.get, 'STOP'):
        if task == 'STOP':
            tasks.put(task)     # Add the sentinel back to the queue for other processes and exit
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
        }

        try:
            if kind == 'stream':
                result = stream_signal(frame_data)
            elif kind == 'file':
                frame_path = frame_data
                result = file_signal(frame_path, index)

        except Exception as err:
            logger.error(err)

        results.put(result)
        work_time += time.time() - t
        tasks.task_done()
        time.sleep(0)

    total_time = time.time() - start_time
    ips = 0.0 if work_time == 0 else num_tasks / work_time
    logger.debug(f'Worker completed {num_tasks}, {ips:0.1f} ips. Run-time: {total_time:0.0f} sec')