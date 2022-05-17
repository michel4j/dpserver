import time
import base64
import uuid
import subprocess
from pathlib import Path
from multiprocessing import Queue

import numpy
from scipy.signal import find_peaks
from mxio import read_image
from mxio.formats import eiger, cbf

from scipy.ndimage.filters import maximum_filter, uniform_filter
from scipy.ndimage.interpolation import zoom
from scipy.ndimage.morphology import generate_binary_structure, binary_erosion
from skimage import filters

from dpserver import parser
from szrpc.log import get_module_logger

SAVE_DELAY = 0.05
RETRY_TIMEOUT = 15
SCALE = 2
RING_SLICE = 0.025
MIN_RING_HEIGHT = 10
MIN_RING_PROMINENCE = 10
MIN_RING_WIDTH = 1
MAX_RING_WIDTH = int(0.1/RING_SLICE)
FACTOR = numpy.sqrt(SCALE ** 2 + SCALE ** 2)


logger = get_module_logger('worker')

numpy.errstate(invalid='ignore', divide='ignore')


def short_uuid():
    """
    Generate a 22 character UUID4 representation
    """
    return base64.b64encode(uuid.uuid4().bytes).strip(b'=')


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
    local_max = maximum_filter(image, footprint=neighborhood) == image
    background = (image == 0)
    eroded_background = binary_erosion(background, structure=neighborhood, border_value=1)
    detected_peaks = local_max ^ eroded_background
    peaks = numpy.argwhere(detected_peaks)
    return peaks


def signal(image, metadata):
    # represent image as z-scores of the standard deviation
    # zero everything less than MIN_SIGMA
    # find peaks in the resulting image
    # calculate the resolution of found peaks
    # mask peaks that are within ice rings

    if SCALE > 1:
        image = zoom(image, 1 / SCALE)
    cy, cx = numpy.array(metadata['beam_center']) / SCALE

    std_img = window_stdev(image, 2)
    data = filters.gaussian(std_img, sigma=2)
    thresh = numpy.percentile(std_img, 99.)
    data[data < thresh] = 0.0
    peaks = detect_peaks(data)
    num_spots = len(peaks)

    # initialize results
    num_rings = 0
    num_bragg = 0
    signal_avg = 0.
    signal_min = 0.
    signal_max = 0.
    resolution = 50.

    if num_spots:
        peak_l = SCALE * metadata['pixel_size'] * ((peaks - (cx, cy)) ** 2).sum(axis=1) ** 0.5
        peak_a = 0.5 * numpy.arctan(peak_l / metadata['distance'])
        peak_d = metadata['wavelength'] / (2 * numpy.sin(peak_a))

        spots = numpy.array([
            (SCALE * pk[1], SCALE * pk[0], metadata['frame_number'], std_img[pk[0], pk[1]], 0, 0, 0)
            for i, pk in enumerate(peaks)
        ])

        peak_shell = numpy.round(peak_d/RING_SLICE)*RING_SLICE
        d_shells = numpy.arange(1, 5, RING_SLICE)  # numpy.unique(peak_shell)

        rings = []
        for i, shell in enumerate(d_shells):
            sel = numpy.abs(peak_shell - shell) <= 1.01 * RING_SLICE
            ss = int(sel.sum())
            rings.append((shell, ss))

        rings = numpy.array(rings)
        ice_rings, ring_props = find_peaks(
            rings[:, 1],
            height=MIN_RING_HEIGHT,
            prominence=(MIN_RING_PROMINENCE, None),
            width=(MIN_RING_WIDTH, MAX_RING_WIDTH),
        )

        mask_refl = peak_d > 20
        for r in zip(ring_props['left_bases'], ring_props['right_bases']):
            mask_refl |= (peak_d >= d_shells[r[0]]) & (peak_d <= d_shells[r[0]])

        flt_spots = spots[~mask_refl]
        good_spots = flt_spots[flt_spots[:, 3].argsort()[::-1]]

        num_rings = len(ice_rings)
        num_bragg = len(good_spots)
        if num_bragg:
            signal_avg = int(good_spots[:50, 3].mean())
            signal_min = int(good_spots[:50, 3].min())
            signal_max = int(good_spots[0, 3])
        resolution = round(numpy.percentile(peak_d, 1), 3)

    return {
        'ice_rings': num_rings,
        'resolution': resolution,
        'total_spots': num_spots,
        'bragg_spots': num_bragg,
        'signal_avg': signal_avg,
        'signal_min': signal_min,
        'signal_max': signal_max,
    }


def signal_worker(inbox: Queue, outbox: Queue):
    """
    Signal strength worker. Reads data from the inbox queue and puts the results to the outbox
    :param inbox: Inbox queue to fetch tasks
    :param outbox: Outbox queue to place completed results
    """
    num_tasks = 0
    work_time = 0
    start_time = time.time()
    worker_name = short_uuid()

    while True:

        task = inbox.get()
        if task == 'END':
            inbox.put(task)     # Add the sentinel back to the queue for other processes and exit
            break

        num_tasks += 1
        t = time.time()
        kind, frame_data = task
        try:
            if kind == 'stream':
                dataset = eiger.EigerStream()
                dataset.read_header(frame_data[:2])
                dataset.read_image(frame_data[2:])
                index = dataset.header['frame_number']
            else:
                frame_path, index = frame_data
                frame = Path(frame_path)

                while time.time() - frame.stat().st_mtime < SAVE_DELAY:
                    # file is now being written to
                    time.sleep(SAVE_DELAY)

                dataset = read_image(frame_path)
            results = signal(dataset.data, dataset.header)
            results['frame_number'] = index
        except Exception as err:
            logger.error(err)
            results = {
                'ice_rings': 0,
                'resolution': 50,
                'total_spots': 0,
                'bragg_spots': 0,
                'signal_avg': 0,
                'signal_min': 0,
                'signal_max': 0,
                'frame_number': frame_data[-1],
            }
        outbox.put(results)
        work_time += time.time() - t
        time.sleep(0)

    total_time = time.time() - start_time
    ips = 0.0 if work_time == 0 else num_tasks/work_time
    logger.info(f'Worker completed {num_tasks}, {ips:0.1f} ips. Run-time: {total_time:0.0f} sec')


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


def distl_worker(inbox: Queue, outbox: Queue):
    """
    Distl signal strength worker. Reads data from the inbox queue and puts the results to the outbox
    :param inbox: Inbox queue to fetch tasks
    :param outbox: Outbox queue to place completed results
    """
    index = 0
    num_tasks = 0
    work_time = 0
    start_time = time.time()
    worker_name = short_uuid().decode('utf-8')

    while True:

        task = inbox.get()
        if task == 'END':
            inbox.put(task)  # Add the sentinel back to the queue for other processes and exit
            break

        num_tasks += 1
        t = time.time()
        kind, frame_data = task
        cleanup = []

        try:
            if kind == 'stream':
                dataset = eiger.EigerStream()
                dataset.read_header(frame_data[:2])
                dataset.read_image(frame_data[2:])
                index = dataset.header['frame_number']
                tmp_file = f'/dev/shm/{worker_name}_{index:05d}.cbf'
                cbf.write_minicbf(tmp_file, dataset.header, dataset.data)
                frame = Path(tmp_file)
                cleanup.append(frame)
            else:
                frame_path, index = frame_data
                frame = Path(frame_path)

                while time.time() - frame.stat().st_mtime < SAVE_DELAY:
                    # file is now being written to
                    time.sleep(SAVE_DELAY)

                dataset = read_image(frame_path)

            args = ['distl.signal_strength', 'distl.res.outer=3', 'distl.res.inner=10.0', str(frame)]
            output = subprocess.check_output(args, stderr=subprocess.STDOUT)

            results = parser.parse_text(output.decode('utf-8'), DISTL_SPECS)
            results['frame_number'] = index
        except Exception as err:
            logger.error(err)
            results = {
                'ice_rings': 0,
                'resolution': 50,
                'total_spots': 0,
                'bragg_spots': 0,
                'signal_avg': 0,
                'signal_min': 0,
                'signal_max': 0,
                'frame_number': index,
            }
        outbox.put(results)
        work_time += time.time() - t
        for f in cleanup:
            f.unlink(missing_ok=True)
        time.sleep(0)

    total_time = time.time() - start_time
    ips = 0.0 if work_time == 0 else num_tasks / work_time
    logger.info(f'Worker completed {num_tasks}, {ips:0.1f} ips. Run-time: {total_time:0.0f} sec')
