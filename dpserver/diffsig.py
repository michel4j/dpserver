import time
import base64
import uuid
import subprocess
from pathlib import Path
from multiprocessing import Queue

from typing import Any, Union

import numpy
import scipy.ndimage
from scipy.signal import find_peaks
from mxio import ImageFrame, DataSet
from mxio.formats import eiger, cbf

from scipy.ndimage.filters import maximum_filter, uniform_filter, gaussian_filter1d
from scipy.ndimage.interpolation import zoom
from scipy.ndimage.morphology import generate_binary_structure, binary_erosion
from skimage import filters

import cv2

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
    mean = uniform_filter(X, window_size, mode='reflect')
    sq_mean = uniform_filter(X * X, window_size, mode='reflect')
    return numpy.sqrt(numpy.abs(sq_mean - mean * mean))


def variance_filter(x, size=3):
    mean = uniform_filter(x, size, mode='reflect')
    sq_mean = uniform_filter(x * x, size, mode='reflect')
    return sq_mean - mean*mean

def mean_and_variance_filter(x, size=3):

    mean = uniform_filter(x.astype(numpy.single), size, mode='reflect')
    sq_mean = uniform_filter(x * x, size, mode='reflect')
    return mean, numpy.abs(sq_mean - mean * mean)

def mean_variance_filter(data, size=3):
    mean, sqr_mean = (
        cv2.boxFilter(x, -1, (size, size), borderType=cv2.BORDER_REFLECT)
        for x in (data, data * data)
    )
    return mean, numpy.abs(sqr_mean - mean*mean)


def std_filter(x, size=3):
    c3 = variance_filter(x, size)
    return numpy.sqrt(c3)


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
    background = (image <= 0)
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

    start_time = time.time()

    scale = 1
    if scale != 1:
        image = zoom(frame.data, 1 / scale)
        cy, cx = numpy.array([frame.center.x, frame.center.y]) / scale
    else:
        image = frame.data
        cy, cx = frame.center.x, frame.center.y

    mask_image = scipy.ndimage.minimum_filter(image, 3)
    mask = mask_image < 0
    image[mask] = 0

    mean, variance = mean_variance_filter(image, 3)
    mask = mask | mean == 0
    dispersion = variance / (mean + 1)
    dispersion[mask] = 1

    thresh = numpy.percentile(dispersion, 99.9)
    dispersion[dispersion < thresh] = 1
    signal = scipy.ndimage.maximum_filter(dispersion, 3)

    # data = filters.gaussian(std_img, sigma=2)
    # thresh = numpy.percentile(std_img, 98.)
    # data[data < thresh] = 0.0
    peaks, counts = detect_peaks(signal)
    num_spots = len(peaks)

    # initialize results
    score = 0.
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
                frame.start_angle + 0.5 * frame.delta_angle,
                counts[i],
                0, 0, 0,
                peak_radii[i], peak_angle[i], peak_resol[i]  # #7 = radius, #8 = angle, #9 = d-spacing
            )
            for i, pk in enumerate(peaks) if peak_radii[i] < max_radius and peak_resol[i] < D_MAX
        ])

        num_bragg = 0
        score = 0
        if len(spots):
            clean_spots, num_rings = remove_rings(spots, bins=2000, min_height=2)
            numpy.save('/tmp/spots.npy', clean_spots)
            num_bragg = len(clean_spots)

            if num_bragg:
                signal_avg = clean_spots[:50, 3].mean()
                signal_min = clean_spots[:50, 3].min()
                signal_max = clean_spots[0, 3]
                resolution = round(numpy.percentile(peak_resol, 1), 3)
                score = signal_max * num_bragg / max(num_rings, 1)

    return {
        'ice_rings': num_rings,
        'resolution': resolution,
        'total_spots': num_spots,
        'bragg_spots': num_bragg,
        'signal_avg': round(signal_avg,2),
        'signal_min': round(signal_min,2),
        'signal_max': round(signal_max,2),
        'score': round(score,3),
        'duration': round(1000*(time.time() - start_time), 3)
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

DOZOR_DATA = """!
detector {detector}
library /cmcf_apps/xds/xds-zcbf.so
nx {x_size}
ny {y_size}
pixel {pixel_size:0.4f}
exposure {exposure:0.4f}
spot_size 2
spot_level 6
detector_distance {distance:0.3f}
X-ray_wavelength {wavelength:0.4f}
fraction_polarization 0.990
pixel_min 0
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

DOZOR_ENTRY = """
 N    | SPOTS     Main     Visible
image | num.of   Score    Resolution
------------------------------------
<int:index> | <int:bragg_spots> <float:score> <float:resolution>
------------------------------------
"""

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


def file_signal(frame_path: str, index: int) -> dict:
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
        dataset = DataSet.new_from_file(frame)
        info = signal(dataset.frame, scale=4)
        info['frame_number'] = dataset.index
        if frame_path.startswith('/dev/shm/'):
            frame.unlink(missing_ok=True)
        result.update(info)
    return result


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
        dat_file = Path(frame_path + '.dat')
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
        if frame_path.startswith('/dev/shm/'):
            frame.unlink(missing_ok=True)
        dat_file.unlink(missing_ok=True)
        result.update(info)
    return result


def stream_signal(frame_data: Any) -> dict:
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
    start_time = time.time()
    dataset = eiger.EigerStream()
    dataset.parse_header(header)
    dataset.parse_image(data)
    info = signal(dataset.frame, scale=4)
    info['frame_number'] = dataset.index
    info['duration'] = 1000*(time.time() - start_time)
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
    index = 0
    num_tasks = 0
    work_time = 0
    start_time = time.time()
    cleanup = []

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
                result = dozor_signal(frame_path, index)

        except Exception as err:
            logger.error(err)

        results.put(result)
        work_time += time.time() - t
        tasks.task_done()
        time.sleep(0)

    total_time = time.time() - start_time
    ips = 0.0 if work_time == 0 else num_tasks / work_time
    logger.debug(f'Worker completed {num_tasks}, {ips:0.1f} ips. Run-time: {total_time:0.0f} sec')
