import lsst.daf.butler as dB
import lsst.obs.lsst as oL
import numpy as np
import matplotlib.pyplot as plt
import copy
from multiprocessing import Pool
from multiprocessing import cpu_count
import yaml


#genCollection = "u/lgiraldo/bps_ALLCCD_20220701"
genCollection = "u/lgiraldo/bps_ALLCCD_v2_20220804"
butler = dB.Butler("/repo/main", collections=genCollection)


CCD = 1
ptcDataset = butler.get("ptc", instrument="LSSTCam", detector=CCD)
exposure_pairs = np.array(sorted(ptcDataset.inputExpIdPairs["C00"]))


gains_pairs = {
    "C10": {"flux": [], "gain": [], "noise": []},
    "C11": {"flux": [], "gain": [], "noise": []},
    "C12": {"flux": [], "gain": [], "noise": []},
    "C13": {"flux": [], "gain": [], "noise": []},
    "C14": {"flux": [], "gain": [], "noise": []},
    "C15": {"flux": [], "gain": [], "noise": []},
    "C16": {"flux": [], "gain": [], "noise": []},
    "C17": {"flux": [], "gain": [], "noise": []},
    "C07": {"flux": [], "gain": [], "noise": []},
    "C06": {"flux": [], "gain": [], "noise": []},
    "C05": {"flux": [], "gain": [], "noise": []},
    "C04": {"flux": [], "gain": [], "noise": []},
    "C03": {"flux": [], "gain": [], "noise": []},
    "C02": {"flux": [], "gain": [], "noise": []},
    "C01": {"flux": [], "gain": [], "noise": []},
    "C00": {"flux": [], "gain": [], "noise": []},
}


camera = butler.get("camera", instrument="LSSTCam")
det_name_to_id = {}
det_id_to_name = {}
det_name_array = []
for det in camera:
    # print(det.getName(), det.getId())
    detName, detId = det.getName(), det.getId()
    if detId < 189:
        det_name_to_id[detName] = detId
        det_id_to_name[detId] = detName
        det_name_array.append(detName)

all_rafts = {detName: copy.deepcopy(gains_pairs) for detName in det_name_array}

all_dets_exps = []

for detName in all_rafts:
    detId = det_name_to_id[detName]
    for pair in exposure_pairs:
        expId = pair[0][0]
        all_dets_exps.append([detId, expId])


def gains_function(index):
    """Function to be passed to multiprocess.pool.
    Must have single index as input.
    """
    detId, expId = all_dets_exps[index]
    # print (detId, expId)
    try:
        cpCovs = butler.get(
            "cpCovariances", instrument="LSSTCam", detector=detId, exposure=expId
        )
    except:
        cpCovs = None
    return detId, expId, cpCovs


processes = cpu_count()
use = 22
print("I have ", processes, "cores here. Using: %g" % use)
pool = Pool(processes=use)
n_total = len(all_dets_exps)
print("before results")
results = pool.map(gains_function, list(range(n_total)))
pool.close()
pool.join()

results = np.array(results)

print(results)

## Now read the gains, fluxes in dictionaries

for line in results:
    detId, expId, cpCovs = line
    detName = det_id_to_name[detId]
    if cpCovs is None:
        print (f"Skipping {detId} {detName} {expId} because cpCovs is None")
        continue
    for ampName in cpCovs.gain:
        all_rafts[detName][ampName]['flux'].append(cpCovs.rawMeans[ampName][0])
        all_rafts[detName][ampName]['gain'].append(cpCovs.gain[ampName])
        all_rafts[detName][ampName]['noise'].append(cpCovs.noise[ampName])

print (all_rafts['R21_S01']) 

#Dump data in a yaml file
with open('./Data_files/BOT_gains_from_flats_2022AUG05.yaml', 'w') as outfile:
    yaml.dump(all_rafts, outfile)
    

