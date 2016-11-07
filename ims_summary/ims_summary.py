import luigi
from utils import read_config
import sys
import os
import numpy as np

class ImzmlDataset(luigi.ExternalTask):
    """
    Example of a possible external data dump
    To depend on external targets (typically at the top of your dependency graph), you can define
    an ExternalTask like this.
    """
    filename = luigi.Parameter()

    def output(self):
        """
        Returns the target output for this task.
        In this case, it expects a file to be present as a local file.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(self.filename)

class PrepareOutputFolder(luigi.Task):
    def output(self):
        return luigi.LocalTarget(os.path.join('outputs'))

    def run(self):
        import os
        if not os.path.exists("/outputs"):
            os.mkdir("/outputs")

class GenerateMeanSpectrum(luigi.Task):
    """
    Read imzml file from disk and produce mean spectrum
    """
    imzml_filename = luigi.Parameter()
    ppm = luigi.Parameter(default=1.)
    spec_type = luigi.Parameter(default=['mean', 'freq', 'max'])
    def requires(self):
        return [ImzmlDataset(self.imzml_filename),
                PrepareOutputFolder()]

    def output(self):
        return [luigi.LocalTarget(os.path.join('outputs','{}_spectrum.json'.format(spec_type))) for spec_type in self.spec_type]

    def run(self):
        from pyImagingMSpec.inMemoryIMS import inMemoryIMS
        import json
        ims_dataset = inMemoryIMS(self.imzml_filename)
        for ii, spec_type in enumerate(self.spec_type):
            mean_spec = ims_dataset.generate_summary_spectrum(summary_type=spec_type, ppm=self.ppm) #todo ppm as variable
            spec_tuples = [(np.round(_mz, decimals=5), np.round(_int, decimals=5)) for _mz, _int in zip(mean_spec[0], mean_spec[1]) if _int>0.]
            with open(self.output()[ii].path, 'w+') as f:
                json.dump(spec_tuples, f)
        print 'wrote summary spectra'


class SpectrumStatistics(luigi.Task):
    imzml_filename = luigi.Parameter()

    def requires(self):
        return [ImzmlDataset(self.imzml_filename)]

    def output(self):
        return luigi.LocalTarget(os.path.join('outputs','spectrum_stats.json'))


    def run(self):
        from pyimzml.ImzMLParser import ImzMLParser
        import json
        n_peaks = []
        s_min = []
        s_max = []
        s_ptp = []
        pcts = [5, 25, 50, 75, 95]
        s_pcts = []
        p = ImzMLParser(self.imzml_filename)
        for i, (x, y, z_) in enumerate(p.coordinates):
            mzs, ints = p.getspectrum(i)
            n_peaks.append(len(mzs))
            s_min.append(np.min(ints))
            s_max.append(np.max(ints))
            s_ptp.append(np.ptp(ints))
            s_pcts.append(list(np.percentile(ints, pcts)))

        stats = {
            'n_peaks': n_peaks,
            's_min': s_min,
            's_max': s_max,
            's_ptp': s_ptp,
            's_pcts': s_pcts
        }
        with open(self.output().path, 'w+') as f:
            json.dump(stats, f)
        print 'wrote spec stats'

class GenerateSummaryImages(luigi.Task):
    imzml_filename = luigi.Parameter()
    im_types = luigi.Parameter(default = ['sum', 'mean', 'max', 'median', 'std'])
    def requires(self):
        return [ImzmlDataset(self.imzml_filename)]

    def output(self):
        return [luigi.LocalTarget(os.path.join('outputs', '{}_image.json'.format(im_type))) for im_type in self.im_types]

    def run(self):
        from pyimzml.ImzMLParser import ImzMLParser
        import json
        p = ImzMLParser(self.imzml_filename)
        im = {}
        for im_type in self.im_types:
            im[im_type] = np.zeros((p.imzmldict["max count of pixels y"], p.imzmldict["max count of pixels x"]))
        for i, (x, y, z_) in enumerate(p.coordinates):
            mzs, ints = p.getspectrum(i)
            for im_type in self.im_types:
                im[im_type][y - 1, x - 1] = getattr(np, im_type)(ints)
        for ii, im_type in enumerate(self.im_types):
            result = {
                'im_vect': [_mz for _mz in im[im_type].flatten()],
                'im_shape': np.shape(im[im_type])
            }
            with open(self.output()[ii].path, 'w+') as f:
                json.dump(result, f)


class GetNumberOfAnnotations(luigi.Task):
    pass


class ProduceSummary(luigi.Task):
    imzml_filename = luigi.Parameter()
    def requires(self):
        return [GenerateMeanSpectrum(self.imzml_filename),
                GenerateSummaryImages(self.imzml_filename),
                SpectrumStatistics(self.imzml_filename),
                ]

if __name__ == '__main__':
    #config = read_config(sys.argv[1])
    import os
    cwd = os.getcwd()
    outdir = os.path.join(cwd,'outputs')
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    imzml_filename = [file for file in os.listdir(cwd) if file.lower().endswith('.imzml')]
    config = {'imzml_filename': imzml_filename[0]}
    # luigi.run(["--lock-pid-dir", "D:\\temp\\", "--local-scheduler"], main_task_cls=DecoratedTask)
    luigi.run(cmdline_args=[
                "--imzml-filename={}".format(config['imzml_filename']),
                            ],
              main_task_cls=ProduceSummary,
              local_scheduler=True)