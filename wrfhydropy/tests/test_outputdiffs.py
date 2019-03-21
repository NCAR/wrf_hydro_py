from wrfhydropy.core.outputdiffs import compare_ncfiles, OutputDataDiffs, OutputMetaDataDiffs
from wrfhydropy.core.simulation import SimulationOutput
import os

def test_outputdiffs_compare_ncfiles(sim_output):

    chrtout = list(sim_output.glob('*CHRTOUT_DOMAIN1*'))
    gwout = list(sim_output.glob('*GWOUT*'))

    assert compare_ncfiles(chrtout,chrtout) == [None,None,None]
    assert compare_ncfiles(chrtout,gwout) != [None,None,None]


def test_outputdiffs_outputdatadiffs(sim_output):

    output=SimulationOutput()
    output.collect_output(sim_dir=sim_output)

    output_diffs = OutputDataDiffs(output,output)
    print(output_diffs.diff_counts)
    assert output_diffs.diff_counts == {
        'channel_rt': 0, 'channel_rt_grid': 0, 'chanobs': 0,
        'lakeout': 0, 'gwout': 0, 'restart_hydro': 0,
        'restart_lsm': 0, 'restart_nudging': 0,
        'ldasout': 0, 'rtout': 0
    }


def test_outputdiffs_outputmetadatadiffs(sim_output):

    output=SimulationOutput()
    output.collect_output(sim_dir=sim_output)

    output_diffs = OutputMetaDataDiffs(output,output)

    assert output_diffs.diff_counts == {
        'channel_rt': 0, 'chanobs': 0, 'lakeout': 0, 'gwout': 3,
        'rtout': 0, 'ldasout': 0, 'restart_hydro': 0,
        'restart_lsm': 0, 'restart_nudging': 0
    }
