from wrfhydropy.core.outputdiffs import compare_ncfiles, OutputDataDiffs, OutputMetaDataDiffs
from wrfhydropy.core.simulation import SimulationOutput
import os

def test_outputdiffs_compare_ncfiles(sim_output):

    chrtout = list(sim_output.glob('*CHRTOUT*'))
    gwout = list(sim_output.glob('*GWOUT*'))

    assert compare_ncfiles(chrtout,chrtout) == [None,None,None]
    assert compare_ncfiles(chrtout,gwout) != [None,None,None]


def test_outputdiffs_outputdatadiffs(sim_output):

    output=SimulationOutput()
    output.collect_output(sim_dir=sim_output)
    public_atts = [att for att in dir(output) if not att.startswith('__')]
    for att in public_atts:
        assert getattr(output,att) is not None

    assert output.check_output_nas() is not None


    #
    # output = SimulationOutput()
    # output.collect_output(sim_output)
    #
    # output_diffs = OutputDataDiffs(output,output)
    #
    # print(output.channel_rt)
    # print(output_diffs.diff_counts)
    # assert 1==2
    #

