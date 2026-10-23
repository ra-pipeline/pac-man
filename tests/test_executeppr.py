"""Unit tests for PAC-MAN executeppr and executevlappr interfaces."""

import os
from unittest.mock import MagicMock, patch
import pytest

import pac_man
from pac_man import (
    pac_man_executeppr,
    pac_man_executevlappr,
)
import pac_man.executeppr as mod_eppr
import pac_man.executevlappr as mod_vlappr


SAMPLE_ALMA_PPR_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<SciPipeRequest xmlns="Alma/pipelinescience/SciPipeRequest"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="SciPipeRequest">
    <SciPipeRequestEntity entityId="UID_UNASSIGNED"
        entityTypeName="SciPipeRequest" datamodelVersion="0.1"/>
    <ProjectSummary>
        <ProposalCode xmlns="">PROJ-101</ProposalCode>
        <Observatory xmlns="">Synthetic Observatory</Observatory>
        <Telescope xmlns="">ALMA</Telescope>
        <ProcessingSite xmlns="">Undefined</ProcessingSite>
        <Operator xmlns="">Anonymous</Operator>
        <Mode xmlns="">CSV</Mode>
        <Version xmlns="">Undefined</Version>
        <CreationTime xmlns="">1970-01-01T00:00:00.000</CreationTime>
    </ProjectSummary>
    <ProjectStructure>
        <AlmaStructure>
            <ns1:ObsUnitSetRef xmlns:ns1="Alma/ObsPrep/ObsProject"
                entityId="uid://A001/X12/X345" partId="X23868574" entityTypeName="ObsProject"/>
            <ObsUnitSetTitle xmlns="">Synthetic OUS</ObsUnitSetTitle>
            <ObsUnitSetType xmlns="">Member</ObsUnitSetType>
            <ns2:ProjectStatusRef
                xmlns:ns2="Alma/Scheduling/ProjectStatus"
                entityId="uid://A001/X23/X456" entityTypeName="ProjectStatus"/>
            <ns3:OUSStatusRef xmlns:ns3="Alma/Scheduling/OUSStatus"
                entityId="uid://A001/X34/X567" entityTypeName="OUSStatus"/>
        </AlmaStructure>
    </ProjectStructure>
    <ProcessingRequests>
        <ProcessingRequest>
            <ProcessingIntents>
                <Intents>
                    <Keyword xmlns="">PROCESS</Keyword>
                    <Value xmlns="">true</Value>
                </Intents>
                <Intents>
                    <Keyword xmlns="">SESSION_1</Keyword>
                    <Value xmlns="">uid://A002/X123456/X78</Value>
                </Intents>
            </ProcessingIntents>
            <ProcessingProcedure>
                <ProcessingCommand>
                    <Command>hifa_importdata</Command>
                    <ParameterSet>
                        <Parameter>
                            <Keyword>dbservice</Keyword>
                            <Value>False</Value>
                        </Parameter>
                    </ParameterSet>
                </ProcessingCommand>
                <ProcessingCommand>
                    <Command>breakpoint</Command>
                    <ParameterSet/>
                </ProcessingCommand>
                <ProcessingCommand>
                    <Command>hifa_flagdata</Command>
                    <ParameterSet/>
                </ProcessingCommand>
            </ProcessingProcedure>
            <DataSet>
                <SchedBlockSet>
                    <SchedBlockIdentifier>
                        <RelativePath xmlns="">synthetic_path</RelativePath>
                        <ns4:SchedBlockRef
                            xmlns:ns4="Alma/ObsPrep/SchedBlock"
                            entityId="uid://A001/X12/X34"
                            entityTypeName="SchedBlock" documentVersion="1"/>
                        <ns5:SBStatusRef
                            xmlns:ns5="Alma/Scheduling/SBStatus"
                            entityId="uid://A001/X12/X35" entityTypeName="SBStatus"/>
                        <SBTitle xmlns="">Synthetic SB</SBTitle>
                        <AsdmIdentifier>
                            <ns7:AsdmRef xmlns:ns7="Alma/ValueTypes">
                                <ns7:ExecBlockId>uid://A002/X123456/X78</ns7:ExecBlockId>
                            </ns7:AsdmRef>
                            <AsdmDiskName xmlns="">APP-202.ms</AsdmDiskName>
                        </AsdmIdentifier>
                    </SchedBlockIdentifier>
                </SchedBlockSet>
            </DataSet>
        </ProcessingRequest>
    </ProcessingRequests>
    <ResultsProcessing>
        <ArchiveResults xmlns="">false</ArchiveResults>
        <CleanUpDisk xmlns="">false</CleanUpDisk>
        <UpdateProjectLifeCycle xmlns="">false</UpdateProjectLifeCycle>
        <NotifyOperatorWhenDone xmlns="">false</NotifyOperatorWhenDone>
        <PipelineOperatorAdress xmlns="">Unknown</PipelineOperatorAdress>
    </ResultsProcessing>
</SciPipeRequest>
"""

SAMPLE_VLA_PPR_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<SciPipeRequest xmlns="Alma/pipelinescience/SciPipeRequest"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="SciPipeRequest">
    <SciPipeRequestEntity entityId="UID_UNASSIGNED"
        entityTypeName="SciPipeRequest" datamodelVersion="0.1"/>
    <ProjectSummary>
        <ProposalCode xmlns="">PROJ-101</ProposalCode>
        <Observatory xmlns="">Synthetic VLA Observatory</Observatory>
        <Telescope xmlns="">VLA</Telescope>
        <ProcessingSite xmlns="">Undefined</ProcessingSite>
        <Operator xmlns="">Anonymous</Operator>
        <Mode xmlns="">CSV</Mode>
        <Version xmlns="">Undefined</Version>
        <CreationTime xmlns="">1970-01-01T00:00:00.000</CreationTime>
    </ProjectSummary>
    <ProcessingRequests>
        <ProcessingRequest>
            <ProcessingIntents>
                <Intents>
                    <Keyword xmlns="">PROCESS</Keyword>
                    <Value xmlns="">true</Value>
                </Intents>
                <Intents>
                    <Keyword xmlns="">SPECTRAL_MODE</Keyword>
                    <Value xmlns="">True</Value>
                </Intents>
            </ProcessingIntents>
            <ProcessingProcedure>
                <ProcessingCommand>
                    <Command>hifv_importdata</Command>
                    <ParameterSet/>
                </ProcessingCommand>
                <ProcessingCommand>
                    <Command>breakpoint</Command>
                    <ParameterSet/>
                </ProcessingCommand>
                <ProcessingCommand>
                    <Command>hifv_hanning</Command>
                    <ParameterSet/>
                </ProcessingCommand>
                <ProcessingCommand>
                    <Command>hifv_flagdata</Command>
                    <ParameterSet/>
                </ProcessingCommand>
            </ProcessingProcedure>
            <DataSet>
                <RelativePath xmlns="">vla_synthetic_path</RelativePath>
                <SdmIdentifier xmlns="">VLA-APP-303.ms</SdmIdentifier>
                <DataType xmlns="">asdm</DataType>
            </DataSet>
        </ProcessingRequest>
    </ProcessingRequests>
</SciPipeRequest>
"""


def test_interface_exports():
    """Verify all new interfaces and aliases are exported correctly."""
    assert pac_man.pac_man_executeppr is pac_man_executeppr
    assert pac_man.pac_man_executevlappr is pac_man_executevlappr
    assert pac_man.runner.executeppr is pac_man_executeppr
    assert pac_man.runner.executevlappr is pac_man_executevlappr
    assert pac_man.executeppr.executeppr is pac_man_executeppr
    assert pac_man.executevlappr.executeppr is pac_man_executevlappr
    assert mod_eppr.executeppr is pac_man_executeppr
    assert mod_vlappr.executeppr is pac_man_executevlappr


def test_executeppr_parsing_and_command_construction(tmp_path):
    """Test PPR parsing, ASDM mapping, and breakpoint handling in pac_man_executeppr."""
    ppr_path = tmp_path / 'test_ppr.xml'
    ppr_path.write_text(SAMPLE_ALMA_PPR_XML)

    with patch('pac_man.runner._run_task_sequence') as mock_run_seq:
        mock_context = MagicMock()
        mock_run_seq.return_value = mock_context

        result_context = pac_man_executeppr(
            pprXmlFile=str(ppr_path),
            importonly=False,
            bp_name='breakpoint',
            bpaction='ignore',
        )

        assert mock_run_seq.called
        call_kwargs = mock_run_seq.call_args[1]
        tasks = call_kwargs['task_sequence']

        # 'breakpoint' should be ignored/skipped
        task_names = [t[0] for t in tasks]
        assert 'hifa_importdata' in task_names
        assert 'hifa_flagdata' in task_names
        assert 'breakpoint' not in task_names

        # Import task should have vis and session injected
        import_task_args = tasks[0][1]
        assert 'vis' in import_task_args
        assert any('APP-202.ms' in str(v) for v in import_task_args['vis'])
        assert 'session' in import_task_args
        assert import_task_args['session'] == ['session_1']

        assert result_context is mock_context


def test_executeppr_breakpoint_break(tmp_path):
    """Test bpaction='break' terminates command list at breakpoint."""
    ppr_path = tmp_path / 'test_ppr.xml'
    ppr_path.write_text(SAMPLE_ALMA_PPR_XML)

    with patch('pac_man.runner._run_task_sequence') as mock_run_seq:
        mock_run_seq.return_value = MagicMock()

        pac_man_executeppr(
            pprXmlFile=str(ppr_path),
            importonly=False,
            bp_name='breakpoint',
            bpaction='break',
        )

        assert mock_run_seq.called
        tasks = mock_run_seq.call_args[1]['task_sequence']
        task_names = [t[0] for t in tasks]
        # Should stop before hifa_flagdata
        assert task_names == ['hifa_importdata']


def test_executevlappr_spectral_mode_and_breakpoint(tmp_path):
    """Test VLA executeppr skips breakpoint and skips hifv_hanning when SPECTRAL_MODE=True."""
    ppr_path = tmp_path / 'test_vla_ppr.xml'
    ppr_path.write_text(SAMPLE_VLA_PPR_XML)

    with patch('pac_man.runner._run_task_sequence') as mock_run_seq:
        mock_run_seq.return_value = MagicMock()

        pac_man_executevlappr(
            pprXmlFile=str(ppr_path),
            importonly=False,
        )

        assert mock_run_seq.called
        tasks = mock_run_seq.call_args[1]['task_sequence']
        task_names = [t[0] for t in tasks]

        # Breakpoint should be skipped
        assert 'breakpoint' not in task_names
        # Hanning smoothing should be skipped because SPECTRAL_MODE=True
        assert 'hifv_hanning' not in task_names
        # Import and flag tasks should remain
        assert 'hifv_importdata' in task_names
        assert 'hifv_flagdata' in task_names

        # Vis should be populated
        import_task_args = tasks[0][1]
        assert any('VLA-APP-303.ms' in str(v) for v in import_task_args['vis'])


def test_real_sample_alma_ppr_parsing():
    """Verify pac_man_executeppr can parse the real ALMA PPR request."""
    sample_ppr = '/home/rxue/Workspace/zfs/nrao/datasets/alma_if/2018.1.00765.S/rawdata/PPR_uid___A001_X133d_X1596.xml'
    if not os.path.exists(sample_ppr):
        pytest.skip('Sample dataset PPR not present in local workspace')

    with patch('pac_man.runner._run_task_sequence') as mock_run_seq:
        mock_run_seq.return_value = MagicMock()

        pac_man_executeppr(
            pprXmlFile=sample_ppr,
            importonly=True,
        )

        assert mock_run_seq.called
        kwargs = mock_run_seq.call_args[1]
        assert kwargs['importonly'] is True
        tasks = kwargs['task_sequence']
        assert len(tasks) > 0
        assert tasks[0][0] == 'hifa_importdata'
        assert len(tasks[0][1]['vis']) > 0


def test_real_sample_vla_ppr_parsing():
    """Verify pac_man_executevlappr can parse the real VLA PPR request."""
    sample_vla_ppr = '/home/rxue/Workspace/zfs/nrao/datasets/vlass/quicklook/VLASS3.1_T32t01.J054347+893000/PPR.xml'
    if not os.path.exists(sample_vla_ppr):
        pytest.skip('Sample VLA dataset PPR not present in local workspace')

    with patch('pac_man.runner._run_task_sequence') as mock_run_seq:
        mock_run_seq.return_value = MagicMock()

        pac_man_executevlappr(
            pprXmlFile=sample_vla_ppr,
            importonly=True,
        )

        assert mock_run_seq.called
        kwargs = mock_run_seq.call_args[1]
        assert kwargs['importonly'] is True
        tasks = kwargs['task_sequence']
        assert len(tasks) > 0
        assert tasks[0][0] == 'hifv_importdata'
