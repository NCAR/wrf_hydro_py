from wrfhydropy import namelist
import copy
import json

# Make some test dicts
main_dict = {'key_1': 'value_1',
             'key_2': 1,
             'sub_dict1': {
                 'subdict1_key1': 'sub_value1',
                 'subdict1_key2': 2,
             },
             'sub_dict2': {
                 'subdict2_key1': 1}
             }

patch_dict = {
    'sub_dict1': {
        'subdict1_key1': 'patched_value'
    },
    'key_2': 'patched_value'
}

# Make some test namelists
main_nl = namelist.Namelist(main_dict)
patch_nl = namelist.Namelist(patch_dict)

def test_namelist_patch():
    patched_nl = main_nl.patch(patch_nl)

    assert patched_nl == {'key_1': 'value_1',
                          'key_2': 'patched_value',
                          'sub_dict1': {'subdict1_key1': 'patched_value', 'subdict1_key2': 2},
                          'sub_dict2': {'subdict2_key1': 1}}

def test_namelist_write_read(tmpdir):
    file_path = tmpdir + '/test_nml_write_f90'
    # Note that for F90nml write method the first key of hte dict must have a value of a dict
    write_nml = namelist.Namelist({'nml1':main_nl})
    write_nml.write(str(file_path))

    read_nl = namelist.load_namelist(str(file_path))

    assert write_nml == read_nl, 'written namelist does not match read namelist'


def test_namelist_diff():
    main_nl_altered = copy.deepcopy(main_nl)
    del main_nl_altered['key_1']
    main_nl_altered['sub_dict2']['subdict2_key1'] = 'altered_key1'

    nl_diffs = namelist.diff_namelist(main_nl,main_nl_altered)

    assert nl_diffs == {'type_changes':
                            {"root['sub_dict2']['subdict2_key1']": {'old_type': int,
                                                                    'new_type': str,
                                                                    'old_value': 1,
                                                                    'new_value': 'altered_key1'}
                             },
                        'dictionary_item_removed': {"root['key_1']"}
                        }


def test_namelist_dictmerge():
    patched_dict = namelist.dict_merge(main_dict,patch_dict)
    assert patched_dict == {'key_1': 'value_1',
                          'key_2': 'patched_value',
                          'sub_dict1':
                              {'subdict1_key1': 'patched_value', 'subdict1_key2': 2},
                          'sub_dict2': {'subdict2_key1': 1}
                          }

def test_namelist_jsonnamelist(tmpdir):
    file_path = tmpdir + '/test_json.json'


    json_string = json.loads('{"base":{"key1":1,"key2":"value2"},"a_config":{'
                             '"key2":"config_value2"}}')
    json.dump(json_string,open(file_path,'w'))

    json_nl = namelist.JSONNamelist(file_path)
    json_nl_config = json_nl.get_config('a_config')

    assert json_nl_config == {'key1': 1, 'key2': 'config_value2'}
    assert type(json_nl_config) == namelist.Namelist
