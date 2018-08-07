from wrfhydropy import namelist


def test_dict_merge():
    main_dict = {'key_1':'value_1',
                 'key_2': 1,
                 'sub_dict1': {
                     'subdict1_key1':'sub_value1',
                     'subdict1_key2':2,
                 },
                 'sub_dict2': {
                     'subdict2_key1':1}
                 }

    patch_dict = {
                     'sub_dict1':{
                         'subdict1_key1':'patched_value'
                     },
                     'key_2':'patched_value'
                 }

    patched_dict = namelist.dict_merge(main_dict,patch_dict)
    assert patched_dict == {'key_1': 'value_1',
                          'key_2': 'patched_value',
                          'sub_dict1':
                              {'subdict1_key1': 'patched_value', 'subdict1_key2': 2},
                          'sub_dict2': {'subdict2_key1': 1}
                          }


