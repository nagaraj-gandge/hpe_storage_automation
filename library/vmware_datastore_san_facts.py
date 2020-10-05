#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2018, Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function
__metaclass__ = type

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'community'
}

DOCUMENTATION = r'''
---
module: vmware_host_datastore_san
short_description: Manage a datastore on ESXi host
description:
- This module can be used to create and delete datastores from SAN volumes on ESXi host.
- All parameters and VMware object names are case sensitive.
version_added: '0.1'
author:
- Harugop, Jayasheel <jch@hpe.com>
- Avinash Jalumuru <avinash.jalumuru@hpe.com>
notes:
- Tested on vSphere 6.0 and 6.5
requirements:
- python >= 2.6
- PyVmomi
options:
  datastore_name:
    description:
    - Name of the datastore to add/remove.
    required: true
  esxi_hostname:
    description:
    - ESXi hostname to manage the datastore.
    required: true
extends_documentation_fragment: vmware.documentation
'''

EXAMPLES = r'''
- name: Mount VMFS datastores to ESXi
  vmware_host_datastore_san:
      hostname: '{{ vcenter_hostname }}'
      username: '{{ vcenter_user }}'
      password: '{{ vcenter_pass }}'
      datastore_name: '{{ item.name }}'
      esxi_hostname: '{{ inventory_hostname }}'
  delegate_to: localhost
'''

RETURN = r'''
'''

try:
    from pyVmomi import vim, vmodl
    HAS_PYVMOMI = True
except ImportError:
    HAS_PYVMOMI = False

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.vmware import (HAS_PYVMOMI, vmware_argument_spec, find_datastore_by_name, get_all_objs,
                                         wait_for_task, find_hostsystem_by_name, find_cluster_by_name, connect_to_api)
from ansible.module_utils._text import to_native

class VMwareDatastore(object):
    def __init__(self, module):
        self.datastore_name = module.params.get('datastore_name')
        self.esxi_hostname = module.params.get('esxi_hostname')
        self.content = connect_to_api(module)
        self.module = module

    def gather_facts(self):
        datastores = list()
        if self.datastore_name:
            datastore = find_datastore_by_name(self.content, self.datastore_name)
            datastores.extend([self.read_datastore(datastore)])
            return datastores
        elif self.esxi_hostname:
            host = find_hostsystem_by_name(self.content, self.esxi_hostname)
            vmware_datastores = host.datastore
        else:
            vmware_datastores = get_all_objs(self.content, [vim.Datastore])

        for datastore in vmware_datastores:
            datastores.extend([self.read_datastore(datastore)])
        return datastores

    def read_datastore(self, datastore):
        try:
            ds = {}
            summary = datastore.summary
            ds['name'] = summary.name
            ds['maintenanceMode'] = summary.maintenanceMode
            ds['url'] = summary.url
            ds['datastore_cluster'] = 'N/A'
            if isinstance(datastore.parent, vim.StoragePod):
                ds['datastore_cluster'] = datastore.parent.name

            vmfs = datastore.info.vmfs
            ds['vmfs_type'] = vmfs.type
            ds['wwn'] = [ x.diskName.split('.')[-1] for x in vmfs.extent]
            return ds
        except (vmodl.RuntimeFault, vmodl.MethodFault) as vmodl_fault:
            self.module.fail_json(msg=to_native(vmodl_fault.msg))
        except Exception as e:
            self.module.fail_json(msg=to_native(e))

def main():
    argument_spec = vmware_argument_spec()
    argument_spec.update(
        datastore_name=dict(type='str', required=False),
        esxi_hostname=dict(type='str', required=False)
    )

    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
    )

    if not HAS_PYVMOMI:
        module.fail_json(msg='pyvmomi is required for this module')

    try:
        vmware_datastore = VMwareDatastore(module)
        datastores = vmware_datastore.gather_facts()

        module.exit_json(changed=False, datastores = datastores)
    except Exception as e:
        module.fail_json(msg=to_native(e))

if __name__ == '__main__':
    main()
