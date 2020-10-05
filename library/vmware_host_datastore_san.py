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
  datacenter_name:
    description:
    - Name of the datacenter to add the datastore.
    required: false
  volume_device_name:
    description:
    - Name of the device to be used as VMFS datastore.
  esxi_hostname:
    description:
    - ESXi hostname to manage the datastore.
    required: true
  state:
    description:
    - "present: Mount datastore on host if datastore is absent else do nothing."
    - "absent: Umount datastore if datastore is present else do nothing."
    default: present
    choices: [ present, absent ]
extends_documentation_fragment: vmware.documentation
'''

EXAMPLES = r'''
- name: Mount VMFS datastores to ESXi
  vmware_host_datastore_san:
      hostname: '{{ vcenter_hostname }}'
      username: '{{ vcenter_user }}'
      password: '{{ vcenter_pass }}'
      datacenter_name: '{{ datacenter }}'
      datastore_name: '{{ item.name }}'
      storage_device_name: 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
      esxi_hostname: '{{ inventory_hostname }}'
      state: present
  delegate_to: localhost

- name: Remove/Umount Datastores from ESXi
  vmware_host_datastore_san:
      hostname: '{{ vcenter_hostname }}'
      username: '{{ vcenter_user }}'
      password: '{{ vcenter_pass }}'
      datacenter_name: '{{ datacenter }}'
      datastore_name: San_datastore01
      esxi_hostname: '{{ inventory_hostname }}'
      state: absent
  delegate_to: localhost
'''

RETURN = r'''
'''

try:
    from pyVmomi import vim, vmodl
except ImportError:
    pass

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.vmware import vmware_argument_spec, PyVmomi, find_datastore_by_name, get_all_objs, wait_for_task
from ansible.module_utils._text import to_native

class VMwareHostSanDatastore(PyVmomi):
    def __init__(self, module):
        super(VMwareHostSanDatastore, self).__init__(module)

        self.datastore_name = module.params['datastore_name']
        self.esxi_hostname = module.params['esxi_hostname']
        self.volume_device_name = module.params.get('volume_device_name')
        self.state = module.params['state']
        self.datastore_cluster_name = module.params.get('datastore_cluster_name')

        self.esxi = self.find_hostsystem_by_name(self.esxi_hostname)
        if self.esxi is None:
            self.module.fail_json(msg="Failed to find ESXi hostname %s " % self.esxi_hostname)

        if self.volume_device_name:
            self.volume_device_name = self.volume_device_name.lower()

    def process_state(self):
        ds_states = {
            'present': self.mount_san_datastore_host,
            'absent': self.umount_san_datastore_host
        }
        try:
            datastore = self.check_datastore_host_state()
            ds_states[self.state](datastore)
        except (vmodl.RuntimeFault, vmodl.MethodFault) as vmodl_fault:
            self.module.fail_json(msg=to_native(vmodl_fault.msg))
        except Exception as e:
            self.module.fail_json(msg=to_native(e))

    def state_exit_unchanged(self):
        self.module.exit_json(changed=False)

    def check_datastore_host_state(self):
        self.esxi.configManager.storageSystem.RescanAllHba()
        return self.find_datastore_by_name(self.datastore_name)

    def umount_san_datastore_host(self, datastore):
        if not datastore:
            self.module.exit_json(changed=False)

        error_message_umount = "Cannot umount datastore %s from host %s" % (self.datastore_name, self.esxi_hostname)
        try:
            #task = datastore.DatastoreEnterMaintenanceMode()
            #success, result = wait_for_task(task)
            for host in datastore.host:
                host.key.configManager.storageSystem.UnmountVmfsVolume(datastore.info.vmfs.uuid)

            self.esxi.configManager.datastoreSystem.RemoveDatastore(datastore)
        except (vim.fault.NotFound, vim.fault.HostConfigFault, vim.fault.ResourceInUse) as fault:
            self.module.fail_json(msg="%s: %s" % (error_message_umount, to_native(fault.msg)))
        except Exception as e:
            self.module.fail_json(msg="%s: %s" % (error_message_umount, to_native(e)))
        self.module.exit_json(changed=True, result="Datastore %s on host %s" % (self.datastore_name, self.esxi_hostname))

    def rescan_other_hosts_in_cluster(self):
        cluster_hosts = self.get_all_hosts_by_cluster(self.esxi.parent.name)
        for host in cluster_hosts:
            if host.name != self.esxi_hostname:
                host.configManager.storageSystem.RescanAllHba()
                host.configManager.storageSystem.RescanVmfs()

    def mount_san_datastore_host(self, datastore):
        ds_path = "/vmfs/devices/disks/naa." + str(self.volume_device_name)
        host_ds_system = self.esxi.configManager.datastoreSystem
        ds_system = vim.host.DatastoreSystem
        error_message_mount = "Cannot mount datastore %s on host %s" % (self.datastore_name, self.esxi_hostname)
        try:
            if not datastore:
                vmfs_ds_options = ds_system.QueryVmfsDatastoreCreateOptions(host_ds_system,
                                                                            ds_path)
                vmfs_ds_options[0].spec.vmfs.volumeName = self.datastore_name
                ds = ds_system.CreateVmfsDatastore(host_ds_system,
                                                   vmfs_ds_options[0].spec)
                result_msg = "Datastore %s on host %s" % (self.datastore_name, self.esxi_hostname)
                if self.datastore_cluster_name:
                    #folders = self.content.viewManager.CreateContainerView(self.content.rootFolder, [vim.Folder], True).view
                    folders = get_all_objs(self.content, [vim.Folder])
                    dsfolder = [x for x in folders if x.name == 'datastore'][0]
                    srcfolder = [x for x in dsfolder.childEntity if x.name == self.datastore_name][0]
                    tgtfolder = [x for x in dsfolder.childEntity if x.name == self.datastore_cluster_name][0]
                    task = tgtfolder.MoveIntoFolder_Task([srcfolder])

                    success, result = wait_for_task(task)
                    result_msg = "Datastore %s of cluster %s on host %s : %s" % (self.datastore_name,
                                                                                 self.datastore_cluster_name,
                                                                                 self.esxi_hostname,
                                                                                 str(result))

                self.rescan_other_hosts_in_cluster()
                self.module.exit_json(changed=True, result=result_msg)

            existing_wwns = [x.diskName.split('.')[-1] for x in datastore.info.vmfs.extent]
            if self.volume_device_name in existing_wwns:
                exp_options = host_ds_system.QueryVmfsDatastoreExpandOptions(datastore = datastore)
                if len(exp_options) > 0:
                    spec = [x.spec for x in exp_options if self.volume_device_name.lower() in x.spec.extent.diskName][0]
                    host_ds_system.ExpandVmfsDatastore(datastore=datastore, spec=spec)
                    result_msg = "Expanded storage on datastore %s" % (self.datastore_name)
                    self.module.exit_json(changed=True, result=result_msg)
            else:
                # TODO: Add missing WWN to datastore
                pass

            self.state_exit_unchanged()

        except (vim.fault.NotFound, vim.fault.DuplicateName,
                vim.fault.HostConfigFault, vmodl.fault.InvalidArgument) as fault:
            self.module.fail_json(msg="%s : %s" % (error_message_mount, to_native(fault.msg)))
        except Exception as e:
            self.module.fail_json(msg="%s : %s" % (error_message_mount, to_native(e)))


def main():
    argument_spec = vmware_argument_spec()
    argument_spec.update(
        esxi_hostname=dict(type='str', required=True),
        datastore_name=dict(type='str', required=True),
        datastore_cluster_name=dict(type='str', required=False),
        volume_device_name=dict(type='str'),
        state=dict(type='str', default='present', choices=['absent', 'present'])
    )

    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
    )

    vmware_host_datastore_san = VMwareHostSanDatastore(module)
    vmware_host_datastore_san.process_state()


if __name__ == '__main__':
    main()
